import { bytesConcat, bytesEquals } from "../../../../deps.ts";
import { incrementLastByte } from "../../../util/bytes.ts";
import { KvBatch, KvDriver } from "../kv/types.ts";

enum Phantomness {
  Phantom,
  Real,
  RealWithPhantom,
}

export type GwilTreeNode<ValueType> = [Phantomness, Uint8Array, ValueType];

const decoder = new TextDecoder();
const d = (b: Uint8Array) => decoder.decode(b);

export class GwilsPrefixTree<ValueType> {
  private kv: KvDriver;

  constructor(kv: KvDriver) {
    this.kv = kv;
  }

  private async addBackingPhantom(
    key: Uint8Array,
    value: GwilTreeNode<ValueType>,
    batch: KvBatch,
  ) {
    if (value[0] === Phantomness.Real) {
      batch.set([key], [
        Phantomness.RealWithPhantom,
        value[1],
        value[2],
      ]);
    }
  }

  async insert(
    key: Uint8Array,
    value: ValueType,
    position = 1,
    lastPassedNode?: { key: Uint8Array; value: GwilTreeNode<ValueType> },
  ): Promise<void> {
    // Check if first element exists in
    const searchKey = key.slice(0, position);
    const actualKey: [number, Uint8Array] = [position, searchKey];

    const existingNode = await this.kv.get<GwilTreeNode<ValueType>>([
      searchKey,
    ]);

    // Create a new node with what's left over.
    if (!existingNode) {
      const vector = key.slice(position);

      const node: GwilTreeNode<ValueType> = [Phantomness.Real, vector, value];

      const batch = this.kv.batch();

      if (lastPassedNode) {
        this.addBackingPhantom(lastPassedNode.key, lastPassedNode.value, batch);
      }

      batch.set([searchKey], node);

      await batch.commit();

      return;
    }

    // There is something here! Complexity begins.

    // First check if the key + vector is a prefix of ours.
    const completeValue = bytesConcat(searchKey, existingNode[1]);
    const foundIsPrefix = isPrefix(completeValue, key);

    // 	If it is, we check from the next position. Buck passed.
    if (foundIsPrefix) {
      return this.insert(key, value, completeValue.byteLength + 1, {
        key: searchKey,
        value: existingNode,
      });
    }

    //	If it's not, we need to fuck with this node.

    // Deal with the special case where the inserted node is a prefix of the one we just found.
    const newIsPrefix = isPrefix(key, completeValue);

    if (newIsPrefix) {
      const batch = this.kv.batch();

      // The new vector is the new key's suffix compared to this key.
      const newVector = key.slice(searchKey.byteLength);

      // Set the search key's new vector and value (the new one)
      batch.set([searchKey], [Phantomness.RealWithPhantom, newVector, value]);

      const newNodeKey = completeValue.slice(
        0,
        searchKey.byteLength + newVector.byteLength + 1,
      );
      const newNodeVector = completeValue.slice(
        searchKey.byteLength + newVector.byteLength + 1,
      );

      // Set the new node's key with

      batch.set([newNodeKey], [
        existingNode[0],
        newNodeVector,
        existingNode[2],
      ]);

      await batch.commit();

      return;
    }

    // Fork the node, creating a PHANTOM
    // Get the common vector
    const batch = this.kv.batch();

    const newVectorFoundNode: number[] = [];

    for (let i = searchKey.byteLength; i < key.byteLength; i++) {
      if (key[i] === completeValue[i]) {
        newVectorFoundNode.push(key[i]);
      } else {
        break;
      }
    }

    const newVectorFoundNodeBytes = new Uint8Array(newVectorFoundNode);

    // And this node is a phantom!!! Whooooo
    batch.set([searchKey], [
      Phantomness.Phantom,
      new Uint8Array(newVectorFoundNodeBytes),
      null as ValueType,
    ]);

    // Deal with the old (real) node that used to be here

    const newValue = bytesConcat(searchKey, newVectorFoundNodeBytes);

    const foundNodeNewKey = completeValue.slice(0, newValue.byteLength + 1);
    const foundNodeNewVector = completeValue.slice(newValue.byteLength + 1);

    batch.set([foundNodeNewKey], [
      existingNode[0],
      foundNodeNewVector,
      existingNode[2],
    ]);

    // And finally insert the new value. Yay.

    const newKey = key.slice(0, newValue.byteLength + 1);
    const newVector = key.slice(newValue.byteLength + 1);

    batch.set([newKey], [Phantomness.Real, newVector, value]);

    await batch.commit();
  }

  async remove(
    key: Uint8Array,
    position = 1,
    phantomParent?: [Uint8Array, Uint8Array],
  ): Promise<boolean> {
    // Try and find the value, and remove it.
    const searchKey = key.slice(0, position);

    const existingNode = await this.kv.get<GwilTreeNode<ValueType>>([
      searchKey,
    ]);

    // No path, no nothing.
    if (!existingNode) {
      return false;
    }

    // First check if the key + vector is a prefix of ours.
    const completeValue = bytesConcat(searchKey, existingNode[1]);

    if (bytesEquals(completeValue, key)) {
      if (phantomParent) {
        // Sibling is a phantom...

        for await (
          const entry of this.kv.list({
            start: [completeValue],
            end: [],
          })
        ) {
        }
      }

      if (existingNode[0] === Phantomness.RealWithPhantom) {
        await this.kv.set([searchKey], [
          Phantomness.Phantom,
          existingNode[1],
          null,
        ]);
      } else {
        await this.kv.delete([searchKey]);
      }

      return true;
    }

    const foundIsPrefix = isPrefix(completeValue, key);

    // 	If it is, we check from the next position. Buck passed.
    if (foundIsPrefix) {
      if (existingNode[0]) {
        return this.remove(key, completeValue.byteLength + 1, [
          searchKey,
          existingNode[1],
        ]);
      }

      return this.remove(key, completeValue.byteLength + 1);
    }

    return false;
  }

  async *prefixesOf(key: Uint8Array): AsyncIterable<[Uint8Array, ValueType]> {
    let searchLength = 1;

    while (true) {
      const searchKey = key.slice(0, searchLength);

      const node = await this.kv.get<GwilTreeNode<ValueType>>([searchKey]);

      if (!node) {
        break;
      }

      const completeVal = bytesConcat(searchKey, node[1]);

      if (completeVal.byteLength >= key.byteLength) {
        break;
      }

      // Only do this if not a phantom.
      if (node[0] !== Phantomness.Phantom && isPrefix(completeVal, key)) {
        yield [completeVal, node[2]];
      }

      searchLength = completeVal.byteLength + 1;
    }
  }

  async *prefixedBy(key: Uint8Array): AsyncIterable<[Uint8Array, ValueType]> {
    // The annoying bit. Find items that are prefixed by the given key but have shorter keys in the store than the key we're searching for.
    let searchLength = 1;

    while (true) {
      const searchKey = key.slice(0, searchLength);

      const node = await this.kv.get<GwilTreeNode<ValueType>>([searchKey]);

      if (!node) {
        break;
      }

      const completeVal = bytesConcat(searchKey, node[1]);

      if (bytesEquals(completeVal, key)) {
        break;
      }

      if (completeVal.byteLength <= key.byteLength) {
        searchLength = completeVal.byteLength + 1;
        continue;
      }

      // Only do this if not a phantom.
      if (node[0] !== Phantomness.Phantom && isPrefix(key, completeVal)) {
        yield [completeVal, node[2]];
      }

      searchLength = completeVal.byteLength + 1;
    }

    // The easy bit
    for await (
      const entry of this.kv.list<GwilTreeNode<ValueType>>({
        start: [key],
        end: [incrementLastByte(key)],
      })
    ) {
      if (entry.value[0] === Phantomness.Phantom) {
        continue;
      }

      const completeVal = bytesConcat(
        entry.key[0] as Uint8Array,
        entry.value[1],
      );

      if (bytesEquals(completeVal, key)) {
        continue;
      }

      yield [completeVal, entry.value[2]];
    }
  }

  async print() {
    for await (
      const { key, value } of this.kv.list<GwilTreeNode<ValueType>>({
        start: [],
        end: [Number.MAX_SAFE_INTEGER],
      })
    ) {
      console.log(
        value[0] === Phantomness.Phantom
          ? "👻"
          : value[0] === Phantomness.Real
          ? "🔑"
          : "🗝",
        `${decoder.decode(key[0] as unknown as Uint8Array)}(${
          decoder.decode(value[1])
        })`,
        "-",
        value[2],
      );
    }

    console.groupEnd();
  }
}

function isPrefix(maybePrefix: Uint8Array, against: Uint8Array) {
  for (let i = 0; i < maybePrefix.byteLength; i++) {
    if (maybePrefix[i] !== against[i]) {
      return false;
    }
  }

  return true;
}
