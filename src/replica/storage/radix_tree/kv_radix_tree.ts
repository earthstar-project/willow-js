import { bytesEquals } from "../../../../deps.ts";
import { compareBytes, incrementLastByte } from "../../../util/bytes.ts";
import { Key, KvDriver } from "../kv/types.ts";

export class KvRadixTree<ValueType> {
  private kv: KvDriver;

  constructor(kv: KvDriver) {
    this.kv = kv;
  }

  insert(key: Uint8Array, value: ValueType) {
    return this.kv.set([key], value);
  }

  remove(key: Uint8Array) {
    return this.kv.delete([key]);
  }

  async *prefixesOf(
    key: Uint8Array,
    atLeast: Key = [],
  ): AsyncIterable<[Uint8Array, ValueType]> {
    for await (
      const entry of this.kv.list<ValueType>({
        start: atLeast,
        end: [key],
      }, {
        batchSize: key.length === 0 ? 1 : undefined,
        limit: key.length === 0 ? 1 : undefined,
      })
    ) {
      const candidate = entry.key[0] as Uint8Array;

      if (compareBytes(candidate, key) >= 0) {
        break;
      }

      const longestCommonPrefix = getLongestCommonPrefix(
        candidate,
        key,
      );

      if (longestCommonPrefix.byteLength === candidate.byteLength) {
        yield [candidate, entry.value];

        const nextAtLeast = new Uint8Array(longestCommonPrefix.byteLength + 1);
        nextAtLeast.set(longestCommonPrefix);

        for await (
          const result of this.prefixesOf(key, [
            nextAtLeast,
          ])
        ) {
          yield result;
        }

        break;
      }
    }
  }

  async *prefixedBy(key: Uint8Array): AsyncIterable<[Uint8Array, ValueType]> {
    for await (
      const entry of this.kv.list<ValueType>({
        start: [key],
        end: [incrementLastByte(key)],
      })
    ) {
      if (bytesEquals(entry.key[0] as Uint8Array, key)) {
        continue;
      }

      yield [entry.key[0] as Uint8Array, entry.value];
    }
  }
}

function getLongestCommonPrefix(
  candidate: Uint8Array,
  target: Uint8Array,
): Uint8Array {
  const bytes: number[] = [];

  for (let i = 0; i < candidate.byteLength; i++) {
    if (candidate[i] !== target[i]) {
      break;
    }

    bytes.push(candidate[i]);
  }

  return new Uint8Array(bytes);
}
