import { compareBytes } from "../../../util/bytes.ts";
import { PrefixIterator } from "./types.ts";

type MemoryNode<ValueType> = {
  key: Uint8Array;
  value: ValueType | null;
  children: Map<number, MemoryNode<ValueType>>;
};

export class RadixishTree<ValueType> implements PrefixIterator<ValueType> {
  private root: MemoryNode<ValueType> = {
    key: new Uint8Array(),
    value: null,
    children: new Map<number, MemoryNode<ValueType>>(),
  };

  print() {
    const printNode = (node: MemoryNode<ValueType>) => {
      console.group(node.key, node.value);

      for (const [key, child] of node.children) {
        console.log(key, "->");
        printNode(child);
      }

      console.groupEnd();
    };

    printNode(this.root);
  }

  insert(key: Uint8Array, value: ValueType): Promise<void> {
    // Start at root node

    let node = this.root;

    while (true) {
      const lcp = getLongestCommonPrefix(node.key, key);

      if (
        lcp.byteLength === key.byteLength &&
        key.byteLength === node.key.byteLength && node.value
      ) {
        break;
      } else if (lcp.byteLength === key.byteLength && node.value === null) {
        node.value = value;

        break;
      } else if (lcp.byteLength === key.byteLength && node.value) {
        const splitNode = {
          key: node.key,
          value: node.value,
          children: new Map(node.children),
        };

        node.key = key;
        node.value = value;
        node.children = new Map();

        node.children.set(splitNode.key[lcp.byteLength], splitNode);

        break;
      } else if (
        node.key.byteLength === 0 ||
        lcp.byteLength === node.key.byteLength
      ) {
        // Check if any children along its edges
        const edge = key[lcp.byteLength];
        const childAtEdge = node.children.get(edge);

        if (childAtEdge) {
          // Node is now that child.
          node = childAtEdge;
          continue;
        } else {
          // Insert child at that edge

          node.children.set(edge, {
            key,
            value,
            children: new Map(),
          });

          break;
        }
      } else if (lcp.byteLength > 0) {
        const splitNode = {
          key: node.key,
          value: node.value,
          children: new Map(node.children),
        };

        const newNode = {
          key: key,
          value: value,
          children: new Map(),
        };

        node.key = lcp;
        node.value = null;
        node.children = new Map();

        node.children.set(splitNode.key[lcp.byteLength], splitNode);
        node.children.set(newNode.key[lcp.byteLength], newNode);

        break;
      } else {
        break;
      }
    }

    return Promise.resolve();
  }

  remove(key: Uint8Array): Promise<boolean> {
    let node = this.root;

    while (true) {
      const lcp = getLongestCommonPrefix(node.key, key);

      if (
        node.key.byteLength === 0 || lcp.byteLength === node.key.byteLength
      ) {
        // Check if any children along its edges
        const edge = key[lcp.byteLength];
        const childAtEdge = node.children.get(edge);

        if (childAtEdge && compareBytes(childAtEdge.key, key) === 0) {
          // We found it, noice...
          //  Absorb any children of the node to be deleted.
          if (childAtEdge.children.size === 0) {
            node.children.delete(edge);

            if (node.children.size === 1 && node.value !== null) {
              // Merge them.
              const nodeToMerge = Array.from(node.children.values())[0];

              node.key = nodeToMerge.key;
              node.children = nodeToMerge.children;
              node.value = nodeToMerge.value;
            }
          } else if (childAtEdge.children.size === 1) {
            node.children.set(
              edge,
              Array.from(childAtEdge.children.values())[0],
            );
          } else {
            childAtEdge.value = null;
          }

          return Promise.resolve(true);
        } else if (childAtEdge) {
          // Node is now that child.
          node = childAtEdge;
          continue;
        } else {
          break;
        }
      } else {
        break;
      }
    }

    return Promise.resolve(false);
  }

  async *prefixesOf(key: Uint8Array): AsyncIterable<[Uint8Array, ValueType]> {
    let node = this.root;

    while (true) {
      const lcp = getLongestCommonPrefix(node.key, key);

      // Is this thing a prefix of ours?
      if (lcp.byteLength === key.byteLength) {
        break;
      } else if (
        node.key.byteLength === 0 ||
        lcp.byteLength === node.key.byteLength
      ) {
        if (node.value !== null) {
          yield [node.key, node.value];
        }

        // Check if any children along its edges
        const edge = key[lcp.byteLength];
        const childAtEdge = node.children.get(edge);

        if (childAtEdge) {
          // Node is now that child.
          node = childAtEdge;
          continue;
        } else {
          // Nothing left, stop iterating.
          break;
        }
      } else {
        break;
      }
    }
  }

  async *prefixedBy(key: Uint8Array): AsyncIterable<[Uint8Array, ValueType]> {
    let searchNode = this.root;
    let firstPrefixed: MemoryNode<ValueType> | null = null;

    // find the first thing that is the key, or the prefix of the key

    // and then all the children of that are prefixed by us

    while (true) {
      const lcp = getLongestCommonPrefix(key, searchNode.key);

      // Is this thing a prefix of ours?
      if (
        lcp.byteLength === key.byteLength
      ) {
        firstPrefixed = searchNode;
      }

      if (
        searchNode.key.byteLength === 0 ||
        lcp.byteLength > 0
      ) {
        // Check if any children along its edges
        const edge = key[lcp.byteLength];
        const childAtEdge = searchNode.children.get(edge);

        if (childAtEdge) {
          // Node is now that child.
          searchNode = childAtEdge;
          continue;
        } else {
          // Nothing left, stop iterating.
          break;
        }
      } else {
        break;
      }
    }

    if (firstPrefixed) {
      if (
        firstPrefixed.value !== null &&
        firstPrefixed.key.byteLength > key.byteLength
      ) {
        yield [firstPrefixed.key, firstPrefixed.value];
      }

      // iterate through all children.
      for (const node of this.allNodesLnr(firstPrefixed)) {
        if (node.value !== null) {
          yield [node.key, node.value];
        }
      }
    }
  }

  private *allNodesLnr(
    node: MemoryNode<ValueType>,
  ): Iterable<MemoryNode<ValueType>> {
    for (const [_key, child] of (node.children)) {
      yield child;

      for (const node of this.allNodesLnr(child)) {
        yield node;
      }
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
