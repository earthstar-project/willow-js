import { RedBlackTree } from "https://deno.land/std@0.188.0/collections/red_black_tree.ts";
import { RedBlackNode } from "https://deno.land/std@0.188.0/collections/red_black_node.ts";

import { KeyPart, KvBatch, KvDriver } from "./types.ts";

/**
 * We use the RB tree implementation from the deno standard library. It is generic only over a single type,
 * but we want a key-value mapping. We cal a pair of a key and a value a `PhysicalKey`.
 */
type PhysicalKey<Key, Value> = {
  key: Key;
  value: Value;
};

/**
 * A thin wrapper around RedBlackTree, with proper key/value typing; also giving us access to protected implementation details.
 */
class UsefulTree<Key, Value> extends RedBlackTree<PhysicalKey<Key, Value>> {
  cmp: (
    a: PhysicalKey<Key, Value>,
    b: PhysicalKey<Key, Value>,
  ) => number;

  keyCmp: (a: Key, b: Key) => number;

  constructor(keyCompare: (a: Key, b: Key) => number) {
    super((x: PhysicalKey<Key, Value>, y: PhysicalKey<Key, Value>) =>
      keyCompare(x.key, y.key)
    );
    this.cmp = (a, b) => keyCompare(a.key, b.key);
    this.keyCmp = keyCompare;
  }

  /**
   * Find the least node that matches a predicate.
   *
   * Assumens that the predicate is false up until some key and true from then on.
   */
  findLeastMatching(
    pred: (k: Key) => boolean,
  ): RedBlackNode<PhysicalKey<Key, Value>> | null {
    let node = this.root;
    let leastSoFar = null;

    while (node !== null) {
      const matched = pred(node.value.key);

      if (matched) {
        leastSoFar = node;
        node = node.left;
      } else {
        node = node.right;
      }
    }

    return leastSoFar;
  }

  /**
   * Find the greatest node that matches a predicate.
   *
   * Assumens that the predicate is true up until some key and false from then on.
   */
  findGreatestMatching(
    pred: (k: Key) => boolean,
  ): RedBlackNode<PhysicalKey<Key, Value>> | null {
    let node = this.root;
    let greatest = null;

    while (node !== null) {
      const matched = pred(node.value.key);

      if (matched) {
        greatest = node;
        node = node.right;
      } else {
        node = node.left;
      }
    }

    return greatest;
  }
}

/**
 * Find the least node greater than the given node.
 */
function nextGreaterNode<Key, Value>(
  node_: RedBlackNode<PhysicalKey<Key, Value>>,
): RedBlackNode<PhysicalKey<Key, Value>> | null {
  let node: RedBlackNode<PhysicalKey<Key, Value>> | null = node_;

  if (node.right === null) {
    // No right child, so node is in the subtree of the next greater node.
    let prevNode: RedBlackNode<PhysicalKey<Key, Value>> | null = node;
    node = node.parent;

    while (node !== null) {
      if (node.right === prevNode) {
        // prevNode was a right child of its parent, so continue.
        prevNode = node;
        node = node.parent;
      } else {
        // prevNode was a left child, so its parent is the next node
        return node;
      }
    }

    // Reached root of the tree.
    return null;
  } else {
    // There is a right child, we need to find the least node in its subtree.
    node = node.right;

    while (node.left !== null) {
      node = node.left;
    }

    return node;
  }
}

/**
 * Find the greatest node less than the given node.
 */
function nextLesserNode<Key, Value>(
  node_: RedBlackNode<PhysicalKey<Key, Value>>,
): RedBlackNode<PhysicalKey<Key, Value>> | null {
  let node: RedBlackNode<PhysicalKey<Key, Value>> | null = node_;

  if (node.left === null) {
    // No left child, so node is in the subtree of the next lesser node.
    let prevNode: RedBlackNode<PhysicalKey<Key, Value>> | null = node;
    node = node.parent;

    while (node !== null) {
      if (node.left === prevNode) {
        // prevNode was a left child of its parent, so continue.
        prevNode = node;
        node = node.parent;
      } else {
        // prevNode was a right child, so its parent is the next node
        return node;
      }
    }

    // Reached root of the tree.
    return null;
  } else {
    // There is a left child, we need to find the greatest node in its subtree.
    node = node.left;

    while (node.right !== null) {
      node = node.right;
    }

    return node;
  }
}

/**
 * An in-memory kv store. No persistence involved at all.
 */
export class KvDriverInMemory<Key extends KeyPart[], Value>
  implements KvDriver<Key, Value> {
  // This is a fairly thin wrapper around a RB tree from the deno standard library.
  private tree: UsefulTree<Key, Value>;

  private isKeyPrefixOf: (possiblePrefix: Key, keyToCompareTo: Key) => boolean;

  constructor(
    keyCompare: (a: Key, b: Key) => number,
    isKeyPrefixOf: (possiblePrefix: Key, keyToCompareTo: Key) => boolean,
  ) {
    this.tree = new UsefulTree<Key, Value>(keyCompare);
    this.isKeyPrefixOf = isKeyPrefixOf;
  }

  get(key: Key): Promise<Value | undefined> {
    const lookup = this.tree.find({ key, value: "unused" as Value });
    return Promise.resolve(lookup === null ? undefined : lookup.value);
  }

  set(key: Key, value: Value): Promise<void> {
    this.tree.insert({ key, value });
    return Promise.resolve();
  }

  delete(key: Key): Promise<void> {
    this.tree.remove({ key, value: "unused" as Value });
    return Promise.resolve();
  }

  async *list(
    selector: { start: Key; end: Key } | { prefix: Key },
    opts: {
      reverse?: boolean | undefined;
      limit?: number | undefined;
      batchSize?: number | undefined;
    } | undefined = { reverse: false, limit: undefined, batchSize: undefined },
  ): AsyncIterable<{ key: Key; value: Value }> {
    const first = opts.reverse
      ? ("end" in selector
        ? this.tree.findGreatestMatching((k) =>
          this.tree.keyCmp(k, selector.end) < 0
        )
        : this.tree.findGreatestMatching((k) =>
          this.isKeyPrefixOf(selector.prefix, k)
        ))
      : ("start" in selector
        ? this.tree.findLeastMatching((k) =>
          this.tree.keyCmp(k, selector.end) >= 0
        )
        : this.tree.findLeastMatching((k) =>
          this.isKeyPrefixOf(selector.prefix, k)
        ));

    const stillInRange = (k: Key) => {
      return opts.reverse
        ? ("start" in selector
          ? (this.tree.keyCmp(k, selector.start) >= 0)
          : (this.isKeyPrefixOf(selector.prefix, k)))
        : ("end" in selector
          ? (this.tree.keyCmp(k, selector.end) < 0)
          : (this.isKeyPrefixOf(selector.prefix, k)));
    };

    let count = 0;
    let node = first;

    while (node !== null && stillInRange(node.value.key)) {
      count += 1;
      if (opts.limit !== undefined && count > opts.limit) {
        return;
      }

      yield node.value;
      node = opts.reverse ? nextLesserNode(node) : nextGreaterNode(node);
    }

    return;
  }

  clear(
    opts?: { prefix: Key; start: Key; end: Key } | undefined,
  ): Promise<void> {
    if (opts === undefined) {
      this.tree.clear();
    } else {
      const predicate = (k: Key) => {
        return this.isKeyPrefixOf(opts.prefix, k) &&
          (this.tree.keyCmp(k, opts.start) >= 0);
      };

      let node = this.tree.findLeastMatching(predicate);
      while (
        node !== null && (this.tree.keyCmp(node.value.key, opts.end) < 0)
      ) {
        this.tree.remove(node.value);
        node = this.tree.findLeastMatching(predicate);
      }
    }

    return Promise.resolve();
  }

  batch(): KvBatch<Key, Value> {
    const operations: BatchOperation<Key, Value>[] = [];

    return {
      set: (key: Key, value: Value) => operations.push({ set: { key, value } }),
      delete: (key: Key) => operations.push({ delete: { key } }),
      commit: () => {
        for (const operation of operations) {
          if ("set" in operation) {
            this.set(operation.set.key, operation.set.value);
          } else {
            this.delete(operation.delete.key);
          }
        }

        return Promise.resolve();
      },
    };
  }
}

type BatchOperation<Key, Value> = { set: { key: Key; value: Value } } | {
  delete: { key: Key };
};
