import { RedBlackTree } from "https://deno.land/std@0.188.0/collections/red_black_tree.ts";
import { RedBlackNode } from "https://deno.land/std@0.188.0/collections/red_black_node.ts";

import {
  compareKeys,
  isFirstKeyPrefixOfSecondKey,
  KvBatch,
  KvDriver,
  KvKey,
} from "./types.ts";

/**
 * We use the RB tree implementation from the deno standard library. It is generic only over a single type,
 * but we want a key-value mapping. We cal a pair of a key and a value a `PhysicalKey`.
 */
type PhysicalKey = {
  key: KvKey;
  value: unknown;
};

/**
 * A thin wrapper around RedBlackTree, with proper key/value typing; also giving us access to protected implementation details.
 */
class UsefulTree extends RedBlackTree<PhysicalKey> {
  constructor() {
    super((x: PhysicalKey, y: PhysicalKey) => compareKeys(x.key, y.key));
  }

  /**
   * Find the least node that matches a predicate.
   *
   * Assumens that the predicate is false up until some key and true from then on.
   */
  findLeastMatching(
    pred: (k: KvKey) => boolean,
  ): RedBlackNode<PhysicalKey> | null {
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
    pred: (k: KvKey) => boolean,
  ): RedBlackNode<PhysicalKey> | null {
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

    return <RedBlackNode<PhysicalKey> | null> greatest;
  }
}

/**
 * Find the least node greater than the given node.
 */
function nextGreaterNode(
  node_: RedBlackNode<PhysicalKey>,
): RedBlackNode<PhysicalKey> | null {
  let node: RedBlackNode<PhysicalKey> | null = node_;

  if (node.right === null) {
    // No right child, so node is in the subtree of the next greater node.
    let prevNode: RedBlackNode<PhysicalKey> | null = node;
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
function nextLesserNode(
  node_: RedBlackNode<PhysicalKey>,
): RedBlackNode<PhysicalKey> | null {
  let node: RedBlackNode<PhysicalKey> | null = node_;

  if (node.left === null) {
    // No left child, so node is in the subtree of the next lesser node.
    let prevNode: RedBlackNode<PhysicalKey> | null = node;
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
export class KvDriverInMemory implements KvDriver {
  // This is a fairly thin wrapper around a RB tree from the deno standard library.
  private tree: UsefulTree;

  constructor() {
    this.tree = new UsefulTree();
  }

  get<Value>(key: KvKey): Promise<Value | undefined> {
    const lookup = this.tree.find({ key, value: "unused" });
    return Promise.resolve(lookup === null ? undefined : <Value> lookup.value);
  }

  set<Value>(key: KvKey, value: Value): Promise<void> {
    this.tree.remove({ key, value: "unused" });
    this.tree.insert({ key, value });
    return Promise.resolve();
  }

  delete(key: KvKey): Promise<boolean> {
    return Promise.resolve(this.tree.remove({ key, value: "unused" }));
  }

  async *list<Value>(
    selector: { start?: KvKey; end?: KvKey; prefix?: KvKey },
    opts: {
      reverse?: boolean | undefined;
      limit?: number | undefined;
      batchSize?: number | undefined;
    } | undefined = { reverse: false, limit: undefined, batchSize: undefined },
  ): AsyncIterable<{ key: KvKey; value: Value }> {
    const prefix = selector.prefix ?? <KvKey> <unknown> [];
    const first = opts.reverse
      ? this.tree.findGreatestMatching((k) =>
        (selector.end ? compareKeys(k, selector.end) < 0 : true) &&
        ((compareKeys(k, prefix) <= 0) ||
          isFirstKeyPrefixOfSecondKey(prefix, k))
      )
      : this.tree.findLeastMatching((k) =>
        (selector.start ? compareKeys(k, selector.start) >= 0 : true) &&
        ((compareKeys(k, prefix) >= 0) ||
          isFirstKeyPrefixOfSecondKey(prefix, k))
      );

    const stillInRange = (k: KvKey) => {
      return opts.reverse
        ? (isFirstKeyPrefixOfSecondKey(prefix, k) &&
          (selector.start ? compareKeys(k, selector.start) >= 0 : true))
        : (isFirstKeyPrefixOfSecondKey(prefix, k) &&
          (selector.end ? compareKeys(k, selector.end) < 0 : true));
    };

    let count = 0;
    let node = first;

    while (node !== null && stillInRange(node.value.key)) {
      count += 1;
      if (opts.limit !== undefined && count > opts.limit) {
        return;
      }

      yield (<{ key: KvKey; value: Value }> node.value);
      node = opts.reverse ? nextLesserNode(node) : nextGreaterNode(node);
    }

    return;
  }

  clear(
    opts?: { prefix?: KvKey; start?: KvKey; end?: KvKey } | undefined,
  ): Promise<void> {
    if (opts === undefined) {
      this.tree.clear();
    } else {
      const prefix = opts.prefix ?? <KvKey> <unknown> [];
      const predicate = (k: KvKey) => {
        return (compareKeys(k, prefix) >= 0) &&
          (opts.start ? (compareKeys(k, opts.start) >= 0) : true);
      };

      let node = this.tree.findLeastMatching(predicate);
      while (
        node !== null &&
        isFirstKeyPrefixOfSecondKey(prefix, node.value.key) &&
        (opts.end ? compareKeys(node.value.key, opts.end) < 0 : true)
      ) {
        this.tree.remove(node.value);
        node = this.tree.findLeastMatching(predicate);
      }
    }

    return Promise.resolve();
  }

  batch(): KvBatch {
    const operations: BatchOperation[] = [];

    return {
      set: <Value>(key: KvKey, value: Value) =>
        operations.push({ set: { key, value } }),
      delete: (key: KvKey) => operations.push({ delete: { key } }),
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

type BatchOperation = { set: { key: KvKey; value: unknown } } | {
  delete: { key: KvKey };
};
