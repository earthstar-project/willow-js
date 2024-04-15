import { RedBlackTree } from "https://deno.land/std@0.174.0/collections/red_black_tree.ts";
import {
  Direction,
  RedBlackNode,
} from "https://deno.land/std@0.188.0/collections/red_black_node.ts";

import { combineMonoid, LiftingMonoid, sizeMonoid } from "./lifting_monoid.ts";
import { SummarisableStorage } from "./types.ts";
import { deferred } from "../../../../deps.ts";

const debug = false;

/** A node for a FingerprintTree, augmented with a label and lifted value. Can update the labels of its ancestors. */
class MonoidTreeNode<
  Key = string,
  Value = Uint8Array,
  Summary = string,
> extends RedBlackNode<Key> {
  declare parent: MonoidTreeNode<Key, Value, Summary> | null;
  declare left: MonoidTreeNode<Key, Value, Summary> | null;
  declare right: MonoidTreeNode<Key, Value, Summary> | null;

  label: Summary;
  liftedValue: Summary;

  isReady = deferred();

  private monoid: LiftingMonoid<[Key, Value], Summary>;
  private valueMapping: Map<Key, Value>;

  constructor(
    parent: MonoidTreeNode<Key, Value, Summary> | null,
    key: Key,
    monoid: LiftingMonoid<[Key, Value], Summary>,
    valueMapping: Map<Key, Value>,
  ) {
    super(parent, key);

    this.label = monoid.neutral;
    this.liftedValue = monoid.neutral;
    this.monoid = monoid;
    this.valueMapping = valueMapping;

    const data = valueMapping.get(key)!;

    this.monoid.lift([key, data]).then((liftedValue) => {
      this.liftedValue = liftedValue;
      this.isReady.resolve();
    });
  }

  async updateLiftedValue() {
    const data = this.valueMapping.get(this.value)!;

    this.liftedValue = await this.monoid.lift([this.value, data]);

    this.updateLabel(true, "updated lifted value");
  }

  updateLabel(updateParent = true, reason?: string) {
    // Update our label

    if (this.left !== null && this.right === null) {
      this.label = this.monoid.combine(
        this.left.label,
        this.liftedValue,
      );
    } else if (this.left === null && this.right !== null) {
      this.label = this.monoid.combine(
        this.liftedValue,
        this.right.label,
      );
    } else if (this.left && this.right) {
      this.label = this.monoid.combine(
        this.left.label,
        this.monoid.combine(
          this.liftedValue,
          this.right.label,
        ),
      );
    } else {
      this.label = this.liftedValue;
    }

    if (debug) {
      if (reason) {
        console.log(reason);
      }
      console.group("Updating...", this.value);
      console.log("Lifted value", this.liftedValue);
      console.log(
        "Label L",
        this.left?.label || this.monoid.neutral,
      );
      console.log(
        "Label R",
        this.right?.label || this.monoid.neutral,
      );
      console.log("Label", this.label);
      console.groupEnd();
    }

    // Update all parent labels all the way to the top...
    if (updateParent) {
      this.parent?.updateLabel(true, "Updated by child");
    }
  }
}

// Lifted type of range, size of range, items in range, max node in range.
type CombinedLabel<V, L> = [L, [number, V]];
type NodeType<V, L> = MonoidTreeNode<V, CombinedLabel<V, L>>;

/** A self-balancing tree which can return fingerprints for ranges of items it holds using a provided monoid. */
class RbTreeBase<Key, Value, Summary> extends RedBlackTree<Key> {
  declare protected root:
    | NodeType<Key, Summary>
    | null;

  monoid: LiftingMonoid<
    [Key, Value],
    CombinedLabel<Key, Summary>
  >;

  private cachedMinNode: NodeType<Key, Summary> | null = null;

  private valueMapping: Map<Key, Value>;

  constructor(
    /** The lifting monoid which is used to label nodes and derive fingerprints from ranges. */
    monoid: LiftingMonoid<[Key, Value], Summary>,
    /** A function to sort values by. Will use JavaScript's default comparison if not provided. */
    compare: (a: Key, b: Key) => number,
    valueMapping: Map<Key, Value>,
  ) {
    super(compare);

    this.valueMapping = valueMapping;

    const maxMonoid = {
      lift: (v: [Key, Value]) => Promise.resolve(v),
      combine: (
        a: [Key, Value] | undefined,
        b: [Key, Value] | undefined,
      ): [Key, Value] => {
        if (a === undefined && b === undefined) {
          return undefined as never;
        }

        if (b === undefined) {
          return a as [Key, Value];
        }

        if (a === undefined) {
          return b;
        }

        return compare(a[0], b[0]) > 0 ? a : b;
      },
      neutral: undefined as [Key, Value],
    } as LiftingMonoid<[Key, Value], [Key, Value]>;

    this.monoid = combineMonoid(
      monoid,
      combineMonoid(sizeMonoid, maxMonoid),
    );
  }

  print(node?: NodeType<Key, Summary>) {
    const nodeToUse = node || this.root;

    if (!nodeToUse) {
      console.log("Empty");
      return;
    }

    console.group(
      `${nodeToUse.value} - ${nodeToUse.liftedValue[0]} - ${
        nodeToUse.label[0]
      }`,
    );

    if (nodeToUse.left) {
      console.log("left");
      this.print(nodeToUse.left);
    }

    if (nodeToUse.right) {
      console.log("right");
      this.print(nodeToUse.right);
    }

    console.groupEnd();
  }

  /** Return the lowest value within this tree. Useful for constructing the maximum range of the tree, which will be [x, x) where x is the result of this function. */
  getLowestValue(): Key {
    if (!this.root) {
      throw new Error("Can't get a range from a tree with no items");
    }

    if (this.cachedMinNode) {
      return this.cachedMinNode.value;
    }

    return this.root.findMinNode().value;
  }

  rotateNode(
    node: NodeType<Key, Summary>,
    direction: Direction,
  ) {
    const replacementDirection: Direction = direction === "left"
      ? "right"
      : "left";
    if (!node[replacementDirection]) {
      throw new TypeError(
        `cannot rotate ${direction} without ${replacementDirection} child`,
      );
    }

    if (debug) {
      console.group("Rotating", direction);
    }

    const replacement: NodeType<Key, Summary> =
      node[replacementDirection]!;
    node[replacementDirection] = replacement[direction] ?? null;

    // if the replacement has a node in the rotation direction
    // the node is now the parent of that node

    // so p.r (b) now has q as a parent
    if (replacement[direction]) {
      replacement[direction]!.parent = node;
    }

    // and p's parent is now q's parent (nothing)
    replacement.parent = node.parent;

    if (node.parent) {
      const parentDirection: Direction = node === node.parent[direction]
        ? direction
        : replacementDirection;
      node.parent[parentDirection] = replacement;
    } else {
      // the root is now p
      this.root = replacement;
    }

    // and p's r is now q
    replacement[direction] = node;

    // and q's parent is now p. wow.
    node.parent = replacement;

    replacement[direction]?.updateLabel(false, "Node rotated");
    replacement.updateLabel(false, "Node rotated");

    if (debug) {
      console.groupEnd();
    }
  }

  removeFixup(
    parent: NodeType<Key, Summary> | null,
    current: NodeType<Key, Summary> | null,
  ) {
    while (parent && !current?.red) {
      const direction: Direction = parent.left === current ? "left" : "right";
      const siblingDirection: Direction = direction === "right"
        ? "left"
        : "right";
      let sibling: NodeType<Key, Summary> | null =
        parent[siblingDirection];

      if (sibling?.red) {
        sibling.red = false;
        parent.red = true;
        this.rotateNode(parent, direction);
        sibling = parent[siblingDirection];
      }
      if (sibling) {
        if (!sibling.left?.red && !sibling.right?.red) {
          sibling!.red = true;
          current = parent;
          parent = current.parent;
        } else {
          if (!sibling[siblingDirection]?.red) {
            sibling[direction]!.red = false;
            sibling.red = true;
            this.rotateNode(sibling, siblingDirection);
            sibling = parent[siblingDirection!];
          }
          sibling!.red = parent.red;
          parent.red = false;
          sibling![siblingDirection]!.red = false;
          this.rotateNode(parent, direction);
          current = this.root;
          parent = null;
        }
      }
    }
    if (current) current.red = false;
  }

  private async insertFingerprintNode(
    value: Key,
  ): Promise<NodeType<Key, Summary> | null> {
    if (!this.root) {
      const newNode = new MonoidTreeNode(
        null,
        value,
        this.monoid,
        this.valueMapping,
      );
      await newNode.isReady;
      this.root = newNode;
      this._size++;
      this.cachedMinNode = this.root;
      return this.root;
    } else {
      let node: NodeType<Key, Summary> = this.root;

      let isMinNode = true;

      while (true) {
        const order: number = this.compare(value, node.value);

        if (order === 0) {
          isMinNode = false;

          break;
        }
        const direction: Direction = order < 0 ? "left" : "right";

        if (isMinNode && direction === "right") {
          isMinNode = false;
        }

        if (node[direction]) {
          node = node[direction]!;
        } else {
          const newNode = new MonoidTreeNode(
            node,
            value,
            this.monoid,
            this.valueMapping,
          );
          await newNode.isReady;
          node[direction] = newNode;
          this._size++;

          if (isMinNode) {
            this.cachedMinNode = node[direction];
          }

          return node[direction];
        }
      }
    }
    return null;
  }

  /** Insert a value into the tree. Will create a lifted value for the resulting node, and update the labels of all rotated and parent nodes in the tree. */
  async insertMonoid(value: Key): Promise<boolean> {
    const originalNode = await this.insertFingerprintNode(
      value,
    );

    let node = originalNode;

    if (node) {
      while (node.parent?.red) {
        let parent: NodeType<Key, Summary> = node
          .parent!;
        const parentDirection: Direction = parent.directionFromParent()!;
        const uncleDirection: Direction = parentDirection === "right"
          ? "left"
          : "right";

        // The uncle is the sibling on the same side of the parent's parent.
        const uncle:
          | NodeType<Key, Summary>
          | null = parent.parent![uncleDirection] ??
            null;

        if (uncle?.red) {
          parent.red = false;
          uncle.red = false;
          parent.parent!.red = true;

          node = parent.parent!;
        } else {
          if (node === parent[uncleDirection]) {
            node = parent;

            this.rotateNode(node, parentDirection);

            parent = node.parent!;
          }
          parent.red = false;
          parent.parent!.red = true;
          this.rotateNode(parent.parent!, uncleDirection);
        }
      }

      this.root!.red = false;
    }

    originalNode?.updateLabel(true, "Node inserted");

    return !!node;
  }

  async removeMonoidNode(
    node: NodeType<Key, Summary>,
  ): Promise<NodeType<Key, Summary> | null> {
    /**
     * The node to physically remove from the tree.
     * Guaranteed to have at most one child.
     */

    const flaggedNode: NodeType<Key, Summary> | null =
      !node.left || !node.right
        ? node
        : node.findSuccessorNode()! as NodeType<Key, Summary>;

    /** Replaces the flagged node. */
    const replacementNode: NodeType<Key, Summary> | null =
      flaggedNode.left ??
        flaggedNode.right;

    if (replacementNode) replacementNode.parent = flaggedNode.parent;
    if (!flaggedNode.parent) {
      this.root = replacementNode;
    } else {
      flaggedNode.parent[flaggedNode.directionFromParent()!] = replacementNode;
    }
    if (flaggedNode !== node) {
      /** Swaps values, in case value of the removed node is still needed by consumer. */

      const swapValue = node.value;
      node.value = flaggedNode.value;
      flaggedNode.value = swapValue;

      await node.updateLiftedValue();
    }

    if (
      this.root && flaggedNode === this.cachedMinNode ||
      node === this.cachedMinNode
    ) {
      this.cachedMinNode = this.root?.findMinNode() as NodeType<
        Key,
        Summary
      >;
    }

    flaggedNode.parent?.updateLabel(true);

    this._size--;
    return flaggedNode;
  }

  /** Remove a value frem the tree. Will recalculate labels for all rotated and parent nodes. */
  async removeMonoid(value: Key): Promise<boolean> {
    const node = this.findNode(
      value,
    ) as (NodeType<Key, Summary> | null);

    if (!node) {
      return false;
    }

    this.valueMapping.delete(value);

    const removedNode = await this.removeMonoidNode(node) as (
      | NodeType<Key, Summary>
      | null
    );

    if (removedNode && !removedNode.red) {
      this.removeFixup(
        removedNode.parent,
        removedNode.left ?? removedNode.right,
      );
    }

    return true;
  }

  /** Calculates a fingerprint of items within the given range, inclusive of xx and exclusive of y. Also returns the size of the range, the items contained within it, */
  async getFingerprint(
    x: Key,
    y: Key,
    nextTree?: NodeType<Key, Summary>,
  ): Promise<{
    /** The fingeprint of this range. */
    fingerprint: Summary;
    /** The size of the range. */
    size: number;
    /** A tree to be used for a subsequent call of `getFingerprint`, where the given y param for the previous call is the x param for the next one. */
    nextTree: NodeType<Key, Summary> | null;
  }> {
    if (this.root === null) {
      return {
        fingerprint: this.monoid.neutral[0],
        size: 0,
        nextTree: null,
      };
    }

    const order = this.compare(x, y);

    if (order === 0) {
      // The full range. Get the root label.
      return {
        fingerprint: this.root.label[0],
        size: this.root.label[1][0],
        nextTree: this.cachedMinNode,
      };
    } else if (order < 0) {
      const minNode = this.compare(x, this.cachedMinNode!.value) <= 0
        ? this.cachedMinNode
        : null;

      const nodeToPass = nextTree || minNode || this.findGteNode(
        x,
      ) as NodeType<Key, Summary>;

      const { label, nextTree: nextNextTree } = this.aggregateUntil(
        nodeToPass,
        x,
        y,
      );

      return {
        fingerprint: label[0],
        size: label[1][0],
        nextTree: nextNextTree,
      };
    }

    // A sub-range where the upper bound is less than the upper bound.
    // e.g. [c, a) or [c, b);

    const minNode = this.cachedMinNode!;
    const maxValue = this.root.label[1][1];

    const willHaveHead = this.compare(y, minNode.value) > 0;
    const willHaveTail = this.compare(x, maxValue) <= 0;

    if (willHaveHead && willHaveTail) {
      const { label: firstLabel, nextTree: firstTree } = this.aggregateUntil(
        minNode as NodeType<Key, Summary>,
        minNode.value,
        y,
      );

      const maxLifted = await this.monoid.lift(
        maxValue,
        this.valueMapping.get(maxValue),
      );

      const synthesisedLabel = [maxLifted[0], [1, maxValue]] as CombinedLabel<
        Key,
        Summary
      >;

      const nodeToPass = nextTree || this.findGteNode(
        x,
      ) as NodeType<Key, Summary>;

      const { label: lastLabel } = this.aggregateUntil(
        nodeToPass,
        x,
        maxValue,
      );

      const combinedLabel = this.monoid.combine(
        lastLabel,
        synthesisedLabel,
      );

      const newLabel = this.monoid.combine(firstLabel, combinedLabel);

      return {
        fingerprint: newLabel[0],
        size: newLabel[1][0],
        nextTree: firstTree,
      };
    } else if (willHaveHead) {
      const { label, nextTree } = this.aggregateUntil(
        minNode as NodeType<Key, Summary>,
        minNode.value,
        y,
      );

      if (this.compare(x, maxValue) === 0) {
        const maxLifted = await this.monoid.lift(
          maxValue,
          this.valueMapping.get(maxValue),
        );

        const synthesisedLabel = [maxLifted[0], [
          1,
          maxValue,
        ]] as CombinedLabel<Key, Summary>;

        const combinedLabel = this.monoid.combine(
          label,
          synthesisedLabel,
        );

        return {
          fingerprint: combinedLabel[0],
          size: combinedLabel[1][0],
          nextTree: nextTree,
        };
      }

      return {
        fingerprint: label[0],
        size: label[1][0],
        nextTree: nextTree,
      };
    } else if (willHaveTail) {
      const minNode = this.compare(x, this.cachedMinNode!.value) <= 0
        ? this.cachedMinNode
        : null;

      const nodeToPass = nextTree || minNode || this.findGteNode(
        x,
      ) as NodeType<Key, Summary>;

      const { label } = this.aggregateUntil(
        nodeToPass,
        x,
        maxValue,
      );

      const maxLifted = await this.monoid.lift(
        maxValue,
        this.valueMapping.get(maxValue),
      );

      const synthesisedLabel = [maxLifted[0], [1, maxValue]] as CombinedLabel<
        Key,
        Summary
      >;

      const combinedLabel = this.monoid.combine(
        label,
        synthesisedLabel,
      );

      return {
        fingerprint: combinedLabel[0],
        size: combinedLabel[1][0],
        nextTree: minNode,
      };
    } else {
      return {
        fingerprint: this.monoid.neutral[0],
        size: 0,
        nextTree: null,
      };
    }
  }

  /** Find the first node holding a value greater than or equal to the given value. */
  private findGteNode(
    value: Key,
  ): NodeType<Key, Summary> | null {
    let node: NodeType<Key, Summary> | null = this.root;
    while (node) {
      const order: number = this.compare(value, node.value);
      if (order === 0) break;
      const direction: "left" | "right" = order < 0 ? "left" : "right";

      if (node[direction]) {
        node = node[direction];
      } else {
        break;
      }
    }
    return node;
  }

  private aggregateUntil(
    node: NodeType<Key, Summary>,
    x: Key,
    y: Key,
  ): {
    label: CombinedLabel<Key, Summary>;
    nextTree: NodeType<Key, Summary> | null;
  } {
    const { label, nextTree } = this.aggregateUp(node, x, y);

    if (nextTree === null || this.compare(nextTree.value, y) >= 0) {
      return { label, nextTree };
    } else {
      return this.aggregateDown(
        nextTree.right,
        y,
        this.monoid.combine(label, nextTree.liftedValue),
      );
    }
  }

  private aggregateUp(
    node: NodeType<Key, Summary>,
    x: Key,
    y: Key,
  ): {
    label: CombinedLabel<Key, Summary>;
    nextTree: NodeType<Key, Summary> | null;
  } {
    let acc: CombinedLabel<Key, Summary> = this.monoid.neutral;
    let tree = node;

    while (this.compare(tree.label[1][1], y) < 0) {
      if (this.compare(tree.value, x) >= 0) {
        acc = this.monoid.combine(
          acc,
          this.monoid.combine(
            tree.liftedValue,
            tree.right?.label || this.monoid.neutral,
          ),
        );
      }

      if (tree.parent === null) {
        return { label: acc, nextTree: null };
      } else {
        tree = tree.parent;
      }
    }

    return { label: acc, nextTree: tree };
  }

  private aggregateDown(
    node: NodeType<Key, Summary> | null,
    y: Key,
    acc: CombinedLabel<Key, Summary>,
  ): {
    label: CombinedLabel<Key, Summary>;
    nextTree: NodeType<Key, Summary> | null;
  } {
    let tree = node;
    let acc2 = acc;

    while (tree !== null) {
      if (this.compare(tree.value, y) < 0) {
        acc2 = this.monoid.combine(
          acc2,
          this.monoid.combine(
            tree.left?.label || this.monoid.neutral,
            tree.liftedValue,
          ),
        );

        tree = tree.right;
      } else if (
        tree.left === null || this.compare(tree.label[1][1], y) < 0
      ) {
        return {
          label: this.monoid.combine(
            acc2,
            tree.left?.label || this.monoid.neutral,
          ),
          nextTree: tree,
        };
      } else {
        tree = tree.left;
      }
    }
    return { label: acc2, nextTree: null };
  }

  isValueEqual(a: Key, b: Key): boolean {
    return this.compare(a, b) === 0;
  }

  *values(
    start: Key | undefined,
    end: Key | undefined,
    opts?: {
      limit?: number;
      reverse?: boolean;
    },
  ): Iterable<Key> {
    let yielded = 0;
    const hitLimit = () => {
      if (opts?.limit) {
        yielded++;

        if (yielded >= opts.limit) {
          return true;
        }
      }

      return false;
    };

    if (
      start === undefined && end === undefined ||
      start && end && this.compare(start, end) === 0
    ) {
      if (opts?.reverse) {
        for (const entry of this.rnlValues()) {
          yield entry;
          if (hitLimit()) break;
        }
      } else {
        for (const entry of this.lnrValues()) {
          yield entry;
          if (hitLimit()) break;
        }
      }

      return;
    }

    const argOrder = start && end ? this.compare(start, end) : -1;

    if (opts?.reverse) {
      if (argOrder === -1) {
        // Collect from right to left, ignoring nodes lower than start and greater or equal to end.
        const nodes: NodeType<Key, Summary>[] = [];
        let node: NodeType<Key, Summary> | null = this.root;

        while (nodes.length || node) {
          if (node) {
            const startOrder = start ? this.compare(node.value, start) : 1;
            const endOrder = end ? this.compare(node.value, end) : -1;

            if (endOrder >= 0) {
              node = node.left;
            } else if (startOrder >= 0) {
              nodes.push(node);
              node = node.right;
            } else {
              node = node.right;
            }
          } else {
            node = nodes.pop()!;

            yield node.value;
            if (hitLimit()) break;
            node = node.left;
          }
        }
      } else {
        // second half
        // collect from right to left, ignoring values less than the start
        {
          const nodes: NodeType<Key, Summary>[] = [];
          let node: NodeType<Key, Summary> | null = this.root;

          while (nodes.length || node) {
            if (node) {
              const startOrder = start ? this.compare(node.value, start) : 1;

              if (startOrder < 0) {
                node = node.right;
              } else {
                nodes.push(node);
                node = node.right;
              }
            } else {
              node = nodes.pop()!;

              yield node.value;
              if (hitLimit()) break;
              node = node.left;
            }
          }
        }

        // first half
        // collect from right to left, ignoring values greater than the end
        {
          const nodes: NodeType<Key, Summary>[] = [];
          let node: NodeType<Key, Summary> | null = this.root;

          while (nodes.length || node) {
            if (node) {
              const endOrder = end ? this.compare(node.value, end) : -1;

              if (endOrder >= 0) {
                node = node.left;
              } else {
                nodes.push(node);
                node = node.right;
              }
            } else {
              node = nodes.pop()!;

              yield node.value;
              if (hitLimit()) break;
              node = node.left;
            }
          }
        }
      }

      return;
    }

    if (argOrder === -1) {
      const nodes: NodeType<Key, Summary>[] = [];
      let node: NodeType<Key, Summary> | null = this.root;

      // Collect from left to right, ignoring nodes lower than start and greater or equal to end.
      while (nodes.length || node) {
        if (node) {
          const startOrder = start ? this.compare(node.value, start) : 1;
          const endOrder = end ? this.compare(node.value, end) : -1;

          if (endOrder >= 0) {
            node = node.left;
          } else if (startOrder >= 0) {
            nodes.push(node);
            node = node.left;
          } else {
            node = node.right;
          }
        } else {
          node = nodes.pop()!;

          yield node.value;
          if (hitLimit()) break;
          node = node.right;
        }
      }
    } else {
      {
        const nodes: NodeType<Key, Summary>[] = [];
        let node: NodeType<Key, Summary> | null = this.root;

        while (nodes.length || node) {
          if (node) {
            const endOrder = end ? this.compare(node.value, end) : -1;

            if (endOrder >= 0) {
              node = node.left;
            } else {
              nodes.push(node);
              node = node.left;
            }
          } else {
            node = nodes.pop()!;

            yield node.value;
            if (hitLimit()) break;
            node = node.right;
          }
        }
      }

      {
        const nodes: NodeType<Key, Summary>[] = [];
        let node: NodeType<Key, Summary> | null = this.root;

        while (nodes.length || node) {
          if (node) {
            const startOrder = start ? this.compare(node.value, start) : 1;

            if (startOrder < 0) {
              node = node.right;
            } else {
              nodes.push(node);
              node = node.left;
            }
          } else {
            node = nodes.pop()!;

            yield node.value;
            if (hitLimit()) break;
            node = node.right;
          }
        }
      }
    }
  }
}

type MonoidRbTreeOpts<Key, Value, Summary> = {
  /** The lifting monoid which is used to label nodes and derive fingerprints from ranges. */
  monoid: LiftingMonoid<Key, Summary>;
  compare: (a: Key, b: Key) => number;
};

export class MonoidRbTree<Key, Value, Summary>
  implements SummarisableStorage<Key, Value, Summary> {
  private tree: RbTreeBase<Key, Summary>;

  private valueMapping = new Map<Key, Value>();

  constructor(opts: MonoidRbTreeOpts<Key, Value, Summary>) {
    this.tree = new RbTreeBase(opts.monoid, opts.compare, this.valueMapping);
  }

  get(key: Key): Promise<Value | undefined> {
    const res = this.tree.find(key);

    if (res) {
      return Promise.resolve(this.valueMapping.get(res));
    }

    return Promise.resolve(undefined);
  }

  async insert(key: Key, value: Value): Promise<void> {
    this.valueMapping.set(key, value);
    await this.tree.insertMonoid(key);
  }

  async summarise(
    start: Key,
    end: Key,
  ): Promise<{ fingerprint: Summary; size: number }> {
    const res = await this.tree.getFingerprint(start, end);

    return { fingerprint: res.fingerprint, size: res.size };
  }

  remove(key: Key): Promise<boolean> {
    return this.tree.removeMonoid(key);
  }

  async *entries(
    start: Key | undefined,
    end: Key | undefined,
    opts?: {
      reverse?: boolean;
      limit?: number;
    },
  ): AsyncIterable<{ key: Key; value: Value }> {
    for (const key of this.tree.values(start, end, opts)) {
      yield { key, value: this.valueMapping.get(key) as Value };
    }
  }

  async *allEntries(): AsyncIterable<{ key: Key; value: Value }> {
    for (const key of this.tree.lnrValues()) {
      yield { key, value: this.valueMapping.get(key) as Value };
    }
  }

  print() {
    this.tree.print();
  }
}
