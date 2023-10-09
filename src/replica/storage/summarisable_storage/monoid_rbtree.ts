import { RedBlackTree } from "https://deno.land/std@0.174.0/collections/red_black_tree.ts";
import {
  Direction,
  RedBlackNode,
} from "https://deno.land/std@0.188.0/collections/red_black_node.ts";
import { deferred } from "$std/async/deferred.ts";
import { combineMonoid, LiftingMonoid, sizeMonoid } from "./lifting_monoid.ts";
import { SummarisableStorage } from "./types.ts";

const debug = false;

/** A node for a FingerprintTree, augmented with a label and lifted value. Can update the labels of its ancestors. */
class MonoidTreeNode<
  ValueType = string,
  LiftType = string,
> extends RedBlackNode<ValueType> {
  declare parent: MonoidTreeNode<ValueType, LiftType> | null;
  declare left: MonoidTreeNode<ValueType, LiftType> | null;
  declare right: MonoidTreeNode<ValueType, LiftType> | null;

  label: LiftType;
  liftedValue: LiftType;

  isReady = deferred();

  private monoid: LiftingMonoid<ValueType, LiftType>;

  constructor(
    parent: MonoidTreeNode<ValueType, LiftType> | null,
    value: ValueType,
    monoid: LiftingMonoid<ValueType, LiftType>,
  ) {
    super(parent, value);

    this.label = monoid.neutral;
    this.liftedValue = monoid.neutral;
    this.monoid = monoid;

    this.monoid.lift(value).then((liftedValue) => {
      this.liftedValue = liftedValue;
      this.isReady.resolve();
    });
  }

  async updateLiftedValue() {
    this.liftedValue = await this.monoid.lift(this.value);

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
class RbTreeBase<ValueType, LiftedType> extends RedBlackTree<ValueType> {
  declare protected root:
    | NodeType<ValueType, LiftedType>
    | null;

  monoid: LiftingMonoid<
    ValueType,
    CombinedLabel<ValueType, LiftedType>
  >;

  private cachedMinNode: NodeType<ValueType, LiftedType> | null = null;

  constructor(
    /** The lifting monoid which is used to label nodes and derive fingerprints from ranges. */
    monoid: LiftingMonoid<ValueType, LiftedType>,
    /** A function to sort values by. Will use JavaScript's default comparison if not provided. */
    compare: (a: ValueType, b: ValueType) => number,
  ) {
    super(compare);

    const maxMonoid = {
      lift: (v: ValueType) => Promise.resolve(v),
      combine: (
        a: ValueType | undefined,
        b: ValueType | undefined,
      ): ValueType => {
        if (a === undefined && b === undefined) {
          return undefined as never;
        }

        if (b === undefined) {
          return a as ValueType;
        }

        if (a === undefined) {
          return b;
        }

        return compare(a, b) > 0 ? a : b;
      },
      neutral: undefined,
    } as LiftingMonoid<ValueType, ValueType>;

    this.monoid = combineMonoid(
      monoid,
      combineMonoid(sizeMonoid, maxMonoid),
    );
  }

  print(node?: NodeType<ValueType, LiftedType>) {
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
  getLowestValue(): ValueType {
    if (!this.root) {
      throw new Error("Can't get a range from a tree with no items");
    }

    if (this.cachedMinNode) {
      return this.cachedMinNode.value;
    }

    return this.root.findMinNode().value;
  }

  rotateNode(
    node: NodeType<ValueType, LiftedType>,
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

    const replacement: NodeType<ValueType, LiftedType> =
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
    parent: NodeType<ValueType, LiftedType> | null,
    current: NodeType<ValueType, LiftedType> | null,
  ) {
    while (parent && !current?.red) {
      const direction: Direction = parent.left === current ? "left" : "right";
      const siblingDirection: Direction = direction === "right"
        ? "left"
        : "right";
      let sibling: NodeType<ValueType, LiftedType> | null =
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
    value: ValueType,
  ): Promise<NodeType<ValueType, LiftedType> | null> {
    if (!this.root) {
      const newNode = new MonoidTreeNode(null, value, this.monoid);
      await newNode.isReady;
      this.root = newNode;
      this._size++;
      this.cachedMinNode = this.root;
      return this.root;
    } else {
      let node: NodeType<ValueType, LiftedType> = this.root;

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
          const newNode = new MonoidTreeNode(node, value, this.monoid);
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
  async insertMonoid(value: ValueType): Promise<boolean> {
    const originalNode = await this.insertFingerprintNode(
      value,
    );

    let node = originalNode;

    if (node) {
      while (node.parent?.red) {
        let parent: NodeType<ValueType, LiftedType> = node
          .parent!;
        const parentDirection: Direction = parent.directionFromParent()!;
        const uncleDirection: Direction = parentDirection === "right"
          ? "left"
          : "right";

        // The uncle is the sibling on the same side of the parent's parent.
        const uncle:
          | NodeType<ValueType, LiftedType>
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
    node: NodeType<ValueType, LiftedType>,
  ): Promise<NodeType<ValueType, LiftedType> | null> {
    /**
     * The node to physically remove from the tree.
     * Guaranteed to have at most one child.
     */

    const flaggedNode: NodeType<ValueType, LiftedType> | null =
      !node.left || !node.right
        ? node
        : node.findSuccessorNode()! as NodeType<ValueType, LiftedType>;

    /** Replaces the flagged node. */
    const replacementNode: NodeType<ValueType, LiftedType> | null =
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
        ValueType,
        LiftedType
      >;
    }

    flaggedNode.parent?.updateLabel(true);

    this._size--;
    return flaggedNode;
  }

  /** Remove a value frem the tree. Will recalculate labels for all rotated and parent nodes. */
  async removeMonoid(value: ValueType): Promise<boolean> {
    const node = this.findNode(
      value,
    ) as (NodeType<ValueType, LiftedType> | null);

    if (!node) {
      return false;
    }

    const removedNode = await this.removeMonoidNode(node) as (
      | NodeType<ValueType, LiftedType>
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
    x: ValueType,
    y: ValueType,
    nextTree?: NodeType<ValueType, LiftedType>,
  ): Promise<{
    /** The fingeprint of this range. */
    fingerprint: LiftedType;
    /** The size of the range. */
    size: number;
    /** A tree to be used for a subsequent call of `getFingerprint`, where the given y param for the previous call is the x param for the next one. */
    nextTree: NodeType<ValueType, LiftedType> | null;
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
      ) as NodeType<ValueType, LiftedType>;

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
    } else {
      // A sub-range where the upper bound is less than the upper bound.
      // e.g. [c, a) or [c, b);

      const minNode = this.cachedMinNode!;
      const maxValue = this.root.label[1][1];

      const willHaveHead = this.compare(y, minNode.value) > 0;
      const willHaveTail = this.compare(x, maxValue) <= 0;

      if (willHaveHead && willHaveTail) {
        const { label: firstLabel, nextTree: firstTree } = this.aggregateUntil(
          minNode as NodeType<ValueType, LiftedType>,
          minNode.value,
          y,
        );

        const maxLifted = await this.monoid.lift(maxValue);

        const synthesisedLabel = [maxLifted[0], [1, maxValue]] as CombinedLabel<
          ValueType,
          LiftedType
        >;

        const nodeToPass = nextTree || this.findGteNode(
          x,
        ) as NodeType<ValueType, LiftedType>;

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
          minNode as NodeType<ValueType, LiftedType>,
          minNode.value,
          y,
        );

        if (this.compare(x, maxValue) === 0) {
          const maxLifted = await this.monoid.lift(maxValue);

          const synthesisedLabel = [maxLifted[0], [
            1,
            maxValue,
          ]] as CombinedLabel<ValueType, LiftedType>;

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
        ) as NodeType<ValueType, LiftedType>;

        const { label } = this.aggregateUntil(
          nodeToPass,
          x,
          maxValue,
        );

        const maxLifted = await this.monoid.lift(maxValue);

        const synthesisedLabel = [maxLifted[0], [1, maxValue]] as CombinedLabel<
          ValueType,
          LiftedType
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
  }

  /** Find the first node holding a value greater than or equal to the given value. */
  private findGteNode(
    value: ValueType,
  ): NodeType<ValueType, LiftedType> | null {
    let node: NodeType<ValueType, LiftedType> | null = this.root;
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
    node: NodeType<ValueType, LiftedType>,
    x: ValueType,
    y: ValueType,
  ): {
    label: CombinedLabel<ValueType, LiftedType>;
    nextTree: NodeType<ValueType, LiftedType> | null;
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
    node: NodeType<ValueType, LiftedType>,
    x: ValueType,
    y: ValueType,
  ): {
    label: CombinedLabel<ValueType, LiftedType>;
    nextTree: NodeType<ValueType, LiftedType> | null;
  } {
    let acc: CombinedLabel<ValueType, LiftedType> = this.monoid.neutral;
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
    node: NodeType<ValueType, LiftedType> | null,
    y: ValueType,
    acc: CombinedLabel<ValueType, LiftedType>,
  ): {
    label: CombinedLabel<ValueType, LiftedType>;
    nextTree: NodeType<ValueType, LiftedType> | null;
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

  isValueEqual(a: ValueType, b: ValueType): boolean {
    return this.compare(a, b) === 0;
  }

  *values(
    start: ValueType | undefined,
    end: ValueType | undefined,
    opts?: {
      limit?: number;
      reverse?: boolean;
    },
  ): Iterable<ValueType> {
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
        const nodes: NodeType<ValueType, LiftedType>[] = [];
        let node: NodeType<ValueType, LiftedType> | null = this.root;

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
          const nodes: NodeType<ValueType, LiftedType>[] = [];
          let node: NodeType<ValueType, LiftedType> | null = this.root;

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
          const nodes: NodeType<ValueType, LiftedType>[] = [];
          let node: NodeType<ValueType, LiftedType> | null = this.root;

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
      const nodes: NodeType<ValueType, LiftedType>[] = [];
      let node: NodeType<ValueType, LiftedType> | null = this.root;

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
        const nodes: NodeType<ValueType, LiftedType>[] = [];
        let node: NodeType<ValueType, LiftedType> | null = this.root;

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
        const nodes: NodeType<ValueType, LiftedType>[] = [];
        let node: NodeType<ValueType, LiftedType> | null = this.root;

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

type MonoidRbTreeOpts<ValueType, LiftedType> = {
  /** The lifting monoid which is used to label nodes and derive fingerprints from ranges. */
  monoid: LiftingMonoid<ValueType, LiftedType>;
  compare: (a: ValueType, b: ValueType) => number;
};

export class MonoidRbTree<ValueType, LiftedType>
  implements SummarisableStorage<ValueType, LiftedType> {
  private tree: RbTreeBase<ValueType, LiftedType>;

  private valueMapping = new Map<ValueType, Uint8Array>();

  constructor(opts: MonoidRbTreeOpts<ValueType, LiftedType>) {
    this.tree = new RbTreeBase(opts.monoid, opts.compare);
  }

  get(key: ValueType): Promise<Uint8Array | undefined> {
    const res = this.tree.find(key);

    if (res) {
      return Promise.resolve(this.valueMapping.get(res));
    }

    return Promise.resolve(undefined);
  }

  async insert(key: ValueType, value: Uint8Array): Promise<void> {
    await this.tree.insertMonoid(key);
    this.valueMapping.set(key, value);
  }

  async summarise(
    start: ValueType,
    end: ValueType,
  ): Promise<{ fingerprint: LiftedType; size: number }> {
    const res = await this.tree.getFingerprint(start, end);

    return { fingerprint: res.fingerprint, size: res.size };
  }

  remove(key: ValueType): Promise<boolean> {
    return this.tree.removeMonoid(key);
  }

  async *entries(
    start: ValueType | undefined,
    end: ValueType | undefined,
    opts?: {
      reverse?: boolean;
      limit?: number;
    },
  ): AsyncIterable<{ key: ValueType; value: Uint8Array }> {
    for (const key of this.tree.values(start, end, opts)) {
      yield { key, value: this.valueMapping.get(key) as Uint8Array };
    }
  }

  async *allEntries(): AsyncIterable<{ key: ValueType; value: Uint8Array }> {
    for (const key of this.tree.lnrValues()) {
      yield { key, value: this.valueMapping.get(key) as Uint8Array };
    }
  }

  print() {
    this.tree.print();
  }
}
