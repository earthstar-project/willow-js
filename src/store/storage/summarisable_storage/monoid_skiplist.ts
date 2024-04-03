import { deferred } from "../../../../deps.ts";
import { KeyPart, KvDriver } from "../kv/types.ts";
import { combineMonoid, LiftingMonoid, sizeMonoid } from "./lifting_monoid.ts";
import { SummarisableStorage } from "./types.ts";

/*
This file implements a skiplist on top of a traditional kv-store.
The skiplist maps *logical keys* to *logical values*. These are stored as *physical values* in the kv-store, indexed by *physical keys*.
A pair of a logical key and its logical value is called a *logical entry*, a pair of a physical key and a physical value is called a *phyical entry*.

On the key value store, we create a physical entry for each node in the skip list. We sort these entries by layer first, logical key second.
*/

type PhysicalKey<LogicalKey extends KeyPart[]> = [
  number, /* which layer? */
  ...LogicalKey,
] | [number /* which layer? */];

function physicalKeyGetLayer<LogicalKey extends KeyPart[]>(
  pk: PhysicalKey<LogicalKey>,
): number {
  return pk[0];
}

function physicalKeyGetLogicalKey<LogicalKey extends KeyPart[]>(
  pk: PhysicalKey<LogicalKey>,
): LogicalKey {
  return <LogicalKey> <unknown> pk.slice(1);
}

/*
Further, the skiplist is summarisable: you can efficiently query for a *summary* of arbitrary ranges, consisting of the number of items in that range, as well as some accumulated monoidal value (see the range-based set reconciliation paper for more details).
To support this efficiently, each physical value consists not only of a logical value, but also of some metadata.
*/

type PhysicalValue<LogicalValue, SummaryData> = {
  /**
   * The LogicalValue that this entry stores. The only piece of non-metadata.
   *
   * Only the physical values on layer zero of the skip list store this, higher layers track metadata only. We forego static typing on this - apologies.
   */
  logicalValue?: LogicalValue;
  /**
   * The maximum layer in the skip list for which there is an entry with the same logical key.
   *
   * Tracked purely as an optimisation detail to be able to omit some kv queries.
   */
  maxLayer: number;
  /**
   * The number of logical entries on layer zero of the skip list whose key is greater than or equal to ours but strictly less than the key of the next entry on the same layer.
   */
  summaryCount: number;
  /**
   * The summary data of the logical entries on layer zero of the skip list whose key is greater than or equal to ours but strictly less than the key of the next entry on the same layer.
   */
  summaryData: SummaryData;
};

/**
 * Used internally to cache skiplist node information in memory.
 */
type Node<
  LogicalKey extends KeyPart[],
  LogicalValue,
  SummaryData,
> = {
  key: PhysicalKey<LogicalKey>;
  value: PhysicalValue<LogicalValue, SummaryData>;
};

// Take this to the power of l to get the probability that the skiplist has a node at layer l.
const LAYER_INSERT_PROBABILITY = 0.5;
// Hard limit on the number of layers, never insert above this.
const LAYER_LEVEL_LIMIT = 64;

type SkiplistOpts<
  LogicalKey extends KeyPart[],
  LogicalValue,
  SummaryData,
> = {
  /**
   * Return a negative number if a < b, zero if a == b, or a positive number if a > b.
   *
   * This **must** coincide with how the underlying kv store compares logical keys!
   */
  logicalKeyCompare: (a: LogicalKey, b: LogicalKey) => number;
  /**
   * Return true iff a and b are considered equal.
   */
  logicalValueEq: (a: LogicalValue, b: LogicalValue) => boolean;
  kv: KvDriver<
    PhysicalKey<LogicalKey>,
    PhysicalValue<LogicalValue, SummaryData>
  >;
  monoid: LiftingMonoid<LogicalKey, SummaryData>;
};

export class Skiplist<
  LogicalKey extends KeyPart[],
  LogicalValue,
  SummaryData,
> //implements SummarisableStorage<LogicalKey, SummaryData>
{
  private logicalKeyCompare: (a: LogicalKey, b: LogicalKey) => number;
  private logicalValueEq: (a: LogicalValue, b: LogicalValue) => boolean;
  private kv: KvDriver<
    PhysicalKey<LogicalKey>,
    PhysicalValue<LogicalValue, SummaryData>
  >;
  private _maxHeight = 0;
  private isSetup = deferred();
  private monoid: LiftingMonoid<LogicalKey, [SummaryData, number]>;

  private layerKeyIndex = 0;
  private valueKeyIndex = 1;

  constructor(opts: SkiplistOpts<LogicalKey, LogicalValue, SummaryData>) {
    this.kv = opts.kv;

    this.logicalKeyCompare = opts.logicalKeyCompare;
    this.logicalValueEq = opts.logicalValueEq;
    this.monoid = combineMonoid(opts.monoid, sizeMonoid);

    this.setup();
  }

  async print() {
    console.group("Skiplist contents");

    const map: Map<
      LogicalKey,
      Record<number, PhysicalValue<LogicalValue, SummaryData>>
    > = new Map();

    for await (
      const entry of this.kv.list(
        {
          start: [0],
          end: [await this.maxHeight() + 1],
        },
      )
    ) {
      const layer = physicalKeyGetLayer(entry.key);
      const key = physicalKeyGetLogicalKey(entry.key);

      const valueEntry = map.get(key);

      if (valueEntry) {
        map.set(key, {
          ...valueEntry,
          [layer]: entry.value,
        });

        continue;
      }

      map.set(key, {
        [layer]: entry.value,
      });
    }

    for (let i = await this.maxHeight(); i >= 0; i--) {
      const line = [`${i}`];

      for (const value of map.values()) {
        if (value) {
          const str = value[i] ? `${value[i]}` : "";

          line.push(str.padEnd(12));
        }
      }

      console.log(line.join(" | "));
    }

    const divider = ["-"];
    const line = [" "];

    for (const key of map.keys()) {
      divider.push("------------");
      line.push(`${String(key)}`.padEnd(12));
    }

    console.log(divider.join("-+-"));
    console.log(line.join(" | "));

    console.groupEnd();
  }

  private async setup() {
    const lastEntry = this.kv.list({
      start: [0],
      end: [LAYER_LEVEL_LIMIT + 1],
    }, {
      limit: 1,
      reverse: true,
    });

    let level = 0;

    for await (const entry of lastEntry) {
      level = entry.key[this.layerKeyIndex] as number;
    }

    this._maxHeight = level;

    this.isSetup.resolve();
  }

  async maxHeight() {
    await this.isSetup;

    return this._maxHeight;
  }

  setMaxHeight(level: number) {
    this._maxHeight = level;
  }

  async insert(
    key: LogicalKey,
    value: LogicalValue,
    // Options are not part of the SummarisableStorage interface, we use them for testing only.
    opts?: {
      // Use this number as the layer up to which to insert, instead of choosing one at random.
      layer?: number;
    },
  ) {
    await this.isSetup;

    /*
    Three things to know about insertion:

    - we need to insert the correct nodes into the skiplist (in the form of physical entries)
    - we need to update all labels that require updating
      - doing this efficiently is the "fun" part of insertion
    - we want to (and do) perform all kv mutations as a single batch at the end of the operation
      - this means that we cannot query how we changed lower layers of the skiplist when computing changes to the upper layers
        - while this complicates the code, it also forces us to be more efficient, since querying the kv store is somewhat expensive
    */

    /*
    First, we check whether the logical key is already present. If it is, *and* the associated logical value is equal to the one we are inserting, we do not need to do anything else.
    Otherwise, we must update the physical kv store by inserting physical entries, and by updating certain labels.
    */

    const layerZeroPhysicalKey: PhysicalKey<LogicalKey> = [0, ...key];
    const got = await this.kv.get(layerZeroPhysicalKey);

    if (
      got &&
      this.logicalValueEq(
        got.logicalValue!, // On layer zero, every physical value stores a logical value.
        value,
      )
    ) {
      // No kv modification necessary!
      return;
    }

    /*
    Unfortunately, we need to do some actual work.

    Instead of modifying the kv store directly, we record all operations with this `batch` object. At the very end, we (atomically) perform all batched operations.
    */
    const batch = this.kv.batch();

    // Determine up to which skip list layer to insert physical nodes.
    const insertionHeight = opts?.layer !== undefined
      ? opts.layer
      : randomHeight();

    /*
    Now, the fun begins. Modification of the skiplist has two separate parts:

    1. We need to update layer zero.
    2. For all greater layers, we need to do some updates that depend on the results of updating the layer below.

    Updating layer zero is far more simple, and it provides the first bit of information-from-the-layer-below for the successive layers. So we start with layer zero.
    */

    /*
    In layer zero, we simply create (or overwrite) a node for the inserted entry.
    We need to compute the label (metadata) of that node, but we need not modify the metadata of any other nodes.

    On layer zero, we do not skip over anything. Hence, we summarise only ourselves.
    */
    const layerZeroSummary = await this.monoid.lift(key);
    const layerZeroPhysicalValue: PhysicalValue<LogicalValue, SummaryData> = {
      logicalValue: value, // On layer zero, we include the physical value. We won't on higher layers.
      maxLayer: insertionHeight,
      summaryCount: layerZeroSummary[1],
      summaryData: layerZeroSummary[0],
    };

    batch.set(layerZeroPhysicalKey, layerZeroPhysicalValue);

    /*
    That is all kv mutation for layer zero, now we need to prepare the information-from-the-layer-below for layer one. Which information is that? So glad you asked, let's define some things!

    Assume we are currently working at layer number `currentLayer`. Two tasks to do in that layer:

    1. If `layer <= insertionHeight`, insert/update a node. To do so, we need to  summarise the correct values - and we will try to be slightly clever about that.
    2. Update the label of the preceeding node on the same layer (if one exists). Again, there will be cleverness to achieve our time complexity goals.

    The cleverness mostly consists of remembering some nodes from the layer below to get around querying the kv store too often.

    Let `current` denote the node for the current key at layer `currentLayer` (if any).
    Let `currentLeft` denote the node preceeding `current` at layer `currentLayer`.
    Left `currentRight` denote the node succceeding `current` at layer `currentLayer`.
    Let `currentDown` denote the node with the same key as `current` but at layer `currentLayer - 1`.
    Let `currentLeftDown` denote the node with the same key as `currentLeft` but at layer `currentLayer - 1`.
    Let `currentRightDown` denote the node with the same key as `currentRight` but at layer `currentLayer - 1`.

    All of these can be undefined, because they might not exist: `current` doesn't exist if `currentLayer > insertionHeight`, `currentLeft` doesn't exist if `current` is a leftmost node, and `currentRight` doesn't exist if `current` is a rightmost node. The down-versions then stop existing one layer higher.

    We can now initialise these for `currentLayer := 1`.
    */

    // The `...Down` versions refer to layer zero initially.
    let currentDown: Node<LogicalKey, LogicalValue, SummaryData> | undefined = {
      key: layerZeroPhysicalKey,
      value: layerZeroPhysicalValue,
    };
    let currentLeftDown:
      | Node<LogicalKey, LogicalValue, SummaryData>
      | undefined = await this.getLeftNode(layerZeroPhysicalKey);
    let currentRightDown:
      | Node<LogicalKey, LogicalValue, SummaryData>
      | undefined = await this.getRightNode(layerZeroPhysicalKey);
    let current: Node<LogicalKey, LogicalValue, SummaryData> | undefined =
      undefined;
    let currentLeft: Node<LogicalKey, LogicalValue, SummaryData> | undefined =
      undefined;
    let currentRight: Node<LogicalKey, LogicalValue, SummaryData> | undefined =
      undefined;

    /*
    Now we iterate through all higher layers (conceptually at least — we do sneak in early returns where possible), copute current, currentLeft, and currentRight based off the -Down versions, record the corresponding kv updates in the batch object, and then set the -Down versions to the current versions before going into the next iteration.
    */
    for (
      let currentLayer = 1;
      currentLayer <= LAYER_LEVEL_LIMIT;
      currentLayer++
    ) {
      /*
      At this point in the code, we can assume that the `...Down` nodes to accurately reflect the layer `currentLayer - 1`.
      We assume `current`, `currentLeft`, and `currentRight` to contain outdated garbage - they merely exist to let us store information without overwriting the informatio from the previous layer.
      */

      /*
      All computations for this iteration done now.

      Record the kv updates.
      */
      if (current) {
        batch.set(current.key, current.value);
      }
      if (currentLeft) {
        batch.set(currentLeft.key, currentLeft.value);
      }
      if (currentRight) {
        batch.set(currentRight.key, currentRight.value);
      }

      // And prepare for the next iteration.
      currentDown = current;
      currentLeftDown = currentLeft;
      currentRightDown = currentRight;
    }

    /** The successor to this key from the previous layer, along with its maximum height. */
    let previousLayerSuccessor:
      | { key: LogicalKey; height: number }
      | undefined = undefined;
    /** The computed label from the previous layer. */
    let previousComputedLabel: [SummaryData, number] | undefined = undefined;

    const liftedValue = await this.monoid.lift(key, value);

    for (let layer = 0; layer <= insertionHeight; layer++) {
      // On the first layer we don't need to compute any labels, as there is no skipping.
      // But unlike higher layers, we DO need to store the payload hash.
      if (layer === 0) {
        const label = await this.monoid.lift(key, value);

        previousComputedLabel = label;

        batch.set([layer, key], [
          insertionHeight,
          label,
          value,
        ] as SkiplistBaseValue<SummaryData>);

        continue;
      }

      // On levels higher than 0, we may need to recompute labels.

      // If the previous successor's height is greater than current layer, great!
      // we can just reuse that value and continue upwards.
      if (
        previousLayerSuccessor && previousLayerSuccessor.height > layer &&
        previousComputedLabel
      ) {
        batch.set(
          [layer, key],
          [insertionHeight, previousComputedLabel] as SkiplistValue<
            SummaryData
          >,
        );
        continue;
      }

      // The next sibling is unknown, so we can't reuse a cached value.

      /** The successor to the inserted key on *this* layer. */
      const nextSuccessor = await this.getRightNode(layer, key, true);

      // But if there is a previous successor, we can use the previously computed label
      // and start iterating from there.

      /** The newly computed label for the inserted value. */
      let newLabel: [SummaryData, number] = previousComputedLabel ||
        liftedValue;

      // If there is a previous successor, we can start from there instead of from the insertion key
      // as we already have the computed label up to that point.
      const startFrom = previousLayerSuccessor
        ? previousLayerSuccessor.key
        : key;

      for await (
        const entry of this.kv.list<SkiplistValue<SummaryData>>(
          {
            start: [layer - 1, startFrom],
            end: nextSuccessor
              ? [layer - 1, nextSuccessor.key[this.valueKeyIndex]]
              : [layer],
          },
        )
      ) {
        newLabel = this.monoid.combine(newLabel, entry.value[1]);
      }

      batch.set([layer, key], [
        insertionHeight,
        newLabel,
      ] as SkiplistValue<SummaryData>);

      previousComputedLabel = newLabel;
      previousLayerSuccessor = nextSuccessor
        ? {
          key: nextSuccessor.key[this.valueKeyIndex] as LogicalKey,
          height: nextSuccessor.value[0],
        }
        : undefined;
    }

    // Now we recompute the preceding labels.

    // For preceding values
    let previousPredecessor: { key: LogicalKey; height: number } | undefined =
      undefined;
    let previousPredecessorComputedLabel: [SummaryData, number] | undefined =
      undefined;

    //  For overarching preceding values
    let previousOverarchingPredecessor:
      | { key: LogicalKey; height: number }
      | undefined = previousPredecessor;
    let previousOverarchingPredecessorComputedLabel:
      | [SummaryData, number]
      | undefined = previousPredecessorComputedLabel;
    let previousOverarchingSuccessorHeight: number | undefined;

    if (
      previousLayerSuccessor && previousLayerSuccessor.height <= insertionHeight
    ) {
      previousComputedLabel = undefined;
    }

    // Start from layer 1 as nothing needs to be recomputed on the base level.
    for (let layer = 1; layer <= this._maxHeight; layer++) {
      // We need to recompute labels which the newly inserted label may have truncated.
      // i.e. the labels for values who have height <= inserted height.
      if (layer <= insertionHeight) {
        if (
          previousPredecessor &&
          previousPredecessor.height >= layer &&
          previousPredecessorComputedLabel
        ) {
          // We can just reuse the previously computed label.
          batch.set(
            [layer, previousPredecessor.key],
            [
              previousPredecessor.height,
              previousPredecessorComputedLabel,
            ] as SkiplistValue<SummaryData>,
          );

          continue;
        }

        const predecessor = await this.getLeftNode(layer, key);

        if (!predecessor) {
          // If there's no predecessor here, there won't be any on the layers above.
          break;
        }

        let newLabel = this.monoid.neutral;

        for await (
          const entry of this.kv.list<SkiplistValue<SummaryData>>(
            {
              start: [layer - 1, predecessor.key[this.valueKeyIndex]],
              end: previousPredecessor
                ? [layer - 1, previousPredecessor.key]
                : [layer - 1, key],
            },
          )
        ) {
          newLabel = this.monoid.combine(newLabel, entry.value[1]);
        }

        if (previousPredecessorComputedLabel) {
          newLabel = this.monoid.combine(
            newLabel,
            previousPredecessorComputedLabel,
          );
        }

        batch.set(predecessor.key, [
          predecessor.value[0],
          newLabel,
        ] as SkiplistValue<SummaryData>);

        previousPredecessorComputedLabel = newLabel;

        previousPredecessor = {
          key: predecessor.key[this.valueKeyIndex] as LogicalKey,
          height: predecessor.value[0],
        };

        continue;
      }

      // We also need to recompute the values of overarching values.

      // If the previous overarching predecessor's height is gte the current layer
      // AND the successor is the same as last time
      // we can reuse the value from the previous iteration.

      if (
        previousOverarchingSuccessorHeight &&
        previousOverarchingSuccessorHeight >= layer &&
        previousOverarchingPredecessor &&
        previousOverarchingPredecessor.height >= layer &&
        previousOverarchingPredecessorComputedLabel
      ) {
        batch.set([layer, previousOverarchingPredecessor.key], [
          previousOverarchingPredecessor.height,
          previousOverarchingPredecessorComputedLabel,
        ] as SkiplistValue<SummaryData>);

        continue;
      }

      const overarchingPredecessor = await this.getLeftNode(layer, key);

      if (!overarchingPredecessor) {
        // If there's no overarching predecessor here, there never will be.
        break;
      }

      const overarchingSuccessor = await this.getRightNode(
        layer,
        overarchingPredecessor.key[this.valueKeyIndex] as LogicalKey,
      );

      let newLabel = this.monoid.neutral;

      for await (
        const entry of this.kv.list<SkiplistValue<SummaryData>>(
          {
            start: [layer - 1, overarchingPredecessor.key[this.valueKeyIndex]],
            end: previousOverarchingPredecessor
              ? [layer - 1, previousOverarchingPredecessor.key]
              : [layer - 1, key],
          },
        )
      ) {
        const overarchingKey = overarchingPredecessor
          .key[this.valueKeyIndex] as LogicalKey;

        if (this.logicalKeyCompare(key, overarchingKey) === 0) {
          continue;
        }

        newLabel = this.monoid.combine(newLabel, entry.value[1]);
      }

      if (previousOverarchingPredecessorComputedLabel) {
        newLabel = this.monoid.combine(
          newLabel,
          previousOverarchingPredecessorComputedLabel,
        );
      }

      if (previousComputedLabel) {
        newLabel = this.monoid.combine(newLabel, previousComputedLabel);
      }

      // The problem: the newly added label is not here!

      if (previousLayerSuccessor) {
        for await (
          const entry of this.kv.list<SkiplistValue<SummaryData>>(
            {
              start: [layer - 1, previousLayerSuccessor.key],
              end: overarchingSuccessor
                ? [
                  layer - 1,
                  overarchingSuccessor.key[this.valueKeyIndex] as LogicalKey,
                ]
                : [layer],
            },
          )
        ) {
          newLabel = this.monoid.combine(newLabel, entry.value[1]);
        }
      }

      batch.set(overarchingPredecessor.key, [
        overarchingPredecessor.value[0],
        newLabel,
      ] as SkiplistValue<SummaryData>);

      previousComputedLabel = undefined;
      previousLayerSuccessor = undefined;

      previousOverarchingPredecessor = {
        key: overarchingPredecessor.key[this.valueKeyIndex] as LogicalKey,
        height: overarchingPredecessor.value[0],
      };
      previousOverarchingPredecessorComputedLabel = newLabel;
      previousOverarchingSuccessorHeight = overarchingSuccessor
        ? overarchingSuccessor.value[0]
        : 65;
    }

    if (insertionHeight > await this.maxHeight()) {
      this.setMaxHeight(insertionHeight);
    }

    await batch.commit();
  }

  /**
   * Get the node to the right of the given key in the skiplist. Returns undefined if the given key is the rightmost item of its layer.
   */
  private async getRightNode(
    key: PhysicalKey<LogicalKey>,
  ): Promise<Node<LogicalKey, LogicalValue, SummaryData> | undefined> {
    const nextItems = this.kv.list({
      start: key,
      end: [key[0] + 1], // First node of the next higher layer.
    }, {
      limit: 1,
      batchSize: 1,
    });

    for await (const next of nextItems) {
      return next;
    }

    return undefined;
  }

  /**
   * Get the node to the left of the given key in the skiplist. Returns undefined if the given key is the leftmost item of its layer.
   */
  private async getLeftNode(
    key: PhysicalKey<LogicalKey>,
  ): Promise<Node<LogicalKey, LogicalValue, SummaryData> | undefined> {
    const nextItems = this.kv.list({
      start: [key[0]], // First node on the same layer as `key`.
      end: key,
    }, {
      reverse: true,
      limit: 1,
      batchSize: 1,
    });

    for await (const next of nextItems) {
      return next;
    }

    return undefined;
  }

  // async remove(key: LogicalKey) {
  //   const toRemoveBase = await this.kv.get<SkiplistBaseValue<SummaryData>>([
  //     0,
  //     key,
  //   ]);

  //   if (!toRemoveBase) {
  //     return false;
  //   }

  //   const batch = this.kv.batch();

  //   const height = toRemoveBase[0];

  //   let previousLabel: [LogicalKey, [SummaryData, number]] | null = null;
  //   let previousSuccessor: LogicalKey | undefined;

  //   for (let layer = 0; layer <= height; layer++) {
  //     batch.delete([layer, key]);
  //   }

  //   for (let layer = 1; layer <= this._maxHeight; layer++) {
  //     const predecessor = await this.getLeftNode(layer, key);

  //     if (!predecessor) {
  //       break;
  //     }

  //     const successor = await this.getRightNode(
  //       layer,
  //       layer <= height
  //         ? key
  //         : predecessor.key[this.valueKeyIndex] as LogicalKey,
  //     );

  //     if (layer - 1 === 0) {
  //       const fromPredecessorToSuccessor = this.kv.list<
  //         SkiplistBaseValue<SummaryData>
  //       >({
  //         start: [0, predecessor.key[this.valueKeyIndex]],
  //         end: successor ? [0, successor.key[this.valueKeyIndex]] : [1],
  //       });

  //       let newLabel = this.monoid.neutral;

  //       for await (const entry of fromPredecessorToSuccessor) {
  //         const entryKey = entry.key[this.valueKeyIndex] as LogicalKey;
  //         if (this.compare(entryKey, key) === 0) {
  //           continue;
  //         }

  //         newLabel = this.monoid.combine(newLabel, entry.value[1]);
  //       }

  //       const predecessorVal = await this.get(
  //         predecessor.key[this.valueKeyIndex] as LogicalKey,
  //       );

  //       batch.set(
  //         predecessor.key,
  //         [
  //           predecessor.value[0],
  //           newLabel,
  //           predecessorVal,
  //         ] as SkiplistBaseValue<SummaryData>,
  //       );

  //       previousLabel = [
  //         predecessor.key[this.valueKeyIndex] as LogicalKey,
  //         newLabel,
  //       ];

  //       previousSuccessor = successor
  //         ? successor.key[this.valueKeyIndex] as LogicalKey
  //         : undefined;

  //       continue;
  //     }

  //     const predecessorIsSame = (!predecessor && !previousLabel) ||
  //       predecessor && previousLabel &&
  //         this.compare(
  //             previousLabel[0],
  //             predecessor.key[this.valueKeyIndex] as LogicalKey,
  //           ) === 0;

  //     const successorIsSame = (!successor && !previousSuccessor) ||
  //       successor && previousSuccessor &&
  //         this.compare(
  //             previousSuccessor,
  //             successor.key[this.valueKeyIndex] as LogicalKey,
  //           ) === 0;

  //     if (predecessorIsSame && successorIsSame && previousLabel) {
  //       batch.set(
  //         predecessor.key,
  //         [predecessor.value[0], previousLabel[1]] as SkiplistValue<
  //           SummaryData
  //         >,
  //       );

  //       continue;
  //     }

  //     const fromPredecessorToSuccessor = this.kv.list<
  //       SkiplistValue<SummaryData>
  //     >({
  //       start: [layer - 1, predecessor.key[this.valueKeyIndex]],
  //       end: successor
  //         ? [layer - 1, successor.key[this.valueKeyIndex]]
  //         : [layer],
  //     });

  //     let newLabel = this.monoid.neutral;

  //     for await (const entry of fromPredecessorToSuccessor) {
  //       const entryValue = entry.key[this.valueKeyIndex] as LogicalKey;

  //       if (this.compare) {
  //         if (
  //           previousLabel && this.compare(
  //               entryValue,
  //               previousLabel[0],
  //             ) === 0
  //         ) {
  //           newLabel = this.monoid.combine(newLabel, previousLabel[1]);
  //         } else if (this.compare(entryValue, key) === 0) {
  //           continue;
  //         } else {
  //           newLabel = this.monoid.combine(newLabel, entry.value[1]);
  //         }
  //       }
  //     }

  //     batch.set(
  //       predecessor.key,
  //       [predecessor.value[0], newLabel] as SkiplistValue<SummaryData>,
  //     );

  //     previousLabel = [
  //       predecessor.key[this.valueKeyIndex] as LogicalKey,
  //       newLabel,
  //     ];

  //     previousSuccessor = successor
  //       ? successor.key[this.valueKeyIndex] as LogicalKey
  //       : undefined;

  //     const remainingOnThisLayer = this.kv.list<LogicalKey>({
  //       start: [layer],
  //       end: [layer + 1],
  //     }, {
  //       limit: 1,
  //     });

  //     let canForgetLayer = true;

  //     for await (const _result of remainingOnThisLayer) {
  //       canForgetLayer = false;
  //     }

  //     if (canForgetLayer && layer > await this.maxHeight()) {
  //       this.setMaxHeight(layer - 1);
  //     }
  //   }

  //   await batch.commit();

  //   return true;
  // }

  // async get(key: LogicalKey) {
  //   const result = await this.kv.get<SkiplistBaseValue<SummaryData>>([
  //     0,
  //     key,
  //   ]);

  //   if (result) {
  //     return result[2];
  //   }

  //   return undefined;
  // }

  // async summarise(
  //   start: LogicalKey,
  //   end: LogicalKey,
  // ): Promise<{ fingerprint: SummaryData; size: number }> {
  //   // Get the first value.

  //   // For each value you find, shoot up.

  //   // Keep moving rightwards until you overshoot.

  //   // Assemble the tail using start: key where things started to go wrong, end: end.

  //   const accumulateUntilOvershoot = async (
  //     start: LogicalKey | undefined,
  //     end: LogicalKey | undefined,
  //     layer: number,
  //   ): Promise<
  //     { label: [SummaryData, number]; overshootKey: LogicalKey | undefined }
  //   > => {
  //     const startUntilEnd = this.kv.list<SkiplistValue<SummaryData>>({
  //       start: start ? [layer, start] : [layer],
  //       end: end ? [layer, end] : [layer + 1],
  //     });

  //     let label = this.monoid.neutral;

  //     let candidateLabel = this.monoid.neutral;
  //     let overshootKey: LogicalKey | undefined;

  //     for await (const entry of startUntilEnd) {
  //       label = this.monoid.combine(label, candidateLabel);
  //       candidateLabel = this.monoid.neutral;

  //       const [entryHeight, entryLabel] = entry.value;

  //       if (entryHeight <= layer) {
  //         overshootKey = entry.key[this.valueKeyIndex] as LogicalKey;
  //         candidateLabel = entryLabel;
  //         continue;
  //       }

  //       const { label: aboveLabel, overshootKey: aboveOvershootKey } =
  //         await accumulateUntilOvershoot(
  //           start,
  //           end,
  //           entryHeight,
  //         );

  //       overshootKey = aboveOvershootKey;

  //       label = this.monoid.combine(label, aboveLabel);

  //       break;
  //     }

  //     // Now we must determine what to do with the leftover candidate.
  //     const [, candidateSize] = candidateLabel;

  //     if (candidateSize === 1 || end === undefined) {
  //       label = this.monoid.combine(label, candidateLabel);
  //       return { label, overshootKey: undefined };
  //     }

  //     return { label, overshootKey };
  //   };

  //   const getTail = async (
  //     overshootLabel: LogicalKey,
  //     end: LogicalKey | undefined,
  //     layer: number,
  //   ): Promise<[SummaryData, number]> => {
  //     const endUntilOvershoot = this.kv.list<SkiplistValue<SummaryData>>({
  //       start: [layer, overshootLabel],
  //       end: end ? [layer, end] : [layer + 1],
  //     }, {
  //       reverse: true,
  //     });

  //     let label = this.monoid.neutral;

  //     for await (const entry of endUntilOvershoot) {
  //       const [entryHeight, entryLabel] = entry.value;

  //       label = this.monoid.combine(entryLabel, label);

  //       if (entryHeight <= layer) {
  //         continue;
  //       }

  //       const entryValue = entry.key[this.valueKeyIndex] as LogicalKey;

  //       const aboveLabel = await getTail(
  //         overshootLabel,
  //         entryValue,
  //         entryHeight,
  //       );

  //       label = this.monoid.combine(aboveLabel, label);

  //       break;
  //     }

  //     return label;
  //   };

  //   const accumulateLabel = async (
  //     { start, end }: {
  //       start?: LogicalKey;
  //       end?: LogicalKey;
  //     },
  //   ) => {
  //     const firstEntry = this.kv.list<SkiplistBaseValue<SummaryData>>({
  //       start: start ? [0, start] : [0],
  //       end: [1],
  //     }, {
  //       limit: 1,
  //     });

  //     for await (const first of firstEntry) {
  //       const height = first.value[0];

  //       const { label: headLabel, overshootKey } =
  //         await accumulateUntilOvershoot(
  //           start,
  //           end,
  //           height,
  //         );

  //       if (!overshootKey) {
  //         // Very lucky.

  //         return headLabel;
  //       }

  //       // Get the tail, return it.
  //       const tailLabel = await getTail(overshootKey, end, 0);

  //       return this.monoid.combine(headLabel, tailLabel);
  //     }

  //     return this.monoid.neutral;
  //   };

  //   const argOrder = this.compare(start, end);

  //   if (argOrder < 0) {
  //     const [fingerprint, size] = await accumulateLabel({ start, end });

  //     return { fingerprint, size };
  //   } else if (argOrder > 0) {
  //     const firstHalf = await accumulateLabel({ end });

  //     const secondHalf = await accumulateLabel({ start });

  //     const [fingerprint, size] = this.monoid.combine(firstHalf, secondHalf);

  //     return { fingerprint, size };
  //   } else {
  //     const [fingerprint, size] = await accumulateLabel({});

  //     return { fingerprint, size };
  //   }
  // }

  // async *entries(
  //   start: LogicalKey | undefined,
  //   end: LogicalKey | undefined,
  //   opts?: {
  //     limit?: number;
  //     reverse?: boolean;
  //   },
  // ): AsyncIterable<{ key: LogicalKey; value: Uint8Array }> {
  //   let yielded = 0;
  //   const hitLimit = () => {
  //     if (opts?.limit) {
  //       yielded++;

  //       if (yielded >= opts.limit) {
  //         return true;
  //       }
  //     }

  //     return false;
  //   };

  //   if (!start && !end) {
  //     for await (const result of this.allEntries(opts?.reverse)) {
  //       yield result;
  //       if (hitLimit()) break;
  //     }

  //     return;
  //   }

  //   const argOrder = start && end ? this.compare(start, end) : -1;

  //   if (argOrder === 0) {
  //     for await (const result of this.allEntries(opts?.reverse)) {
  //       yield result;

  //       if (hitLimit()) break;
  //     }
  //   } else if (argOrder < 0) {
  //     const results = this.kv.list<SkiplistBaseValue<SummaryData>>({
  //       start: start ? [0, start] : [0],
  //       end: end ? [0, end] : [1],
  //     }, {
  //       limit: opts?.limit,
  //       reverse: opts?.reverse,
  //     });

  //     for await (const entry of results) {
  //       yield {
  //         key: entry.key[this.valueKeyIndex] as LogicalKey,
  //         value: entry.value[2],
  //       };
  //     }
  //   } else if (opts?.reverse) {
  //     const secondHalf = this.kv.list<SkiplistBaseValue<SummaryData>>({
  //       start: start ? [0, start] : [0],
  //       end: [1],
  //     }, { reverse: true });

  //     for await (const entry of secondHalf) {
  //       yield {
  //         key: entry.key[this.valueKeyIndex] as LogicalKey,
  //         value: entry.value[2],
  //       };

  //       if (hitLimit()) break;
  //     }

  //     const firstHalf = this.kv.list<SkiplistBaseValue<SummaryData>>({
  //       start: [0],
  //       end: end ? [0, end] : [1],
  //     }, { reverse: true });

  //     for await (const entry of firstHalf) {
  //       yield {
  //         key: entry.key[this.valueKeyIndex] as LogicalKey,
  //         value: entry.value[2],
  //       };

  //       if (hitLimit()) break;
  //     }
  //   } else {
  //     const firstHalf = this.kv.list<SkiplistBaseValue<SummaryData>>({
  //       start: [0],
  //       end: end ? [0, end] : [1],
  //     });

  //     for await (const entry of firstHalf) {
  //       yield {
  //         key: entry.key[this.valueKeyIndex] as LogicalKey,
  //         value: entry.value[2],
  //       };

  //       if (hitLimit()) break;
  //     }

  //     const secondHalf = this.kv.list<SkiplistBaseValue<SummaryData>>({
  //       start: start ? [0, start] : [0],
  //       end: [1],
  //     });

  //     for await (const entry of secondHalf) {
  //       yield {
  //         key: entry.key[this.valueKeyIndex] as LogicalKey,
  //         value: entry.value[2],
  //       };

  //       if (hitLimit()) break;
  //     }
  //   }
  // }

  // async *allEntries(
  //   reverse?: boolean,
  // ): AsyncIterable<{ key: LogicalKey; value: Uint8Array }> {
  //   const iter = this.kv.list<SkiplistBaseValue<SummaryData>>({
  //     start: [0],
  //     end: [1],
  //   }, {
  //     reverse,
  //   });

  //   for await (const entry of iter) {
  //     yield {
  //       key: entry.key[this.valueKeyIndex] as LogicalKey,
  //       value: entry.value[2],
  //     };
  //   }
  // }
}

function randomHeight() {
  let level = 0;

  while (
    Math.random() <= LAYER_INSERT_PROBABILITY && level < LAYER_LEVEL_LIMIT
  ) {
    level += 1;
  }

  return level;
}
