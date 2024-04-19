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

export type PhysicalKey<LogicalKey extends KeyPart[]> = [
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

function physicalKeyIncrementLayer<LogicalKey extends KeyPart[]>(
  pk: PhysicalKey<LogicalKey>,
): PhysicalKey<LogicalKey> {
  return [physicalKeyGetLayer(pk) + 1, ...physicalKeyGetLogicalKey(pk)];
}

function physicalKeyDecrementLayer<LogicalKey extends KeyPart[]>(
  pk: PhysicalKey<LogicalKey>,
): PhysicalKey<LogicalKey> {
  return [physicalKeyGetLayer(pk) - 1, ...physicalKeyGetLogicalKey(pk)];
}

/*
Further, the skiplist is summarisable: you can efficiently query for a *summary* of arbitrary ranges, consisting of the number of items in that range, as well as some accumulated monoidal value (see the range-based set reconciliation paper for more details).
To support this efficiently, each physical value consists not only of a logical value, but also of some metadata.
*/

export type PhysicalValue<LogicalValue, SummaryData> = {
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
   * The summary data of the logical entries on layer zero of the skip list whose key is greater than or equal to ours but strictly less than the key of the next entry on the same layer.
   */
  summary: [
    SummaryData,
    number, /* number of logical entries on layer zero until the next entry */
  ];
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

function nodeIncrementLayer<
  LogicalKey extends KeyPart[],
  LogicalValue,
  SummaryData,
>(
  node: Node<LogicalKey, LogicalValue, SummaryData>,
): Node<LogicalKey, LogicalValue, SummaryData> {
  return {
    key: physicalKeyIncrementLayer(node.key),
    value: { ...node.value },
  };
}

// Take this to the power of l to get the probability that the skiplist has a node at layer l.
const LAYER_INSERT_PROBABILITY = 0.5;
// Hard limit on the number of layers, never insert above this.
const LAYER_LEVEL_LIMIT = 64;

export type SkiplistOpts<
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
  monoid: LiftingMonoid<[LogicalKey, LogicalValue], SummaryData>;
};

export class Skiplist<
  LogicalKey extends KeyPart[],
  LogicalValue,
  SummaryData,
> implements SummarisableStorage<LogicalKey, LogicalValue, SummaryData> {
  private logicalKeyCompare: (a: LogicalKey, b: LogicalKey) => number;
  private logicalValueEq: (a: LogicalValue, b: LogicalValue) => boolean;
  private kv: KvDriver<
    PhysicalKey<LogicalKey>,
    PhysicalValue<LogicalValue, SummaryData>
  >;
  private _maxHeight = 0;
  private isSetup = deferred();
  private monoid: LiftingMonoid<
    [LogicalKey, LogicalValue],
    [SummaryData, number]
  >;

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
      string,
      Record<number, PhysicalValue<LogicalValue, SummaryData>>
    > = new Map();

    for await (const entry of this.kv.list({})) {
      const layer = physicalKeyGetLayer(entry.key);
      const key = physicalKeyGetLogicalKey(entry.key);
      const keyJson = JSON.stringify(key);

      const valueEntry = map.get(keyJson);

      if (valueEntry) {
        map.set(keyJson, {
          ...valueEntry,
          [layer]: entry.value,
        });

        continue;
      }

      map.set(keyJson, {
        [layer]: entry.value,
      });
    }

    // console.log(map);

    for (let i = await this.maxHeight(); i >= 0; i--) {
      const line = [`${i}`];

      for (const value of map.values()) {
        if (value) {
          const str = value[i] ? `${value[i].summary}` : "";

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
      level = physicalKeyGetLayer(entry.key);
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
  ): Promise<void> {
    await this.isSetup;

    // console.log("\n====\nentering insert for key", key, " and value ", value);

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

    const insertingCompletelyNew = !got;
    const oldInsertionHeight = got ? got.maxLayer : undefined;

    /*
    Unfortunately, we need to do some actual work.

    Instead of modifying the kv store directly, we record all operations with this `batch` object. At the very end, we (atomically) perform all batched operations.
    */
    const batch = this.kv.batch();

    // Determine up to which skip list layer to insert physical nodes.
    const insertionHeight = oldInsertionHeight === undefined
      ? (opts?.layer !== undefined ? opts.layer : randomHeight())
      : oldInsertionHeight;

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
    const layerZeroPhysicalValue: PhysicalValue<LogicalValue, SummaryData> = {
      logicalValue: value, // On layer zero, we include the physical value. We won't on higher layers.
      maxLayer: insertionHeight,
      summary: await this.monoid.lift([key, value]),
    };

    batch.set(layerZeroPhysicalKey, layerZeroPhysicalValue);
    /*
    That is all kv mutation for layer zero, now we need to prepare the information-from-the-layer-below for layer one.
    Which information is that? So glad you asked, let's define some things!

    Assume we are currently working at layer number `currentLayer`. Two tasks to do in that layer:

    1. If `layer <= insertionHeight`, insert/update a node. To do so, we need to  summarise the correct values - and we will try to be slightly clever about that.
    2. Update the label of the preceeding node on the same layer (if one exists). Again, there will be cleverness to achieve our time complexity goals.

    The cleverness mostly consists of remembering some nodes from the layer below to get around querying the kv store too often.

    Let `current` denote the node for the current key at layer `currentLayer` (if any).
    Let `currentLeft` denote the node preceeding `current` at layer `currentLayer`.
    Left `currentRight` denote the node succceeding `current` at layer `currentLayer`.
    Let `prior` denote the node with the same key as `current` but at layer `currentLayer - 1`.
    Let `priorLeft` denote the node preceeding `prior` (at layer `currentLayer - 1`).
    Let `priorRight` denote the node succeeding `prior` (at layer `currentLayer - 1`).

    To simplify the code, we always maintain `current` and `prior`, even above the `insertionHeight`. Those computations are not wasted, these values are used when computing labels for `currentLeft` at layers above `insertionHeight`. Accordingly, the summaries we store in `currentLeft` and `priorLeft` summarise only up until (and excluding) `current` and `prior` respectively. In layers above `insertionHeight`, we write full summary information to the underlying kv store, but the information in these variables stays truncated.

    Left and right nodes can be undefined, because they might not exist: `currentLeft` doesn't exist if `current` is a leftmost node, and `currentRight` doesn't exist if `current` is a rightmost node. The prior-versions then stop existing one layer higher.

    We can now initialise these for `currentLayer := 1`.
    */

    // The `priorFoo` versions refer to layer zero initially.
    let prior: Node<LogicalKey, LogicalValue, SummaryData> = {
      key: layerZeroPhysicalKey,
      value: layerZeroPhysicalValue,
    };
    let priorLeft:
      | Node<LogicalKey, LogicalValue, SummaryData>
      | undefined = await this.getLeftNode(layerZeroPhysicalKey);
    let priorRight:
      | Node<LogicalKey, LogicalValue, SummaryData>
      | undefined = await this.getRightNode(layerZeroPhysicalKey);
    let current: Node<LogicalKey, LogicalValue, SummaryData> = prior; // This initialisation is not meaningful, we just needed a non-undefined value.
    let currentLeft: Node<LogicalKey, LogicalValue, SummaryData> | undefined =
      undefined;
    let currentRight: Node<LogicalKey, LogicalValue, SummaryData> | undefined =
      undefined;

    /*
    Now we iterate through all higher layers (conceptually at least — we do sneak in some early returns), compute current, currentLeft, and currentRight based off the priorFoo versions, record the corresponding kv updates in the batch object, and then set the priorFoo versions to the current versions before going into the next iteration.
    */
    for (
      let currentLayer = 1;
      currentLayer <= LAYER_LEVEL_LIMIT;
      currentLayer++
    ) {
      /*
      At this point in the code, we can assume that the `priorFoo` nodes to accurately reflect the layer `currentLayer - 1`.
      We assume `current`, `currentLeft`, and `currentRight` to contain outdated garbage - they merely exist to let us store information without overwriting the information from the previous layer.

      Now, we need to update `current`, `currentLeft`, and `currentRight` into non-garbage. `currentRight` only exists to make computations more efficient, whereas `current`, `currentLeft` receive updates that need to be reflected in the kv store at the end of this function.
      */

      const currentKey: PhysicalKey<LogicalKey> = [currentLayer, ...key];

      // console.log("insert loop iteration with currentKey:", currentKey);      

      /*
      If the maxLayer of priorRight is greater than or equal to the currentLayer, then currentRight is simply the node above priorRight.
      If priorRight is undefined, then currentRight will also be undefined.
      */
      if (!priorRight) {
        currentRight = undefined;
      } else if (priorRight.value.maxLayer > currentLayer) {
        currentRight = nodeIncrementLayer(priorRight);
      } else {
        // Query the kv store for the right node.
        currentRight = await this.getRightNode(currentKey);
      }

      // We now have all information to compute (the label of) `current`.
      //
      // The label of current summarises until currentRight, which is the same as summarising the label of `prior` and everything from priorRight to currentRight.
      // If priorRight is already undefined, then the new label is simply the old label.
      let newCurrentSummary = prior!.value.summary;
      if (priorRight) {
        newCurrentSummary = this.monoid.combine(
          newCurrentSummary,
          await this.summariseSingleLayer(
            priorRight.key,
            currentRight
              ? physicalKeyGetLogicalKey(currentRight.key)
              : undefined,
          ),
        );
      }

      // Update `current` with its proper label, and record the mutation to be passed on to the kv store eventually.
      current = nodeIncrementLayer(prior!);
      current.value.summary = newCurrentSummary;

      if (currentLayer <= insertionHeight) {
        batch.set(currentKey, current.value);
      }

      /*
      It remains to tackle currentLeft; figuring out its label is slightly more tricky than for `current`.
      But first, we simply obtain the node, analogously to that of currentRight.

      If the maxLayer of priorLeft is greater than or equal to the currentLayer, then currentLeft is simply the node above priorLeft.
      If priorLeft is undefined, then currentLeft will also be undefined.
      */
      if (!priorLeft) {
        currentLeft = undefined;
      } else if (priorLeft.value.maxLayer > currentLayer) {
        currentLeft = nodeIncrementLayer(priorLeft);
      } else {
        // Query the kv store for the left node.
        currentLeft = await this.getLeftNode(currentKey);
      }

      // console.log("currentLeft", currentLeft);  
      // console.log("priorLeft", priorLeft);      

      /*
      Now for computing (the label of) `currentLeft`.
      */

      if (!currentLeft) {
        // Nothing to do here. Yay =)
      } else {
        if (!insertingCompletelyNew && (currentLayer <= insertionHeight)) {
          // We know the left labels to stay unchanged in these cases. Hence, nothing to do ^_^
        } else {
          // The label of `currentLeft` summarises everything from currentLeft to priorLeft, combined with the label of priorLeft (which summarises up until `current`).
          const newCurrentLeftSummary = this.monoid.combine(
            await this.summariseSingleLayer(
              physicalKeyDecrementLayer(currentLeft.key),
              physicalKeyGetLogicalKey(priorLeft!.key),
            ),
            priorLeft!.value.summary,
          );

          // Update `currentLeft` with its proper label, and record the mutation to be passed on to the kv store eventually.
          currentLeft.value.summary = newCurrentLeftSummary;
          batch.set(currentLeft.key, currentLeft.value);

          if (currentLayer <= insertionHeight) {
            batch.set(currentKey, current.value);
          } else {
            // On the kv store, we need to summarise not only until `current`, but until `currentRight`.
            const actualLeftValue = { ...currentLeft.value };
            actualLeftValue.summary = this.monoid.combine(
              currentLeft.value.summary,
              current.value.summary,
            );
            batch.set(currentLeft.key, actualLeftValue);
          }
        }
      }

      /*
      All computations for this iteration done. Yay!

      Can we break early?
      */
      if (
        currentLeft === undefined && currentLayer > insertionHeight
      ) {
        break;
      }

      // Prepare for the next iteration.
      prior = current;
      priorLeft = currentLeft;
      priorRight = currentRight;
    }

    // Bookkeeping.
    if (insertionHeight > await this.maxHeight()) {
      this.setMaxHeight(insertionHeight);
    }

    // Write mutations to the kv store.
    await batch.commit();
  }

  async remove(key: LogicalKey): Promise<boolean> {
    await this.isSetup;

    // console.log("\n====\nentering delete for key", key);

    /*
    Deletion employs similar techniques as insertion; you should read the comments in the `insert` functionif you want to make sense of this function too.
    */

    // Do we acually contain this key?
    const layerZeroPhysicalKey: PhysicalKey<LogicalKey> = [0, ...key];
    const got = await this.kv.get(layerZeroPhysicalKey);

    if (!got) {
      // Nothing to do but to report back if we didn't have the key in the first place.
      return false;
    }

    // While we still need to update labels above this layer, we can stop deleting nodes after this layer.
    const itemMaxLayer = got.maxLayer;

    // Queue deletion of the layer zero key (we handle all other layers later in a loop).
    const batch = this.kv.batch();
    batch.delete(layerZeroPhysicalKey);

    /*
    Deletion is more simple than insertion, because we only need to update a single label per layer.
    For efficiency, we cache priorLeft and priorRight similat to insertion. We don't need to cache `prior`.
    */

    let priorLeft = await this.getLeftNode(layerZeroPhysicalKey);
    let priorRight = await this.getRightNode(layerZeroPhysicalKey);
    let currentLeft = priorLeft; // Initialisation is not meaningful, will be overwritten before being read.
    let currentRight = priorRight; // Initialisation is not meaningful, will be overwritten before being read.

    // There is no need to modify the left label on layer zero.

    for (
      let currentLayer = 1;
      currentLayer <= LAYER_LEVEL_LIMIT;
      currentLayer++
    ) {
      // The key to delete (at least until layer `itemMaxLayer`).
      const currentKey: PhysicalKey<LogicalKey> = [currentLayer, ...key];
      if (currentLayer <= itemMaxLayer) {
        batch.delete(currentKey);
      }

      // console.log("delete loop iteration with currentKey:", currentKey);

      // It remains to update the label of currentLeft.

      // First, update currentLeft and currentRight.
      if (!priorLeft) {
        currentLeft = undefined;
      } else if (priorLeft.value.maxLayer > currentLayer) {
        currentLeft = nodeIncrementLayer(priorLeft);
      } else {
        // Query the kv store for the left node.
        currentLeft = await this.getLeftNode(currentKey);
      }

      if (!priorRight) {
        currentRight = undefined;
      } else if (priorRight.value.maxLayer > currentLayer) {
        currentRight = nodeIncrementLayer(priorRight);
      } else {
        // Query the kv store for the right node.
        currentRight = await this.getRightNode(currentKey);
      }

      // console.log("currentLeft", currentLeft);
      // console.log("priorLeft", priorLeft); 
      // console.log("currentRight", currentRight);
      // console.log("priorRight", priorRight); 

      // The new label of currentLeft summarises until currentRight.
      // This is the same as summarising until priorLeft, from priorLeft to priorRight (this summary we have cached in the `priorLeft` variable), and from priorRight to currentRight (all on the previous layer).
      let newCurrentLeftSummary = priorLeft
        ? priorLeft.value.summary
        : this.monoid.neutral;

      // Nothing more to do if currentLeft is undefined.
      if (currentLeft) {
        // Otherwise, add summary from currentLeft to priorLeft to the summary.
        newCurrentLeftSummary = this.monoid.combine(
          await this.summariseSingleLayer(
            physicalKeyDecrementLayer(currentLeft.key),
            physicalKeyGetLogicalKey(priorLeft!.key), // currentLeft != undefined implies priorLeft != undefined.
          ),
          newCurrentLeftSummary,
        );

        if (priorRight) {
          // Add summary from priorRight to currentRight to the summary.
          newCurrentLeftSummary = this.monoid.combine(
            newCurrentLeftSummary,
            await this.summariseSingleLayer(
              priorRight.key,
              currentRight
                ? physicalKeyGetLogicalKey(currentRight.key)
                : undefined,
            ),
          );
        }

        // Update the currentLeft summary.
        currentLeft.value.summary = newCurrentLeftSummary;
        batch.set(currentLeft.key, currentLeft.value);
      }

      // Preparing for next iteration.
      if (currentLayer > itemMaxLayer && currentLeft === undefined) {
        // No more kv store modifications necessary.
        // Write mutations to the kv store.
        await batch.commit();
        return true;
      } else {
        priorLeft = currentLeft;
        priorRight = currentRight;
      }
    }

    // Write mutations to the kv store.
    await batch.commit();
    return true;
  }

  async get(key: LogicalKey): Promise<LogicalValue | undefined> {
    const result = await this.kv.get([0, ...key]);

    if (result) {
      return result.logicalValue;
    } else {
      return undefined;
    }
  }

  async summarise(
    start?: LogicalKey,
    end?: LogicalKey,
  ): Promise<{ fingerprint: SummaryData; size: number }> {
    // Get the first value.

    // For each value you find, shoot up.

    // Keep moving rightwards until you overshoot.

    // Assemble the tail using start: key where things started to go wrong, end: end.

    const accumulateUntilOvershoot = async (
      start: LogicalKey | undefined,
      end: LogicalKey | undefined,
      layer: number,
    ): Promise<
      { label: [SummaryData, number]; overshootKey: LogicalKey | undefined }
    > => {
      const startUntilEnd = this.kv.list({
        start: start ? [layer, ...start] : [layer],
        end: end ? [layer, ...end] : [layer + 1],
      });

      let label = this.monoid.neutral;

      let candidateLabel = this.monoid.neutral;
      let overshootKey: LogicalKey | undefined;

      for await (const entry of startUntilEnd) {
        label = this.monoid.combine(label, candidateLabel);
        candidateLabel = this.monoid.neutral;

        if (entry.value.maxLayer <= layer) {
          overshootKey = physicalKeyGetLogicalKey(entry.key);
          candidateLabel = entry.value.summary;
          continue;
        }

        const { label: aboveLabel, overshootKey: aboveOvershootKey } =
          await accumulateUntilOvershoot(
            start,
            end,
            entry.value.maxLayer,
          );

        overshootKey = aboveOvershootKey;

        label = this.monoid.combine(label, aboveLabel);

        break;
      }

      // Now we must determine what to do with the leftover candidate.
      const [, candidateSize] = candidateLabel;

      if (candidateSize === 1 || end === undefined) {
        label = this.monoid.combine(label, candidateLabel);
        return { label, overshootKey: undefined };
      }

      return { label, overshootKey };
    };

    const getTail = async (
      overshootLabel: LogicalKey,
      end: LogicalKey | undefined,
      layer: number,
    ): Promise<[SummaryData, number]> => {
      const endUntilOvershoot = this.kv.list({
        start: [layer, ...overshootLabel],
        end: end ? [layer, ...end] : [layer + 1],
      }, {
        reverse: true,
      });

      let label = this.monoid.neutral;

      for await (const entry of endUntilOvershoot) {
        label = this.monoid.combine(entry.value.summary, label);

        if (entry.value.maxLayer <= layer) {
          continue;
        }

        const entryValue = physicalKeyGetLogicalKey(entry.key);

        const aboveLabel = await getTail(
          overshootLabel,
          entryValue,
          entry.value.maxLayer,
        );

        label = this.monoid.combine(aboveLabel, label);

        break;
      }

      return label;
    };

    const accumulateLabel = async (
      { start, end }: {
        start?: LogicalKey;
        end?: LogicalKey;
      },
    ) => {
      const firstEntry = this.kv.list({
        start: start ? [0, ...start] : [0],
        end: [1],
      }, {
        limit: 1,
      });

      for await (const first of firstEntry) {
        const { label: headLabel, overshootKey } =
          await accumulateUntilOvershoot(
            start,
            end,
            first.value.maxLayer,
          );

        if (!overshootKey) {
          // Very lucky.

          return headLabel;
        }

        // Get the tail, return it.
        const tailLabel = await getTail(overshootKey, end, 0);

        return this.monoid.combine(headLabel, tailLabel);
      }

      return this.monoid.neutral;
    };

    const [fingerprint, size] = await accumulateLabel({ start, end });

    return { fingerprint, size };
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
      limit: 2,
      batchSize: 2,
    });

    for await (const next of nextItems) {
      if (
        this.logicalKeyCompare(
          physicalKeyGetLogicalKey(next.key),
          physicalKeyGetLogicalKey(key),
        ) === 0
      ) {
        continue;
      } else {
        return next;
      }
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

  /**
   * Summarise nodes on a single layer until reaching a node whose logical key is >= `end` (the `end` node is *excluded*). If `end` is undefined, summarises untl the end of the layer.
   *
   * Returns the empty summary if the sequence of nodes in question is empty.
   */
  private async summariseSingleLayer(
    start: PhysicalKey<LogicalKey>,
    end?: LogicalKey,
  ): Promise<[SummaryData, number]> {
    const layer = physicalKeyGetLayer(start);
    let summary = this.monoid.neutral;

    for await (
      const { value } of this.kv.list({
        start,
        end: end ? [layer, ...end] : [layer + 1],
      })
    ) {
      summary = this.monoid.combine(summary, value.summary);
    }

    return summary;
  }

  async *entries(
    start: LogicalKey | undefined,
    end: LogicalKey | undefined,
    opts?: {
      limit?: number;
      reverse?: boolean;
    },
  ): AsyncIterable<{ key: LogicalKey; value: LogicalValue }> {
    const physicalStart: PhysicalKey<LogicalKey> | undefined = start
      ? [0, ...start]
      : [0];
    const physicalEnd: PhysicalKey<LogicalKey> | undefined = end
      ? [0, ...end]
      : [1];

    for await (
      const physicalEntry of this.kv.list({
        start: physicalStart,
        end: physicalEnd,
      }, opts)
    ) {
      yield {
        key: physicalKeyGetLogicalKey(physicalEntry.key),
        value: physicalEntry.value.logicalValue!,
      };
    }
  }

  allEntries(
    reverse?: boolean,
  ): AsyncIterable<{ key: LogicalKey; value: LogicalValue }> {
    return this.entries(undefined, undefined, { reverse });
  }
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
