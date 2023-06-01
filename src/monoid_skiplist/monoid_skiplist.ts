import { deferred } from "https://deno.land/std@0.188.0/async/deferred.ts";
import { combineMonoid, LiftingMonoid, sizeMonoid } from "./lifting_monoid.ts";

const LAYER_INSERT_PROBABILITY = 0.5;
// TODO: This should be 64 but it is 5 because otherwise we will go over the atomic operations limit.
const LAYER_LEVEL_LIMIT = 5;

type SkiplistOpts<
  ValueType extends
    | Uint8Array
    | string
    | number
    | bigint
    | boolean,
  LiftedType,
> = {
  compare: (a: ValueType, b: ValueType) => number;
  kv: Deno.Kv;
  monoid: LiftingMonoid<ValueType, LiftedType>;
  keyPrefix: string;
};

type SkiplistValue<LiftedType> = [
  /** The precomputed label for this. */
  [LiftedType, number],
  /** The payload associated with this identifier. */
  Uint8Array,
];

// TODO: Add size + items monoid.
// TODO: Raise the limit
// TODO: Add remove method
export class Skiplist<
  ValueType extends
    | Uint8Array
    | string
    | number
    | bigint
    | boolean,
  LiftedType,
> {
  private compare: (a: ValueType, b: ValueType) => number;
  private kv: Deno.Kv;
  private currentHighestLevel = 0;
  private isSetup = deferred();
  private checkedUndoneWork = deferred();
  private monoid: LiftingMonoid<ValueType, [LiftedType, number]>;

  keyPrefix: string;

  constructor(opts: SkiplistOpts<ValueType, LiftedType>) {
    this.kv = opts.kv;

    this.compare = opts.compare;
    this.monoid = combineMonoid(opts.monoid, sizeMonoid);
    this.keyPrefix = opts.keyPrefix;

    this.checkUndoneWork();
    this.setup();
  }

  async print() {
    for await (
      const entry of this.kv.list(
        {
          start: [this.keyPrefix, 0],
          end: [this.keyPrefix, await this.currentLevel()],
        },
      )
    ) {
      console.log(entry);
    }
  }

  private async setup() {
    const lastEntry = this.kv.list({
      start: [this.keyPrefix, 0],
      end: [this.keyPrefix, LAYER_LEVEL_LIMIT + 1],
    }, {
      limit: 1,
      reverse: true,
    });

    let level = 0;

    for await (const entry of lastEntry) {
      level = entry.key[1] as number;
    }

    this.currentHighestLevel = level;

    this.isSetup.resolve();
  }

  private async checkUndoneWork() {
    // Check for presence of insertion or delete operations that were left unfinished.
    const existingInsert = await this.kv.get<[ValueType, Uint8Array]>([
      this.keyPrefix,
      -1,
      "insert",
    ]);
    const existingRemove = await this.kv.get<ValueType>([
      this.keyPrefix,
      -1,
      "remove",
    ]);

    if (existingInsert.value) {
      await this.remove(existingInsert.value[0], { doNotWaitForCheck: true });
      await this.insert(existingInsert.value[0], existingInsert.value[1], {
        doNotWaitForCheck: true,
      });
    }

    if (existingRemove.value) {
      await this.remove(existingRemove.value, { doNotWaitForCheck: true });
    }

    this.checkedUndoneWork.resolve();
  }

  async currentLevel() {
    await this.isSetup;

    return this.currentHighestLevel;
  }

  setCurrentLevel(level: number) {
    this.currentHighestLevel = level;
  }

  async insert(
    key: ValueType,
    value: Uint8Array,
    opts?: { layer?: number; doNotWaitForCheck?: boolean },
  ) {
    if (
      opts?.doNotWaitForCheck === false || opts === undefined ||
      opts.doNotWaitForCheck === undefined
    ) {
      await this.checkedUndoneWork;
    }

    await this.kv.set([this.keyPrefix, -1, "insert"], [key, value]);

    const level = opts?.layer !== undefined ? opts.layer : randomLevel();
    const atomicOperation = this.kv.atomic();

    let justInsertedLabel: [LiftedType, number] = this.monoid.neutral;
    let justModifiedPredecessor: [ValueType, [LiftedType, number]] | null =
      null;

    for (let currentLayer = 0; currentLayer < level; currentLayer++) {
      // Compute new values.
      {
        if (currentLayer === 0) {
          const label = this.monoid.lift(key);

          atomicOperation.set([this.keyPrefix, currentLayer, key], [
            label,
            value,
          ]);

          justInsertedLabel = label;
        } else {
          const whereToStop = await this.getRightItemAbstract(
            currentLayer,
            key,
          );

          let acc = justInsertedLabel;

          for await (
            const entry of this.kv.list<SkiplistValue<LiftedType>>(
              {
                start: [this.keyPrefix, currentLayer - 1, key],
                end: whereToStop
                  ? [this.keyPrefix, currentLayer - 1, whereToStop.key[2]]
                  : [this.keyPrefix, currentLayer],
              },
            )
          ) {
            acc = this.monoid.combine(acc, entry.value[0]);
          }

          atomicOperation.set([this.keyPrefix, currentLayer, key], [
            acc,
            value,
          ]);
          justInsertedLabel = acc;
        }
      }

      // Recompute preceding values
      {
        const prevItem = await this.getLeftItem(currentLayer, key);

        if (!prevItem) {
          continue;
        }

        if (currentLayer === 0) {
          justModifiedPredecessor = [
            prevItem.key[2] as ValueType,
            prevItem.value[0],
          ];

          continue;
        }

        let acc = this.monoid.neutral;

        for await (
          const entry of this.kv.list<SkiplistValue<LiftedType>>({
            start: [this.keyPrefix, currentLayer - 1, prevItem.key[2]],
            end: [
              this.keyPrefix,
              currentLayer - 1,
              justModifiedPredecessor![0],
            ],
          })
        ) {
          acc = this.monoid.combine(acc, entry.value[0]);
        }

        const newLabel = this.monoid.combine(
          acc,
          justModifiedPredecessor![1],
        );

        atomicOperation.set(prevItem.key, [newLabel, prevItem.value[1]]);

        justModifiedPredecessor = [prevItem.key[2] as ValueType, newLabel];
      }
    }

    for (let i = level; i < await this.currentLevel(); i++) {
      // Recompute preceding values on HIGHER levels.

      const itemNeedingNewLabel = await this.getLeftItem(i, key);

      if (!itemNeedingNewLabel) {
        // There won't be any other items on higher levels.
        break;
      }

      const whereToStop = await this.getRightItemConcrete(
        itemNeedingNewLabel.key,
      );

      let acc = this.monoid.neutral;

      let hasUsedJustInserted = false;

      // Accumulate values until next is read.
      for await (
        const item of this.kv.list<SkiplistValue<LiftedType>>(
          {
            start: [this.keyPrefix, i - 1, itemNeedingNewLabel.key[2]],
            end: whereToStop
              ? [this.keyPrefix, i - 1, whereToStop.key[2]]
              : [this.keyPrefix, i],
          },
        )
      ) {
        if (
          hasUsedJustInserted === false &&
          this.compare(item.key[2] as ValueType, key) > 0
        ) {
          acc = this.monoid.combine(acc, justInsertedLabel);
          acc = this.monoid.combine(acc, item.value[0]);

          hasUsedJustInserted = true;
        } else {
          acc = this.monoid.combine(acc, item.value[0]);
        }
      }

      // should the acc have the thing appended.
      // only if hasUsedJustInserted is false
      // AND where to stop is greater than than inserted value OR where to stop is undefined

      const shouldAppend = hasUsedJustInserted === false &&
        (whereToStop &&
            this.compare(whereToStop.key[2] as ValueType, key) > 0 ||
          whereToStop !== undefined);

      atomicOperation.set(
        itemNeedingNewLabel.key,
        [
          shouldAppend ? this.monoid.combine(acc, justInsertedLabel) : acc,
          itemNeedingNewLabel.value[1],
        ],
      );

      justModifiedPredecessor = [
        itemNeedingNewLabel.key[2] as ValueType,
        acc,
      ];
    }

    await atomicOperation.commit();

    await this.kv.delete([this.keyPrefix, -1, "insert"]);

    if (level > await this.currentLevel()) {
      this.setCurrentLevel(level);
    }
  }

  private async getRightItemAbstract(
    layer: number,
    key: ValueType,
  ) {
    const nextItems = this.kv.list<SkiplistValue<LiftedType>>({
      start: [this.keyPrefix, layer, key],
      end: [this.keyPrefix, layer + 1],
    }, {
      limit: 1,
      batchSize: 1,
    });

    for await (const next of nextItems) {
      return next;
    }

    return undefined;
  }

  private async getRightItemConcrete(
    key: Deno.KvKey,
  ) {
    const nextItems = this.kv.list<SkiplistValue<LiftedType>>({
      start: key,
      end: [this.keyPrefix, (key[1] as number) + 1],
    }, {
      limit: 2,
      batchSize: 2,
    });

    let shouldReturn = false;

    for await (const next of nextItems) {
      if (shouldReturn) {
        return next;
      } else {
        shouldReturn = true;
      }
    }

    return undefined;
  }

  private async getLeftItem(layer: number, key: Deno.KvKeyPart) {
    const nextItems = this.kv.list<SkiplistValue<LiftedType>>({
      start: [this.keyPrefix, layer],
      end: [this.keyPrefix, layer, key],
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

  async remove(key: ValueType, opts?: { doNotWaitForCheck?: boolean }) {
    if (
      opts?.doNotWaitForCheck === false || opts === undefined ||
      opts.doNotWaitForCheck === undefined
    ) {
      await this.checkedUndoneWork;
    }

    await this.kv.set([this.keyPrefix, -1, "remove"], key);

    let removed = false;

    for (let i = 0; i < LAYER_LEVEL_LIMIT; i++) {
      const result = await this.kv.get([this.keyPrefix, i, key]);

      if (result.value === null) {
        break;
      }

      removed = true;

      await this.kv.delete([this.keyPrefix, i, key]);

      const remainingOnThisLayer = this.kv.list<ValueType>({
        start: [this.keyPrefix, i],
        end: [this.keyPrefix, i + 1],
      }, {
        limit: 1,
      });

      let canForgetLayer = true;

      for await (const _result of remainingOnThisLayer) {
        canForgetLayer = false;
      }

      if (canForgetLayer && i > await this.currentLevel()) {
        this.setCurrentLevel(i - 1);
      }
    }

    await this.kv.delete([this.keyPrefix, -1, "remove"]);

    return removed;
  }

  async find(key: ValueType) {
    const result = await this.kv.get<SkiplistValue<LiftedType>>([
      this.keyPrefix,
      0,
      key,
    ]);

    if (result.value) {
      return result.value[1];
    }

    return undefined;
  }

  async summarise(
    start: ValueType,
    end: ValueType,
  ): Promise<{ fingerprint: LiftedType; size: number }> {
    const accumulateLabel = async (
      { layer, lowerBound, upperBound }: {
        layer: number;

        lowerBound?: ValueType;
        upperBound?: ValueType;
      },
    ): Promise<[LiftedType, number]> => {
      // Loop over layer.
      const iter = this.kv.list<SkiplistValue<LiftedType>>({
        start: lowerBound
          ? [this.keyPrefix, layer, lowerBound]
          : [this.keyPrefix, layer],
        end: upperBound
          ? [this.keyPrefix, layer, upperBound]
          : [this.keyPrefix, layer + 1],
      });

      let acc = this.monoid.neutral;
      let accumulateCandidate: [ValueType, [LiftedType, number]] | null = null;

      const isLessThanLowerBound = (value: ValueType) => {
        if (lowerBound) {
          return this.compare(value, lowerBound) < 0;
        }

        return false;
      };

      const isEqualLowerBound = (value: ValueType) => {
        if (lowerBound) {
          return this.compare(value, lowerBound) === 0;
        }

        return false;
      };

      const isGtLowerBound = (value: ValueType) => {
        if (lowerBound) {
          return this.compare(value, lowerBound) > 0;
        }

        return true;
      };

      const isLessThanUpperBound = (value: ValueType) => {
        if (upperBound) {
          return this.compare(value, upperBound) < 0;
        }

        return true;
      };

      const isEqualUpperbound = (value: ValueType) => {
        if (upperBound) {
          return this.compare(value, upperBound) === 0;
        }

        return false;
      };

      const isGtUpperbound = (value: ValueType) => {
        if (upperBound) {
          return this.compare(value, upperBound) > 0;
        }

        return false;
      };

      let foundHead = false;
      let foundTail = false;

      for await (const entry of iter) {
        const entryValue = entry.key[2] as ValueType;

        if (isLessThanLowerBound(entryValue)) {
          continue;
        } else if (isEqualLowerBound(entryValue)) {
          // That is lucky.

          accumulateCandidate = [entryValue, entry.value[0]];
          foundHead = true;
        } else if (
          isGtLowerBound(entryValue) && isLessThanUpperBound(entryValue)
        ) {
          if (foundHead === false) {
            // Get the head from the layers below.
            acc = await accumulateLabel({
              layer: layer - 1,
              lowerBound,
              upperBound: entryValue,
            });

            foundHead = true;
          } else {
            if (accumulateCandidate) {
              acc = this.monoid.combine(acc, accumulateCandidate[1]);
            }
          }

          accumulateCandidate = [entryValue, entry.value[0]];
        } else if (isEqualUpperbound(entryValue)) {
          // Hooray!
          // Accumulate last value.
          if (accumulateCandidate) {
            acc = this.monoid.combine(acc, accumulateCandidate[1]);
          }

          break;
        } else if (isGtUpperbound(entryValue)) {
          // The last accumulation candidate's label is invalid because it includes too many items.

          if (layer > 0) {
            const tailResult = await accumulateLabel({
              layer: layer - 1,
              lowerBound: accumulateCandidate
                ? accumulateCandidate[0]
                : lowerBound,
              upperBound: upperBound,
            });

            acc = this.monoid.combine(acc, tailResult);
          }

          foundTail = true;
          break;
        }
      }

      // The loop is now over. Now what?
      //  If we didn't find the tail yet, find it here.

      if (foundTail === false && layer > 0) {
        const tailResult = await accumulateLabel({
          layer: layer - 1,
          lowerBound: accumulateCandidate ? accumulateCandidate[0] : lowerBound,
          upperBound: upperBound,
        });

        acc = this.monoid.combine(acc, tailResult);
      } else if (foundTail === false && layer === 0 && accumulateCandidate) {
        acc = this.monoid.combine(acc, accumulateCandidate[1]);
      }

      return acc;
    };

    const argOrder = this.compare(start, end);

    if (argOrder < 0) {
      const [fingerprint, size] = await accumulateLabel({
        layer: await this.currentLevel() - 1,
        lowerBound: start,
        upperBound: end,
      });

      return { fingerprint, size };
    } else if (argOrder > 0) {
      const firstHalf = await accumulateLabel({
        layer: await this.currentLevel() - 1,
        upperBound: end,
      });

      const secondHalf = await accumulateLabel({
        layer: await this.currentLevel() - 1,
        lowerBound: start,
      });

      const [fingerprint, size] = this.monoid.combine(firstHalf, secondHalf);

      return { fingerprint, size };
    } else {
      const [fingerprint, size] = await accumulateLabel({
        layer: await this.currentLevel() - 1,
      });

      return { fingerprint, size };
    }
  }

  async *entries(
    start: ValueType,
    end: ValueType,
  ): AsyncIterable<{ key: ValueType; value: Uint8Array }> {
    const argOrder = this.compare(start, end);

    if (argOrder === 0) {
      for await (const result of this.allEntries()) {
        yield result;
      }
    } else if (argOrder < 0) {
      const results = this.kv.list<SkiplistValue<LiftedType>>({
        start: [this.keyPrefix, 0, start],
        end: [this.keyPrefix, 0, end],
      });

      for await (const entry of results) {
        yield { key: entry.key[2] as ValueType, value: entry.value[1] };
      }
    } else {
      const firstHalf = this.kv.list<SkiplistValue<LiftedType>>({
        start: [this.keyPrefix, 0],
        end: [this.keyPrefix, 0, end],
      });

      for await (const entry of firstHalf) {
        yield { key: entry.key[2] as ValueType, value: entry.value[1] };
      }

      const secondHalf = this.kv.list<SkiplistValue<LiftedType>>({
        start: [this.keyPrefix, 0, start],
        end: [this.keyPrefix, 1],
      });

      for await (const entry of secondHalf) {
        yield { key: entry.key[2] as ValueType, value: entry.value[1] };
      }
    }
  }

  async *allEntries(): AsyncIterable<{ key: ValueType; value: Uint8Array }> {
    const iter = this.kv.list<SkiplistValue<LiftedType>>({
      start: [this.keyPrefix, 0],
      end: [this.keyPrefix, 1],
    });

    for await (const entry of iter) {
      yield { key: entry.key[2] as ValueType, value: entry.value[1] };
    }
  }
}

function randomLevel() {
  let level = 1;

  while (
    Math.random() <= LAYER_INSERT_PROBABILITY && level < LAYER_LEVEL_LIMIT
  ) {
    level += 1;
  }

  return level;
}
