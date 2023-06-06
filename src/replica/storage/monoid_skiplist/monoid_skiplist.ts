import { Key, KeyPart, KvDriver } from "../kv/types.ts";
import { SummarisableStorage } from "../types.ts";
import { combineMonoid, LiftingMonoid, sizeMonoid } from "../lifting_monoid.ts";
import { deferred } from "https://deno.land/std@0.188.0/async/deferred.ts";

const LAYER_INSERT_PROBABILITY = 0.5;
const LAYER_LEVEL_LIMIT = 64;

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
  kv: KvDriver;
  monoid: LiftingMonoid<ValueType, LiftedType>;
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
  ValueType extends KeyPart,
  LiftedType,
> implements SummarisableStorage<ValueType, LiftedType> {
  private compare: (a: ValueType, b: ValueType) => number;
  private kv: KvDriver;
  private currentHighestLevel = 0;
  private isSetup = deferred();
  private checkedUndoneWork = deferred();
  private monoid: LiftingMonoid<ValueType, [LiftedType, number]>;

  private layerKeyIndex: number;
  private valueKeyIndex: number;

  constructor(opts: SkiplistOpts<ValueType, LiftedType>) {
    this.kv = opts.kv;

    this.layerKeyIndex = this.kv.prefixLevel + 0;
    this.valueKeyIndex = this.kv.prefixLevel + 1;

    this.compare = opts.compare;
    this.monoid = combineMonoid(opts.monoid, sizeMonoid);

    this.checkUndoneWork();
    this.setup();
  }

  async print() {
    for await (
      const entry of this.kv.list(
        {
          start: [0],
          end: [await this.currentLevel()],
        },
      )
    ) {
      console.log(entry);
    }
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

    this.currentHighestLevel = level;

    this.isSetup.resolve();
  }

  private async checkUndoneWork() {
    // Check for presence of insertion or delete operations that were left unfinished.
    const existingInsert = await this.kv.get<[ValueType, Uint8Array]>([
      -1,
      "insert",
    ]);
    const existingRemove = await this.kv.get<ValueType>([
      -1,
      "remove",
    ]);

    if (existingInsert) {
      await this.remove(existingInsert[0], { doNotWaitForCheck: true });
      await this.insert(existingInsert[0], existingInsert[1], {
        doNotWaitForCheck: true,
      });
    }

    if (existingRemove) {
      await this.remove(existingRemove, { doNotWaitForCheck: true });
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

    await this.kv.set([-1, "insert"], [key, value]);

    const level = opts?.layer !== undefined ? opts.layer : randomLevel();

    const batch = this.kv.batch();

    let justInsertedLabel: [LiftedType, number] = this.monoid.neutral;
    let justModifiedPredecessor: [ValueType, [LiftedType, number]] | null =
      null;

    for (let currentLayer = 0; currentLayer < level; currentLayer++) {
      // Compute new values.
      {
        if (currentLayer === 0) {
          const label = this.monoid.lift(key);

          batch.set([currentLayer, key], [
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
                start: [currentLayer - 1, key],
                end: whereToStop
                  ? [currentLayer - 1, whereToStop.key[this.valueKeyIndex]]
                  : [currentLayer],
              },
            )
          ) {
            acc = this.monoid.combine(acc, entry.value[0]);
          }

          batch.set([currentLayer, key], [
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
            prevItem.key[this.valueKeyIndex] as ValueType,
            prevItem.value[0],
          ];

          continue;
        }

        let acc = this.monoid.neutral;

        for await (
          const entry of this.kv.list<SkiplistValue<LiftedType>>({
            start: [currentLayer - 1, prevItem.key[this.valueKeyIndex]],
            end: [
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

        batch.set(prevItem.key, [newLabel, prevItem.value[1]]);

        justModifiedPredecessor = [
          prevItem.key[this.valueKeyIndex] as ValueType,
          newLabel,
        ];
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
            start: [i - 1, itemNeedingNewLabel.key[this.valueKeyIndex]],
            end: whereToStop
              ? [i - 1, whereToStop.key[this.valueKeyIndex]]
              : [i],
          },
        )
      ) {
        if (
          hasUsedJustInserted === false &&
          this.compare(item.key[this.valueKeyIndex] as ValueType, key) > 0
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
            this.compare(
                whereToStop.key[this.valueKeyIndex] as ValueType,
                key,
              ) > 0 ||
          whereToStop !== undefined);

      batch.set(
        itemNeedingNewLabel.key,
        [
          shouldAppend ? this.monoid.combine(acc, justInsertedLabel) : acc,
          itemNeedingNewLabel.value[1],
        ],
      );

      justModifiedPredecessor = [
        itemNeedingNewLabel.key[this.valueKeyIndex] as ValueType,
        acc,
      ];
    }

    await batch.commit();

    await this.kv.delete([-1, "insert"]);

    if (level > await this.currentLevel()) {
      this.setCurrentLevel(level);
    }
  }

  private async getRightItem(
    layer: number,
    key: ValueType,
    abstract = false,
  ) {
    const nextItems = this.kv.list<SkiplistValue<LiftedType>>({
      start: [layer, key],
      end: [layer + 1],
    }, {
      limit: abstract ? 1 : 2,
      batchSize: abstract ? 1 : 2,
    });

    let shouldReturn = abstract;

    for await (const next of nextItems) {
      if (shouldReturn) {
        return next;
      } else {
        shouldReturn = true;
      }
    }

    return undefined;
  }

  private async getRightItemAbstract(
    layer: number,
    key: ValueType,
  ) {
    const nextItems = this.kv.list<SkiplistValue<LiftedType>>({
      start: [layer, key],
      end: [layer + 1],
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
    key: Key,
  ) {
    const nextItems = this.kv.list<SkiplistValue<LiftedType>>({
      start: key,
      end: [(key[this.valueKeyIndex] as number) + 1],
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
      start: [layer],
      end: [layer, key],
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

    await this.kv.set([-1, "remove"], key);

    const batch = this.kv.batch();

    let removed = false;

    let justModified: [ValueType, [LiftedType, number]] | null = null;

    for (let i = 0; i < LAYER_LEVEL_LIMIT; i++) {
      const result = await this.kv.get<SkiplistValue<LiftedType>>([i, key]);

      if (result) {
        removed = true;

        batch.delete([i, key]);
      }

      const precedingValue = await this.getLeftItem(i, key);

      if (precedingValue && i !== 0) {
        const nextValue = await this.getRightItem(
          i,
          key,
          result ? false : true,
        );

        const iter = this.kv.list<SkiplistValue<LiftedType>>({
          start: [i - 1, precedingValue.key[this.valueKeyIndex]],
          end: nextValue ? [i - 1, nextValue.key[this.valueKeyIndex]] : [i],
        });

        let acc = this.monoid.neutral;

        for await (const entry of iter) {
          const entryValue = entry.key[this.valueKeyIndex] as ValueType;

          if (
            justModified && this.compare(
                entryValue,
                justModified[0],
              ) === 0
          ) {
            acc = this.monoid.combine(acc, justModified[1]);
          } else if (this.compare(entryValue, key) === 0) {
            continue;
          } else {
            acc = this.monoid.combine(acc, entry.value[0]);
          }
        }

        justModified = [
          precedingValue.key[this.valueKeyIndex] as ValueType,
          acc,
        ];
        batch.set(precedingValue.key, [acc, precedingValue.value[1]]);

        console.groupEnd();
      }

      const remainingOnThisLayer = this.kv.list<ValueType>({
        start: [i],
        end: [i + 1],
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

    await batch.commit();

    await this.kv.delete([-1, "remove"]);

    return removed;
  }

  async get(key: ValueType) {
    const result = await this.kv.get<SkiplistValue<LiftedType>>([
      0,
      key,
    ]);

    if (result) {
      return result[1];
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
        start: lowerBound ? [layer, lowerBound] : [layer],
        end: upperBound ? [layer, upperBound] : [layer + 1],
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
        const entryValue = entry.key[this.valueKeyIndex] as ValueType;

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
        start: [0, start],
        end: [0, end],
      });

      for await (const entry of results) {
        yield {
          key: entry.key[this.valueKeyIndex] as ValueType,
          value: entry.value[1],
        };
      }
    } else {
      const firstHalf = this.kv.list<SkiplistValue<LiftedType>>({
        start: [0],
        end: [0, end],
      });

      for await (const entry of firstHalf) {
        yield {
          key: entry.key[this.valueKeyIndex] as ValueType,
          value: entry.value[1],
        };
      }

      const secondHalf = this.kv.list<SkiplistValue<LiftedType>>({
        start: [0, start],
        end: [1],
      });

      for await (const entry of secondHalf) {
        yield {
          key: entry.key[this.valueKeyIndex] as ValueType,
          value: entry.value[1],
        };
      }
    }
  }

  async *allEntries(): AsyncIterable<{ key: ValueType; value: Uint8Array }> {
    const iter = this.kv.list<SkiplistValue<LiftedType>>({
      start: [0],
      end: [1],
    });

    for await (const entry of iter) {
      yield {
        key: entry.key[this.valueKeyIndex] as ValueType,
        value: entry.value[1],
      };
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
