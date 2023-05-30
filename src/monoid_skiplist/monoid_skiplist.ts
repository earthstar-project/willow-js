import { deferred } from "https://deno.land/std@0.188.0/async/deferred.ts";

type LiftingMonoid<
  ValueType extends
    | Uint8Array
    | string
    | number
    | bigint
    | boolean,
  LiftedType,
> = {
  lift: (i: ValueType) => LiftedType;
  combine: (
    a: LiftedType,
    b: LiftedType,
  ) => LiftedType;
  neutral: LiftedType;
};

/** A monoid which lifts the member as a string, and combines by concatenating together. */
export const concatMonoid: LiftingMonoid<string, string> = {
  lift: (a: string) => a,
  combine: (a: string, b: string) => {
    if (a === "0" && b === "0") {
      return "0";
    }

    const fst = a === "0" ? "" : a;
    const snd = b === "0" ? "" : b;

    return fst + snd;
  },
  neutral: "0",
};

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
};

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
  private monoid: LiftingMonoid<ValueType, LiftedType>;

  constructor(opts: SkiplistOpts<ValueType, LiftedType>) {
    this.kv = opts.kv;

    this.compare = opts.compare;
    this.monoid = opts.monoid;

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
      level = entry.key[0] as number;
    }

    this.currentHighestLevel = level;

    this.isSetup.resolve();
  }

  async currentLevel() {
    await this.isSetup;

    return this.currentHighestLevel;
  }

  setCurrentLevel(level: number) {
    this.currentHighestLevel = level;
  }

  async insert(insertedValue: ValueType, layer?: number) {
    const level = layer !== undefined ? layer : randomLevel();
    const atomicOperation = this.kv.atomic();

    let justInsertedLabel: LiftedType = this.monoid.neutral;
    let justModifiedPredecessor: [ValueType, LiftedType] | null = null;

    for (let currentLayer = 0; currentLayer < level; currentLayer++) {
      // Compute new values.
      {
        if (currentLayer === 0) {
          const label = this.monoid.lift(insertedValue);

          atomicOperation.set([currentLayer, insertedValue], label);

          justInsertedLabel = label;
        } else {
          const whereToStop = await this.getRightItemAbstract(
            currentLayer,
            insertedValue,
          );

          let acc = justInsertedLabel;

          for await (
            const entry of this.kv.list<LiftedType>(
              {
                start: [currentLayer - 1, insertedValue],
                end: whereToStop
                  ? [currentLayer - 1, whereToStop.key[1]]
                  : [currentLayer],
              },
            )
          ) {
            acc = this.monoid.combine(acc, entry.value);
          }

          atomicOperation.set([currentLayer, insertedValue], acc);
          justInsertedLabel = acc;
        }
      }

      // Recompute preceding values
      {
        const prevItem = await this.getLeftItem(currentLayer, insertedValue);

        if (!prevItem) {
          continue;
        }

        if (currentLayer === 0) {
          justModifiedPredecessor = [
            prevItem.key[1] as ValueType,
            prevItem.value,
          ];

          continue;
        }

        let acc = this.monoid.neutral;

        for await (
          const entry of this.kv.list<LiftedType>({
            start: [currentLayer - 1, prevItem.key[1]],
            end: [currentLayer - 1, justModifiedPredecessor![0]],
          })
        ) {
          acc = this.monoid.combine(acc, entry.value);
        }

        const newLabel = this.monoid.combine(
          acc,
          justModifiedPredecessor![1],
        );

        atomicOperation.set(prevItem.key, newLabel);

        justModifiedPredecessor = [prevItem.key[1] as ValueType, newLabel];
      }
    }

    for (let i = level; i < await this.currentLevel(); i++) {
      // Recompute preceding values on HIGHER levels.

      const itemNeedingNewLabel = await this.getLeftItem(i, insertedValue);

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
        const item of this.kv.list<LiftedType>(
          {
            start: [i - 1, itemNeedingNewLabel.key[1]],
            end: whereToStop ? [i - 1, whereToStop.key[1]] : [i],
          },
        )
      ) {
        if (
          hasUsedJustInserted === false &&
          this.compare(item.key[1] as ValueType, insertedValue) > 0
        ) {
          acc = this.monoid.combine(acc, justInsertedLabel);
          acc = this.monoid.combine(acc, item.value);

          hasUsedJustInserted = true;
        } else if (
          this.compare(
            item.key[1] as ValueType,
            justModifiedPredecessor![0],
          ) === 0
        ) {
          // do i need this
          acc = this.monoid.combine(acc, justModifiedPredecessor![1]);
        } else {
          acc = this.monoid.combine(acc, item.value);
        }
      }

      // should the acc have the thing appended.
      // only if hasUsedJustInserted is false
      // AND where to stop is greater than than inserted value OR where to stop is undefined

      const shouldAppend = hasUsedJustInserted === false &&
        (whereToStop &&
            this.compare(whereToStop.key[1] as ValueType, insertedValue) > 0 ||
          whereToStop !== undefined);

      atomicOperation.set(
        itemNeedingNewLabel.key,
        shouldAppend ? this.monoid.combine(acc, justInsertedLabel) : acc,
      );

      justModifiedPredecessor = [
        itemNeedingNewLabel.key[1] as ValueType,
        acc,
      ];
    }

    await atomicOperation.commit();

    if (level > await this.currentLevel()) {
      this.setCurrentLevel(level);
    }
  }

  async getBelowItem(key: Deno.KvKey) {
    const res = await this.kv.get<LiftedType>([(key[0] as number) - 1, key[1]]);

    if (res.value === null) {
      throw new Error(
        "Requested below item and did not get a result, malformed skiplist?",
      );
    }

    return res as Deno.KvEntry<LiftedType>;
  }

  async getRightItemAbstract(
    layer: number,
    key: ValueType,
  ) {
    const nextItems = this.kv.list<LiftedType>({
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

  async getRightItemConcrete(
    key: Deno.KvKey,
  ) {
    const nextItems = this.kv.list<LiftedType>({
      start: key,
      end: [(key[0] as number) + 1],
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

  async getLeftItem(layer: number, key: Deno.KvKeyPart) {
    const nextItems = this.kv.list<LiftedType>({
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

  async remove(key: string) {
    let removed = false;

    for (let i = 0; i < LAYER_LEVEL_LIMIT; i++) {
      const result = await this.kv.get([i, key]);

      if (result.value === null) {
        break;
      }

      removed = true;

      await this.kv.delete([i, key]);

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

    return removed;
  }

  async find(key: ValueType) {
    return this.findOnLayer(await this.currentLevel(), key);
  }

  private async findOnLayer(
    layer: number,
    key: ValueType,
    layerStart?: string,
  ): Promise<ValueType | undefined> {
    const results = this.kv.list<ValueType>({
      start: layerStart ? [layer, layerStart] : [layer],
      end: [LAYER_LEVEL_LIMIT + 1],
    });

    let lastSmallerKey = layerStart || "";

    for await (const result of results) {
      const order = this.compare(result.key[1] as ValueType, key);

      //	If the key is the same, we found it. Yay!
      if (order === 0) {
        return result.value;
      }

      // If the result is bigger than what we're looking for,
      // We need to drop down a layer,
      // starting from the previous key we were bigger than.
      if (order > 0) {
        const nextLayerDown = layer - 1;

        if (nextLayerDown < 0) {
          return undefined;
        }

        return this.findOnLayer(nextLayerDown, key, lastSmallerKey);
      }

      // If the key is smaller than the one we are looking for
      // then we set the last smaller key to this.
      if (order < 0) {
        lastSmallerKey = result.key[1] as string;
      }
    }

    const nextLayerDown = layer - 1;

    if (nextLayerDown < 0) {
      return undefined;
    }

    return this.findOnLayer(nextLayerDown, key, lastSmallerKey);
  }

  async summarise(start: ValueType, end: ValueType) {
    const accumulateLabel = async (
      { layer, lowerBound, upperBound }: {
        layer: number;

        lowerBound?: ValueType;
        upperBound?: ValueType;
      },
    ): Promise<LiftedType> => {
      // Loop over layer.
      const iter = this.kv.list<LiftedType>({
        start: lowerBound ? [layer, lowerBound] : [layer],
        end: upperBound ? [layer, upperBound] : [layer + 1],
      });

      let acc = this.monoid.neutral;
      let accumulateCandidate: [ValueType, LiftedType] | null = null;

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
        const entryValue = entry.key[1] as ValueType;

        if (isLessThanLowerBound(entryValue)) {
          continue;
        } else if (isEqualLowerBound(entryValue)) {
          // That is lucky.

          accumulateCandidate = [entryValue, entry.value];
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

          accumulateCandidate = [entryValue, entry.value];
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
      return accumulateLabel({
        layer: await this.currentLevel() - 1,
        lowerBound: start,
        upperBound: end,
      });
    } else if (argOrder > 0) {
      // Do some fancy shit.
      const firstHalf = await accumulateLabel({
        layer: await this.currentLevel() - 1,
        upperBound: end,
      });

      const secondHalf = await accumulateLabel({
        layer: await this.currentLevel() - 1,
        lowerBound: start,
      });

      return this.monoid.combine(firstHalf, secondHalf);
    } else {
      // Also do some fancy shit? Geez.

      return accumulateLabel({
        layer: await this.currentLevel() - 1,
      });
    }
  }

  async leastValue() {
    for await (
      const entry of this.kv.list({
        start: [0],
        end: [1],
      })
    ) {
      return entry.key[1] as ValueType;
    }

    return undefined;
  }

  lnrValues(): AsyncIterable<ValueType> {
    const iter = this.kv.list<LiftedType>({
      start: [0],
      end: [1],
    });

    return {
      [Symbol.asyncIterator]() {
        return {
          async next() {
            const next = await iter.next();

            return Promise.resolve({
              done: next.done,
              value: next.value?.key[1] as ValueType,
            });
          },
        };
      },
    };
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
