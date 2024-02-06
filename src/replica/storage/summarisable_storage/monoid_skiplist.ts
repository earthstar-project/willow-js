import { deferred } from "../../../../deps.ts";
import { KeyPart, KvDriver } from "../kv/types.ts";
import { combineMonoid, LiftingMonoid, sizeMonoid } from "./lifting_monoid.ts";
import { SummarisableStorage } from "./types.ts";

const LAYER_INSERT_PROBABILITY = 0.5;
const LAYER_LEVEL_LIMIT = 64;

type SkiplistOpts<
  ValueType extends
    | Uint8Array
    | string
    | number
    | bigint
    | boolean
    | symbol,
  LiftedType,
> = {
  compare: (a: ValueType, b: ValueType) => number;
  kv: KvDriver;
  monoid: LiftingMonoid<ValueType, LiftedType>;
};

type SkiplistBaseValue<LiftedType> = [number, [LiftedType, number], Uint8Array];

type SkiplistValue<LiftedType> = [number, [LiftedType, number]];

export class Skiplist<
  ValueType extends KeyPart,
  LiftedType,
> implements SummarisableStorage<ValueType, LiftedType> {
  private compare: (a: ValueType, b: ValueType) => number;
  private kv: KvDriver;
  private _maxHeight = 0;
  private isSetup = deferred();
  private monoid: LiftingMonoid<ValueType, [LiftedType, number]>;

  private layerKeyIndex = 0;
  private valueKeyIndex = 1;

  constructor(opts: SkiplistOpts<ValueType, LiftedType>) {
    this.kv = opts.kv;

    this.compare = opts.compare;
    this.monoid = combineMonoid(opts.monoid, sizeMonoid);

    this.setup();
  }

  async print() {
    console.group("Skiplist contents");

    const map: Map<ValueType, Record<number, [LiftedType, number]>> = new Map();

    for await (
      const entry of this.kv.list<SkiplistValue<LiftedType>>(
        {
          start: [0],
          end: [await this.maxHeight() + 1],
        },
      )
    ) {
      const key = entry.key[this.valueKeyIndex] as ValueType;

      const valueEntry = map.get(key);

      if (valueEntry) {
        map.set(key, {
          ...valueEntry,
          [entry.key[0] as number]: entry.value[1],
        });

        continue;
      }

      map.set(key, {
        [entry.key[0] as number]: entry.value[1],
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
    key: ValueType,
    value: Uint8Array,
    opts?: { layer?: number },
  ) {
    await this.isSetup;

    const existing = await this.kv.get<SkiplistBaseValue<LiftedType>>([
      0,
      key,
    ]);

    if (existing) {
      await this.kv.set([0, key], [
        existing[0],
        existing[1],
        value,
      ] as SkiplistBaseValue<LiftedType>);

      return;
    }

    const batch = this.kv.batch();

    const insertionHeight = opts?.layer !== undefined
      ? opts.layer
      : randomHeight();

    // console.log(`["${key}", ${insertionHeight}],`);

    let previousSuccessor: { key: ValueType; height: number } | undefined =
      undefined;
    let previousComputedLabel: [LiftedType, number] | undefined = undefined;

    const liftedValue = await this.monoid.lift(key, value);

    for (let layer = 0; layer <= insertionHeight; layer++) {
      // On the first layer we don't need to compute any labels, as there is no skipping.
      // But unlike higher layeys, we DO need to store the payload hash.
      if (layer === 0) {
        const label = await this.monoid.lift(key, value);

        previousComputedLabel = label;

        batch.set([layer, key], [
          insertionHeight,
          label,
          value,
        ] as SkiplistBaseValue<LiftedType>);

        continue;
      }

      // On levels higher than 0, we may need to recompute labels.

      // If the previous successor's height is greater than current layer, great!
      // we can just reuse that value and continue upwards.
      if (
        previousSuccessor && previousSuccessor.height > layer &&
        previousComputedLabel
      ) {
        batch.set(
          [layer, key],
          [insertionHeight, previousComputedLabel] as SkiplistValue<LiftedType>,
        );
        continue;
      }

      // The next sibling is unknown, so we can't reuse a cached value.

      const nextSuccessor = await this.getRightItem(layer, key, true);

      // But if there is a previous successor, we can use the previously computed label
      // and start iterating from there.

      /** The newly computed label for the inserted value. */
      let newLabel: [LiftedType, number] = previousComputedLabel ||
        liftedValue;

      // If there is a previous successor, we can start from there instead of from the insertion key
      // as we already have the computed label up to that point.
      const startFrom = previousSuccessor ? previousSuccessor.key : key;

      for await (
        const entry of this.kv.list<SkiplistValue<LiftedType>>(
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
      ] as SkiplistValue<LiftedType>);

      previousComputedLabel = newLabel;
      previousSuccessor = nextSuccessor
        ? {
          key: nextSuccessor.key[this.valueKeyIndex] as ValueType,
          height: nextSuccessor.value[0],
        }
        : undefined;
    }

    // Now we recompute the preceding labels.

    // For preceding values
    let previousPredecessor: { key: ValueType; height: number } | undefined =
      undefined;
    let previousPredecessorComputedLabel: [LiftedType, number] | undefined =
      undefined;

    //  For overarching preceding values
    let previousOverarchingPredecessor:
      | { key: ValueType; height: number }
      | undefined = previousPredecessor;
    let previousOverarchingPredecessorComputedLabel:
      | [LiftedType, number]
      | undefined = previousPredecessorComputedLabel;
    let previousOverarchingSuccessorHeight: number | undefined;

    if (previousSuccessor && previousSuccessor.height <= insertionHeight) {
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
            ] as SkiplistValue<LiftedType>,
          );

          continue;
        }

        const predecessor = await this.getLeftItem(layer, key);

        if (!predecessor) {
          // If there's no predecessor here, there won't be any on the layers above.
          break;
        }

        let newLabel = this.monoid.neutral;

        for await (
          const entry of this.kv.list<SkiplistValue<LiftedType>>(
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
        ] as SkiplistValue<LiftedType>);

        previousPredecessorComputedLabel = newLabel;

        previousPredecessor = {
          key: predecessor.key[this.valueKeyIndex] as ValueType,
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
        ] as SkiplistValue<LiftedType>);

        continue;
      }

      const overarchingPredecessor = await this.getLeftItem(layer, key);

      if (!overarchingPredecessor) {
        // If there's no overarching predecessor here, there never will be.
        break;
      }

      const overarchingSuccessor = await this.getRightItem(
        layer,
        overarchingPredecessor.key[this.valueKeyIndex] as ValueType,
      );

      let newLabel = this.monoid.neutral;

      for await (
        const entry of this.kv.list<SkiplistValue<LiftedType>>(
          {
            start: [layer - 1, overarchingPredecessor.key[this.valueKeyIndex]],
            end: previousOverarchingPredecessor
              ? [layer - 1, previousOverarchingPredecessor.key]
              : [layer - 1, key],
          },
        )
      ) {
        const overarchingKey = overarchingPredecessor
          .key[this.valueKeyIndex] as ValueType;

        if (this.compare(key, overarchingKey) === 0) {
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

      if (previousSuccessor) {
        for await (
          const entry of this.kv.list<SkiplistValue<LiftedType>>(
            {
              start: [layer - 1, previousSuccessor.key],
              end: overarchingSuccessor
                ? [
                  layer - 1,
                  overarchingSuccessor.key[this.valueKeyIndex] as ValueType,
                ]
                : [layer - 1, key],
            },
          )
        ) {
          newLabel = this.monoid.combine(newLabel, entry.value[1]);
        }
      }

      batch.set(overarchingPredecessor.key, [
        overarchingPredecessor.value[0],
        newLabel,
      ] as SkiplistValue<LiftedType>);

      previousComputedLabel = undefined;
      previousSuccessor = undefined;

      previousOverarchingPredecessor = {
        key: overarchingPredecessor.key[this.valueKeyIndex] as ValueType,
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

  async remove(key: ValueType) {
    const toRemoveBase = await this.kv.get<SkiplistBaseValue<LiftedType>>([
      0,
      key,
    ]);

    if (!toRemoveBase) {
      return false;
    }

    const batch = this.kv.batch();

    const height = toRemoveBase[0];

    let previousLabel: [ValueType, [LiftedType, number]] | null = null;
    let previousSuccessor: ValueType | undefined;

    for (let layer = 0; layer <= height; layer++) {
      batch.delete([layer, key]);
    }

    for (let layer = 1; layer <= this._maxHeight; layer++) {
      const predecessor = await this.getLeftItem(layer, key);

      if (!predecessor) {
        break;
      }

      const successor = await this.getRightItem(
        layer,
        layer <= height
          ? key
          : predecessor.key[this.valueKeyIndex] as ValueType,
      );

      if (layer - 1 === 0) {
        const fromPredecessorToSuccessor = this.kv.list<
          SkiplistBaseValue<LiftedType>
        >({
          start: [0, predecessor.key[this.valueKeyIndex]],
          end: successor ? [0, successor.key[this.valueKeyIndex]] : [1],
        });

        let newLabel = this.monoid.neutral;

        for await (const entry of fromPredecessorToSuccessor) {
          const entryKey = entry.key[this.valueKeyIndex] as ValueType;
          if (this.compare(entryKey, key) === 0) {
            continue;
          }

          newLabel = this.monoid.combine(newLabel, entry.value[1]);
        }

        const predecessorVal = await this.get(
          predecessor.key[this.valueKeyIndex] as ValueType,
        );

        batch.set(
          predecessor.key,
          [
            predecessor.value[0],
            newLabel,
            predecessorVal,
          ] as SkiplistBaseValue<LiftedType>,
        );

        previousLabel = [
          predecessor.key[this.valueKeyIndex] as ValueType,
          newLabel,
        ];

        previousSuccessor = successor
          ? successor.key[this.valueKeyIndex] as ValueType
          : undefined;

        continue;
      }

      const predecessorIsSame = (!predecessor && !previousLabel) ||
        predecessor && previousLabel &&
          this.compare(
              previousLabel[0],
              predecessor.key[this.valueKeyIndex] as ValueType,
            ) === 0;

      const successorIsSame = (!successor && !previousSuccessor) ||
        successor && previousSuccessor &&
          this.compare(
              previousSuccessor,
              successor.key[this.valueKeyIndex] as ValueType,
            ) === 0;

      if (predecessorIsSame && successorIsSame && previousLabel) {
        batch.set(
          predecessor.key,
          [predecessor.value[0], previousLabel[1]] as SkiplistValue<LiftedType>,
        );

        continue;
      }

      const fromPredecessorToSuccessor = this.kv.list<
        SkiplistValue<LiftedType>
      >({
        start: [layer - 1, predecessor.key[this.valueKeyIndex]],
        end: successor
          ? [layer - 1, successor.key[this.valueKeyIndex]]
          : [layer],
      });

      let newLabel = this.monoid.neutral;

      for await (const entry of fromPredecessorToSuccessor) {
        const entryValue = entry.key[this.valueKeyIndex] as ValueType;

        if (this.compare) {
          if (
            previousLabel && this.compare(
                entryValue,
                previousLabel[0],
              ) === 0
          ) {
            newLabel = this.monoid.combine(newLabel, previousLabel[1]);
          } else if (this.compare(entryValue, key) === 0) {
            continue;
          } else {
            newLabel = this.monoid.combine(newLabel, entry.value[1]);
          }
        }
      }

      batch.set(
        predecessor.key,
        [predecessor.value[0], newLabel] as SkiplistValue<LiftedType>,
      );

      previousLabel = [
        predecessor.key[this.valueKeyIndex] as ValueType,
        newLabel,
      ];

      previousSuccessor = successor
        ? successor.key[this.valueKeyIndex] as ValueType
        : undefined;

      const remainingOnThisLayer = this.kv.list<ValueType>({
        start: [layer],
        end: [layer + 1],
      }, {
        limit: 1,
      });

      let canForgetLayer = true;

      for await (const _result of remainingOnThisLayer) {
        canForgetLayer = false;
      }

      if (canForgetLayer && layer > await this.maxHeight()) {
        this.setMaxHeight(layer - 1);
      }
    }

    await batch.commit();

    return true;
  }

  async get(key: ValueType) {
    const result = await this.kv.get<SkiplistBaseValue<LiftedType>>([
      0,
      key,
    ]);

    if (result) {
      return result[2];
    }

    return undefined;
  }

  async summarise(
    start: ValueType,
    end: ValueType,
  ): Promise<{ fingerprint: LiftedType; size: number }> {
    // Get the first value.

    // For each value you find, shoot up.

    // Keep moving rightwards until you overshoot.

    // Assemble the tail using start: key where things started to go wrong, end: end.

    const accumulateUntilOvershoot = async (
      start: ValueType | undefined,
      end: ValueType | undefined,
      layer: number,
    ): Promise<
      { label: [LiftedType, number]; overshootKey: ValueType | undefined }
    > => {
      const startUntilEnd = this.kv.list<SkiplistValue<LiftedType>>({
        start: start ? [layer, start] : [layer],
        end: end ? [layer, end] : [layer + 1],
      });

      let label = this.monoid.neutral;

      let candidateLabel = this.monoid.neutral;
      let overshootKey: ValueType | undefined;

      for await (const entry of startUntilEnd) {
        label = this.monoid.combine(label, candidateLabel);
        candidateLabel = this.monoid.neutral;

        const [entryHeight, entryLabel] = entry.value;

        if (entryHeight <= layer) {
          overshootKey = entry.key[this.valueKeyIndex] as ValueType;
          candidateLabel = entryLabel;
          continue;
        }

        const { label: aboveLabel, overshootKey: aboveOvershootKey } =
          await accumulateUntilOvershoot(
            start,
            end,
            entryHeight,
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
      overshootLabel: ValueType,
      end: ValueType | undefined,
      layer: number,
    ): Promise<[LiftedType, number]> => {
      const endUntilOvershoot = this.kv.list<SkiplistValue<LiftedType>>({
        start: [layer, overshootLabel],
        end: end ? [layer, end] : [layer + 1],
      }, {
        reverse: true,
      });

      let label = this.monoid.neutral;

      for await (const entry of endUntilOvershoot) {
        const [entryHeight, entryLabel] = entry.value;

        label = this.monoid.combine(entryLabel, label);

        if (entryHeight <= layer) {
          continue;
        }

        const entryValue = entry.key[this.valueKeyIndex] as ValueType;

        const aboveLabel = await getTail(
          overshootLabel,
          entryValue,
          entryHeight,
        );

        label = this.monoid.combine(aboveLabel, label);

        break;
      }

      return label;
    };

    const accumulateLabel = async (
      { start, end }: {
        start?: ValueType;
        end?: ValueType;
      },
    ) => {
      const firstEntry = this.kv.list<SkiplistBaseValue<LiftedType>>({
        start: start ? [0, start] : [0],
        end: [1],
      }, {
        limit: 1,
      });

      for await (const first of firstEntry) {
        const height = first.value[0];

        const { label: headLabel, overshootKey } =
          await accumulateUntilOvershoot(
            start,
            end,
            height,
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

    const argOrder = this.compare(start, end);

    if (argOrder < 0) {
      const [fingerprint, size] = await accumulateLabel({ start, end });

      return { fingerprint, size };
    } else if (argOrder > 0) {
      const firstHalf = await accumulateLabel({ end });

      const secondHalf = await accumulateLabel({ start });

      const [fingerprint, size] = this.monoid.combine(firstHalf, secondHalf);

      return { fingerprint, size };
    } else {
      const [fingerprint, size] = await accumulateLabel({});

      return { fingerprint, size };
    }
  }

  async *entries(
    start: ValueType | undefined,
    end: ValueType | undefined,
    opts?: {
      limit?: number;
      reverse?: boolean;
    },
  ): AsyncIterable<{ key: ValueType; value: Uint8Array }> {
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

    if (!start && !end) {
      for await (const result of this.allEntries(opts?.reverse)) {
        yield result;
        if (hitLimit()) break;
      }

      return;
    }

    const argOrder = start && end ? this.compare(start, end) : -1;

    if (argOrder === 0) {
      for await (const result of this.allEntries(opts?.reverse)) {
        yield result;

        if (hitLimit()) break;
      }
    } else if (argOrder < 0) {
      const results = this.kv.list<SkiplistBaseValue<LiftedType>>({
        start: start ? [0, start] : [0],
        end: end ? [0, end] : [1],
      }, {
        limit: opts?.limit,
        reverse: opts?.reverse,
      });

      for await (const entry of results) {
        yield {
          key: entry.key[this.valueKeyIndex] as ValueType,
          value: entry.value[2],
        };
      }
    } else if (opts?.reverse) {
      const secondHalf = this.kv.list<SkiplistBaseValue<LiftedType>>({
        start: start ? [0, start] : [0],
        end: [1],
      }, { reverse: true });

      for await (const entry of secondHalf) {
        yield {
          key: entry.key[this.valueKeyIndex] as ValueType,
          value: entry.value[2],
        };

        if (hitLimit()) break;
      }

      const firstHalf = this.kv.list<SkiplistBaseValue<LiftedType>>({
        start: [0],
        end: end ? [0, end] : [1],
      }, { reverse: true });

      for await (const entry of firstHalf) {
        yield {
          key: entry.key[this.valueKeyIndex] as ValueType,
          value: entry.value[2],
        };

        if (hitLimit()) break;
      }
    } else {
      const firstHalf = this.kv.list<SkiplistBaseValue<LiftedType>>({
        start: [0],
        end: end ? [0, end] : [1],
      });

      for await (const entry of firstHalf) {
        yield {
          key: entry.key[this.valueKeyIndex] as ValueType,
          value: entry.value[2],
        };

        if (hitLimit()) break;
      }

      const secondHalf = this.kv.list<SkiplistBaseValue<LiftedType>>({
        start: start ? [0, start] : [0],
        end: [1],
      });

      for await (const entry of secondHalf) {
        yield {
          key: entry.key[this.valueKeyIndex] as ValueType,
          value: entry.value[2],
        };

        if (hitLimit()) break;
      }
    }
  }

  async *allEntries(
    reverse?: boolean,
  ): AsyncIterable<{ key: ValueType; value: Uint8Array }> {
    const iter = this.kv.list<SkiplistBaseValue<LiftedType>>({
      start: [0],
      end: [1],
    }, {
      reverse,
    });

    for await (const entry of iter) {
      yield {
        key: entry.key[this.valueKeyIndex] as ValueType,
        value: entry.value[2],
      };
    }
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
