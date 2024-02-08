import { KeyPart, KvDriver } from "../kv/types.ts";
import { LiftingMonoid } from "./lifting_monoid.ts";
import { SummarisableStorage } from "./types.ts";

type SimpleKvOpts<
  ValueType extends
    | Uint8Array
    | string
    | number
    | bigint
    | boolean
    | symbol,
  LiftedType,
> = {
  kv: KvDriver;
  compare: (a: ValueType, b: ValueType) => number;
  monoid: LiftingMonoid<ValueType, LiftedType>;
};

export class SimpleKv<ValueType extends KeyPart, LiftedType>
  implements SummarisableStorage<ValueType, LiftedType> {
  private kv: KvDriver;
  private compare: (a: ValueType, b: ValueType) => number;

  private monoid: LiftingMonoid<ValueType, LiftedType>;

  constructor(opts: SimpleKvOpts<ValueType, LiftedType>) {
    this.kv = opts.kv;
    this.compare = opts.compare;

    this.monoid = opts.monoid;
  }

  get(key: ValueType): Promise<Uint8Array | undefined> {
    return this.kv.get(["simple0", key]);
  }

  insert(key: ValueType, value: Uint8Array): Promise<void> {
    return this.kv.set(["simple0", key], value);
  }

  async remove(key: ValueType): Promise<boolean> {
    await this.kv.delete(["simple0", key]);

    return true;
  }

  async *entries(
    start: ValueType | undefined,
    end: ValueType | undefined,
    opts?:
      | { reverse?: boolean | undefined; limit?: number | undefined }
      | undefined,
  ): AsyncIterable<{ key: ValueType; value: Uint8Array }> {
    const argOrder = start && end ? this.compare(start, end) : -1;

    if (argOrder === -1) {
      const iter = this.kv.list({
        start: start ? ["simple0", start] : ["simple0"],
        end: end ? ["simple0", end] : ["simple1"],
      }, {
        limit: opts?.limit,
        reverse: opts?.reverse,
      });

      for await (const entry of iter) {
        yield {
          key: entry.key[1] as ValueType,
          value: entry.value as Uint8Array,
        };
      }

      return;
    } else if (argOrder === 0) {
      const iter = this.kv.list({
        start: ["simple0"],
        end: ["simple1"],
      }, {
        limit: opts?.limit,
        reverse: opts?.reverse,
      });

      for await (const entry of iter) {
        yield {
          key: entry.key[1] as ValueType,
          value: entry.value as Uint8Array,
        };
      }

      return;
    }

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

    if (opts?.reverse) {
      const iter2 = this.kv.list({
        start: ["simple0", start!],
        end: ["simple1"],
      }, {
        limit: opts?.limit,
        reverse: opts?.reverse,
      });

      for await (const entry of iter2) {
        yield {
          key: entry.key[1] as ValueType,
          value: entry.value as Uint8Array,
        };

        if (hitLimit()) break;
      }

      const iter1 = this.kv.list({
        start: ["simple0"],
        end: ["simple0", end!],
      }, {
        limit: opts?.limit,
        reverse: opts?.reverse,
      });

      for await (const entry of iter1) {
        yield {
          key: entry.key[1] as ValueType,
          value: entry.value as Uint8Array,
        };

        if (hitLimit()) break;
      }

      return;
    }

    const iter1 = this.kv.list({
      start: ["simple0"],
      end: ["simple0", end!],
    }, {
      limit: opts?.limit,
      reverse: opts?.reverse,
    });

    for await (const entry of iter1) {
      yield {
        key: entry.key[1] as ValueType,
        value: entry.value as Uint8Array,
      };

      if (hitLimit()) break;
    }

    const iter2 = this.kv.list({
      start: ["simple0", start!],
      end: ["simple1"],
    }, {
      limit: opts?.limit,
      reverse: opts?.reverse,
    });

    for await (const entry of iter2) {
      yield {
        key: entry.key[1] as ValueType,
        value: entry.value as Uint8Array,
      };

      if (hitLimit()) break;
    }
  }

  async *allEntries(): AsyncIterable<{ key: ValueType; value: Uint8Array }> {
    const iter = this.kv.list({ start: ["simple0"], end: ["simple1"] });

    for await (const entry of iter) {
      yield {
        key: entry.key[1] as ValueType,
        value: entry.value as Uint8Array,
      };
    }
  }

  async summarise(
    start: ValueType,
    end: ValueType,
  ): Promise<{ fingerprint: LiftedType; size: number }> {
    let fingerprint = this.monoid.neutral;
    let size = 0;

    for await (const entry of this.entries(start, end)) {
      const lifted = await this.monoid.lift(entry.key, entry.value);

      fingerprint = this.monoid.combine(fingerprint, lifted);
      size += 1;
    }

    return { fingerprint, size };
  }
}
