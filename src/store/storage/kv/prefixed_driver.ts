import { Key, KvBatch, KvDriver } from "./types.ts";

export class PrefixedDriver implements KvDriver {
  private parentDriver: KvDriver;
  private prefix: Key;

  constructor(prefix: Key, driver: KvDriver) {
    this.parentDriver = driver;
    this.prefix = prefix;
  }

  get<ValueType>(key: Key): Promise<ValueType | undefined> {
    return this.parentDriver.get([...this.prefix, ...key]);
  }

  set(key: Key, value: unknown): Promise<void> {
    return this.parentDriver.set([...this.prefix, ...key], value);
  }

  delete(key: Key): Promise<void> {
    return this.parentDriver.delete([...this.prefix, ...key]);
  }

  async *list<ValueType>(
    selector: { start: Key; end: Key } | { prefix: Key },
    opts?: {
      reverse?: boolean;
      limit?: number;
      batchSize?: number;
    },
  ): AsyncIterable<{ key: Key; value: ValueType }> {
    const selectorPrefixed = "start" in selector
      ? {
        start: [...this.prefix, ...selector.start],
        end: [...this.prefix, ...selector.end],
      }
      : {
        prefix: [...this.prefix, ...selector.prefix],
      };

    // console.log({ selectorPrefixed });

    for await (
      const entry of this.parentDriver.list<ValueType>(
        selectorPrefixed,
        opts,
      )
    ) {
      yield {
        key: entry.key.slice(this.prefix.length),
        value: entry.value,
      };
    }
  }

  clear(
    opts?: { prefix: Key; start: Key; end: Key } | undefined,
  ): Promise<void> {
    if (opts) {
      return this.parentDriver.clear({
        prefix: [...this.prefix, ...opts.prefix],
        start: [...this.prefix, ...opts.start],
        end: [...this.prefix, ...opts.start],
      });
    }

    return this.parentDriver.clear();
  }

  batch(): KvBatch {
    const prefix = this.prefix;
    const batch = this.parentDriver.batch();

    return {
      set(key, value) {
        return batch.set([...prefix, ...key], value);
      },
      delete(key) {
        return batch.delete([...prefix, ...key]);
      },
      commit: batch.commit,
    };
  }
}
