import { KvBatch, KvDriver, KvKey } from "./types.ts";

export class PrefixedDriver implements KvDriver {
  private parentDriver: KvDriver;
  private prefix: KvKey;

  constructor(prefix: KvKey, driver: KvDriver) {
    this.parentDriver = driver;
    this.prefix = prefix;
  }

  get<Value>(key: KvKey): Promise<Value | undefined> {
    return this.parentDriver.get([...this.prefix, ...key]);
  }

  set<Value>(key: KvKey, value: Value): Promise<void> {
    return this.parentDriver.set([...this.prefix, ...key], value);
  }

  delete(key: KvKey): Promise<boolean> {
    return this.parentDriver.delete([...this.prefix, ...key]);
  }

  async *list<Value>(
    selector: { start?: KvKey; end?: KvKey; prefix?: KvKey },
    opts?: {
      reverse?: boolean;
      limit?: number;
      batchSize?: number;
    },
  ): AsyncIterable<{ key: KvKey; value: Value }> {
    const selectorPrefixed: {
      start?: KvKey;
      end?: KvKey;
      prefix?: KvKey;
    } = {};

    if (selector.start !== undefined) {
      selectorPrefixed.start = [...this.prefix, ...selector.start];
    }
    if (selector.end !== undefined) {
      selectorPrefixed.end = [...this.prefix, ...selector.end];
    }
    if (selector.prefix !== undefined) {
      selectorPrefixed.prefix = [...this.prefix, ...selector.prefix];
    } else {
      selectorPrefixed.prefix = this.prefix;
    }

    for await (
      const entry of this.parentDriver.list<Value>(
        selectorPrefixed,
        opts,
      )
    ) {
      yield {
        key: <KvKey> entry.key.slice(this.prefix.length),
        value: entry.value,
      };
    }
  }

  clear(
    opts?: { prefix?: KvKey; start?: KvKey; end?: KvKey },
  ): Promise<void> {
    if (opts) {
      return this.parentDriver.clear({
        prefix: opts.prefix === undefined
          ? this.prefix
          : [...this.prefix, ...opts.prefix],
        start: opts.start === undefined
          ? undefined
          : [...this.prefix, ...opts.start],
        end: opts.end === undefined ? undefined : [...this.prefix, ...opts.end],
      });
    }

    return this.parentDriver.clear({prefix: this.prefix});
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