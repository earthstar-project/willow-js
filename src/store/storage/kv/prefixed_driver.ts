import { KeyPart, KvBatch, KvDriver } from "./types.ts";

export class PrefixedDriver<
  Prefix extends KeyPart[],
  Key extends KeyPart[],
  Value,
> implements KvDriver<Key, Value> {
  private parentDriver: KvDriver<[...Prefix, ...Key], Value>;
  private prefix: Prefix;

  constructor(prefix: Prefix, driver: KvDriver<[...Prefix, ...Key], Value>) {
    this.parentDriver = driver;
    this.prefix = prefix;
  }

  get(key: Key): Promise<Value | undefined> {
    return this.parentDriver.get([...this.prefix, ...key]);
  }

  set(key: Key, value: Value): Promise<void> {
    return this.parentDriver.set([...this.prefix, ...key], value);
  }

  delete(key: Key): Promise<boolean> {
    return this.parentDriver.delete([...this.prefix, ...key]);
  }

  async *list(
    selector: { start?: Key; end?: Key; prefix?: Key },
    opts?: {
      reverse?: boolean;
      limit?: number;
      batchSize?: number;
    },
  ): AsyncIterable<{ key: Key; value: Value }> {
    const selectorPrefixed: {
      start?: [...Prefix, ...Key];
      end?: [...Prefix, ...Key];
      prefix?: [...Prefix, ...Key];
    } = {};

    if (selector.start !== undefined) {
      selectorPrefixed.start = [...this.prefix, ...selector.start];
    }
    if (selector.end !== undefined) {
      selectorPrefixed.end = [...this.prefix, ...selector.end];
    }
    if (selector.prefix !== undefined) {
      selectorPrefixed.prefix = [...this.prefix, ...selector.prefix];
    }

    for await (
      const entry of this.parentDriver.list(
        selectorPrefixed,
        opts,
      )
    ) {
      yield {
        key: <Key> entry.key.slice(this.prefix.length),
        value: entry.value,
      };
    }
  }

  clear(
    opts?: { prefix?: Key; start?: Key; end?: Key },
  ): Promise<void> {
    if (opts) {
      return this.parentDriver.clear({
        prefix: opts.prefix === undefined
          ? undefined
          : [...this.prefix, ...opts.prefix],
        start: opts.start === undefined
          ? undefined
          : [...this.prefix, ...opts.start],
        end: opts.end === undefined ? undefined : [...this.prefix, ...opts.end],
      });
    }

    return this.parentDriver.clear();
  }

  batch(): KvBatch<Key, Value> {
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
