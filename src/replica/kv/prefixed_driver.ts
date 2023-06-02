import { Key, KvDriver } from "./types.ts";

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

  list<ValueType>(
    range: { start: Key; end: Key },
    opts?: {
      prefix?: Key;
      reverse?: boolean;
      limit?: number;
      batchSize?: number;
    },
  ): AsyncIterable<{ key: Key; value: ValueType }> {
    if (opts) {
      return this.parentDriver.list(
        range,
        {
          prefix: opts.prefix ? [...this.prefix, ...opts.prefix] : undefined,
          ...opts,
        },
      );
    }

    return this.parentDriver.list(
      range,
    );
  }

  clear(opts?: { start: Key; end: Key } | undefined): Promise<void> {
    if (opts) {
      return this.parentDriver.clear({
        start: [...this.prefix, ...opts.start],
        end: [...this.prefix, ...opts.start],
      });
    }

    return this.parentDriver.clear();
  }
}
