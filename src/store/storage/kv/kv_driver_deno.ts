import { KeyPart, KvBatch, KvDriver } from "./types.ts";

export class KvDriverDeno<Key extends KeyPart[], Value>
  implements KvDriver<Key, Value> {
  private kv: Deno.Kv;

  constructor(kv: Deno.Kv) {
    this.kv = kv;
  }

  close(): void {
    this.kv.close();
  }

  async get(key: Key): Promise<Value | undefined> {    
    const res = await this.kv.get<Value>(key);

    if (res.value) {
      return res.value;
    }

    return undefined;
  }

  async set(key: Key, value: Value): Promise<void> {
    await this.kv.set(key, value);
  }

  async delete(key: Key): Promise<boolean> {
    const hadIt = (await this.kv.get<Value>(key)) === undefined;
    await this.kv.delete(key);
    return Promise.resolve(hadIt);
  }

  async *list(
    selector_: { start?: Key; end?: Key; prefix?: Key },
    opts?: {
      reverse?: boolean;
      limit?: number;
      batchSize?: number;
    },
  ): AsyncIterable<{ key: Key; value: Value }> {
    const selector =
      (selector_.start !== undefined && selector_.end !== undefined)
        ? { start: selector_.start, end: selector_.end }
        : {
          prefix: (selector_.prefix === undefined)
            ? <Key> <unknown> []
            : selector_.prefix,
          start: selector_.start,
          end: selector_.end,
        };

    const iter = this.kv.list<Value>(selector, {
      reverse: opts?.reverse,
      limit: opts?.limit,
      batchSize: opts?.batchSize,
    });

    for await (const entry of iter) {
      yield { key: entry.key as Key, value: entry.value };
    }
  }

  async clear(opts?: { prefix?: Key; start?: Key; end?: Key }): Promise<void> {
    if (!opts) {
      const iter = this.kv.list<Value>({ prefix: <Key> <unknown> [] });

      for await (const entry of iter) {
        await this.delete(entry.key as Key);
      }

      return;
    } else {
      const selector =
      (opts.start !== undefined && opts.end !== undefined)
        ? { start: opts.start, end: opts.end }
        : {
          prefix: (opts.prefix === undefined)
            ? <Key> <unknown> []
            : opts.prefix,
          start: opts.start,
          end: opts.end,
        };

      const iter = this.kv.list<Value>(selector);

      for await (const entry of iter) {
        await this.delete(entry.key as Key);
      }
    }
  }

  batch(): KvBatch<Key, Value> {
    const OPS_LIMIT = 1000;

    let currentAtomicOperation = this.kv.atomic();
    let currentOps = 0;

    const fullBatches: Deno.AtomicOperation[] = [];

    const incrementAtomic = () => {
      currentOps += 1;

      if (currentOps === OPS_LIMIT) {
        fullBatches.push(currentAtomicOperation);
        currentAtomicOperation = this.kv.atomic();
        currentOps = 0;
      }
    };

    return {
      set(key: Key, value: Value) {
        currentAtomicOperation.set(key, value);
        incrementAtomic();
      },
      delete(key: Key) {
        currentAtomicOperation.delete(key);
        incrementAtomic();
      },
      async commit() {
        await Promise.all([
          fullBatches.map((batch) => batch.commit()),
          currentAtomicOperation.commit(),
        ]);
      },
    };
  }
}
