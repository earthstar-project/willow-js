import { Key, KvBatch, KvDriver } from "./types.ts";

export class KvDriverDeno implements KvDriver {
  private kv: Deno.Kv;

  constructor(kv: Deno.Kv) {
    this.kv = kv;
  }

  async get<ValueType>(key: Key): Promise<ValueType | undefined> {
    const res = await this.kv.get<ValueType>(key);

    if (res.value) {
      return res.value;
    }

    return undefined;
  }

  async set(key: Key, value: unknown): Promise<void> {
    await this.kv.set(key, value);
  }

  delete(key: Key): Promise<void> {
    return this.kv.delete(key);
  }

  async *list<ValueType>(
    selector: { start: Key; end: Key } | { prefix: Key },
    opts?: {
      reverse?: boolean;
      limit?: number;
      batchSize?: number;
    },
  ): AsyncIterable<{ key: Key; value: ValueType }> {
    const iter = this.kv.list<ValueType>(selector, {
      reverse: opts?.reverse,
      limit: opts?.limit,
      batchSize: opts?.batchSize,
    });

    for await (const entry of iter) {
      yield { key: entry.key as Key, value: entry.value };
    }
  }

  async clear<ValueType>(
    opts?: { prefix: Key; start: Key; end: Key } | undefined,
  ): Promise<void> {
    if (!opts) {
      const iter = this.kv.list<ValueType>({ prefix: [] });

      for await (const entry of iter) {
        await this.delete(entry.key as Key);
      }

      return;
    }

    const iter = this.kv.list<ValueType>(opts);

    for await (const entry of iter) {
      await this.delete(entry.key as Key);
    }
  }

  batch(): KvBatch {
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
      set(key, value) {
        currentAtomicOperation.set(key, value);
        incrementAtomic();
      },
      delete(key) {
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
