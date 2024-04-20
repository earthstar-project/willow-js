import { compareKeys } from "./types.ts";
import { KvKey, KvBatch, KvDriver } from "./types.ts";

export class KvDriverDeno implements KvDriver {
  private kv: Deno.Kv;

  constructor(kv: Deno.Kv) {
    this.kv = kv;
  }

  close(): void {
    this.kv.close();
  }

  async get<Value>(key: KvKey): Promise<Value | undefined> {
    const res = await this.kv.get<Value>(key);

    if (res.value) {
      return res.value;
    }

    return undefined;
  }

  async set<Value>(key: KvKey, value: Value): Promise<void> {
    await this.kv.set(key, value);
  }

  async delete(key: KvKey): Promise<boolean> {
    const hadIt = (await this.kv.get<unknown>(key)) === undefined;
    await this.kv.delete(key);
    return Promise.resolve(hadIt);
  }

  async *list<Value>(
    selector_: { start?: KvKey; end?: KvKey; prefix?: KvKey },
    opts?: {
      reverse?: boolean;
      limit?: number;
      batchSize?: number;
    },
  ): AsyncIterable<{ key: KvKey; value: Value }> {
    // This function would be simple and elegant if Deno didn't treat prefixes as irreflexive =(
    // To be honest, there are much cleaner ways for adding the reflexive case, I just got frustrated...

    let limit: undefined | number = opts === undefined ? undefined : opts.limit;

    const selector =
      (selector_.start !== undefined && selector_.end !== undefined)
        ? { start: selector_.start, end: selector_.end }
        : {
          prefix: (selector_.prefix === undefined)
            ? <KvKey> <unknown> []
            : selector_.prefix,
          start: selector_.start,
          end: selector_.end,
        };

    if (opts === undefined || !opts.reverse) {
      if (selector_.prefix) {
        if (
          (selector_.start === undefined ||
            compareKeys(selector_.start, selector_.prefix) <= 0) &&
          (selector_.end === undefined ||
            compareKeys(selector_.end, selector_.prefix) > 0)
        ) {
          const directMatch = await this.kv.get<Value>(selector_.prefix);
          if (directMatch.value !== null) {
            yield { key: <KvKey>[...directMatch.key], value: directMatch.value };
            if (limit !== undefined) {
              limit -= 1;
            }
          }
        }
      }
    }

    if (limit === undefined || limit > 0) {
      const iter = this.kv.list<Value>(selector, {
        reverse: opts?.reverse,
        limit: limit,
        batchSize: opts?.batchSize,
      });

      for await (const entry of iter) {
        yield { key: entry.key as KvKey, value: entry.value };
        if (limit !== undefined) {
          limit -= 1;
        }
      }
    }

    if (
      opts !== undefined && opts.reverse && (limit === undefined || limit > 0)
    ) {
      if (selector_.prefix) {
        if (
          (selector_.start === undefined ||
            compareKeys(selector_.start, selector_.prefix) <= 0) &&
          (selector_.end === undefined ||
            compareKeys(selector_.end, selector_.prefix) > 0)
        ) {
          const directMatch = await this.kv.get<Value>(selector_.prefix);
          if (directMatch.value !== null) {
            yield { key: <KvKey>[...directMatch.key], value: directMatch.value };
            if (limit !== undefined) {
              limit -= 1;
            }
          }
        }
      }
    }
  }

  async clear(opts?: { prefix?: KvKey; start?: KvKey; end?: KvKey }): Promise<void> {
    if (!opts) {
      const iter = this.kv.list<unknown>({ prefix: <KvKey> <unknown> [] });

      for await (const entry of iter) {
        await this.delete(entry.key as KvKey);
      }

      return;
    } else {
      const selector = (opts.start !== undefined && opts.end !== undefined)
        ? { start: opts.start, end: opts.end }
        : {
          prefix: (opts.prefix === undefined)
            ? <KvKey> <unknown> []
            : opts.prefix,
          start: opts.start,
          end: opts.end,
        };

      const iter = this.kv.list<unknown>(selector);

      for await (const entry of iter) {
        await this.delete(entry.key as KvKey);
      }
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
      set<Value>(key: KvKey, value: Value) {
        currentAtomicOperation.set(key, value);
        incrementAtomic();
      },
      delete(key: KvKey) {
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
