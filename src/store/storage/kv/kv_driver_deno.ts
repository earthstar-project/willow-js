import { compareKeys, isFirstKeyPrefixOfSecondKey } from "./types.ts";
import { KvBatch, KvDriver, KvKey } from "./types.ts";

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

    if (res.value !== null) {
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
    selector: { start?: KvKey; end?: KvKey; prefix?: KvKey },
    opts?: {
      reverse?: boolean;
      limit?: number;
      batchSize?: number;
    },
  ): AsyncIterable<{ key: KvKey; value: Value }> {
    // Couple gotchas: deno kv uses a strict prefix relation, but our driver interfaces assumes a non-strict prefix relation
    // Deno throws if start or end are not prefixed by prefix.

    const reverse = opts ? opts.reverse : undefined;
    let limit = opts ? opts.limit : undefined;
    const batchSize = opts ? opts.batchSize : undefined;

    // Turns out it simplifies our code if we can assume that limit is not zero from now on.
    if (limit !== undefined && limit === 0) {
      return;
    }

    let prefix: KvKey | undefined = selector.prefix === undefined
      ? []
      : selector.prefix;
    let start = selector.start;
    let end = selector.end;

    if (selector.prefix !== undefined) {
      // Deno errors if start or end are are not strictly prefixed by prefix, so handle those cases explicitly.

      if (
        selector.start &&
        !(isFirstKeyPrefixOfSecondKey(selector.prefix, selector.start) &&
          (compareKeys(selector.prefix, selector.start) !== 0))
      ) {
        if (compareKeys(selector.start, selector.prefix) <= 0) {
          // start <= prefix, so might as well have no start value at all.
          start = undefined;
        } else {
          // start > prefix but not prefixed by it, so no entries can match.
          return;
        }
      }

      if (
        selector.end &&
        !(isFirstKeyPrefixOfSecondKey(selector.prefix, selector.end) &&
          (compareKeys(selector.prefix, selector.end) !== 0))
      ) {
        if (compareKeys(selector.end, selector.prefix) > 0) {
          // end > prefix, so might as well have no end value at all.
          end = undefined;
        } else {
          // end < prefix but not prefixed by it, so no entries can match.
          return;
        }
      }
    }

    // If we contain an entry keyed by the prefix itself, we need to work around Deno using strict prefixes in its list function.
    const directMatch = await this.kv.get<Value>(prefix);

    if (directMatch.value !== null && !reverse) {
      // Not reversing, so the directMatch is the first item to emit.
      // We handle the case if `reverse` later, because emitting the directMatch is the last thing to do in this function in that case.

      yield {
        key: <KvKey> [...directMatch.key],
        value: directMatch.value,
      };

      // We emitted one value, so if there is a limit, decrease it by one.
      if (limit !== undefined) {
        limit -= 1;

        // If our limit was 1 to start with, return.
        if (limit === 0) {
          return;
        }
      }
    }

    // Deno complains if we specify all three of prefix, start, and end. If both start and end are not undefined at this point, then we can omit the prefix from our inner list query without changing the results (because both start and end are prefixed by prefix in that case).
    if (start !== undefined && end !== undefined) {
      prefix = undefined;
    }

    const iter = this.kv.list<Value>(
      <Deno.KvListSelector> {
        prefix,
        start,
        end,
      },
      {
        reverse,
        limit,
        batchSize,
      },
    );

    for await (const entry of iter) {
      yield { key: entry.key as KvKey, value: entry.value };
      if (limit !== undefined) {
        limit -= 1;
      }
    }

    if (
      directMatch.value !== null && reverse &&
      (limit === undefined || limit > 0)
    ) {
      yield {
        key: <KvKey> [...directMatch.key],
        value: directMatch.value,
      };
    }
  }

  async clear(
    opts: { prefix?: KvKey; start?: KvKey; end?: KvKey } = {},
  ): Promise<void> {
    for await (const entry of this.list(opts)) {
      await this.delete(entry.key as KvKey);
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
