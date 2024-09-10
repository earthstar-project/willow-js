/**
 * Provides a summarisable storage that summarises in linear time.
 *
 * This is not very efficient. Use for testing only.
 */

import type { KvDriver, KvKey } from "../kv/types.ts";
import {
  combineMonoid,
  type LiftingMonoid,
  sizeMonoid,
} from "./lifting_monoid.ts";
import type { SummarisableStorage } from "./types.ts";

export type LinearStorageOpts<
  Key extends KvKey,
  Value,
  SummaryData,
> = {
  kv: KvDriver;
  monoid: LiftingMonoid<[Key, Value], SummaryData>;
};

export class LinearStorage<
  Key extends KvKey,
  Value,
  SummaryData,
> implements SummarisableStorage<Key, Value, SummaryData> {
  kv: KvDriver;
  monoid: LiftingMonoid<[Key, Value], [SummaryData, number]>;

  constructor(opts: LinearStorageOpts<Key, Value, SummaryData>) {
    this.kv = opts.kv;
    this.monoid = combineMonoid(opts.monoid, sizeMonoid);
  }

  async insert(
    key: Key,
    value: Value,
  ): Promise<void> {
    await this.kv.set(key, value);
  }

  async remove(key: Key): Promise<boolean> {
    const got = await this.kv.get(key);
    await this.kv.delete(key);
    return Promise.resolve(got !== undefined);
  }

  async get(key: Key): Promise<Value | undefined> {
    return await this.kv.get(key);
  }

  async summarise(
    start?: Key,
    end?: Key,
  ): Promise<{ fingerprint: SummaryData; size: number }> {
    let summary = this.monoid.neutral;

    for await (const entry of this.kv.list<Value>({ start, end })) {
      summary = this.monoid.combine(
        summary,
        await this.monoid.lift([<Key> entry.key, entry.value]),
      );
    }

    return {
      fingerprint: summary[0],
      size: summary[1],
    };
  }

  entries(
    start: Key | undefined,
    end: Key | undefined,
    opts?: {
      limit?: number;
      reverse?: boolean;
    },
  ): AsyncIterable<{ key: Key; value: Value }> {
    return <AsyncIterable<{ key: Key; value: Value }>> this.kv.list<Value>({
      start,
      end,
    }, opts);
  }

  allEntries(
    reverse?: boolean,
  ): AsyncIterable<{ key: Key; value: Value }> {
    return this.entries(undefined, undefined, { reverse });
  }
}
