import {
  compareKeys,
  isFirstKeyPrefixOfSecondKey,
  type KvBatch,
  type KvDriver,
  type KvKey,
} from "./types.ts";
import { pack, unpack } from "./key_codec/kv_key_codec.ts";
import { WillowError } from "../../../errors.ts";
import { FIFO } from "@korkje/fifo";
import { successorBytesFixedWidth } from "@earthstar/willow-utils";
import { equals as equalsBytes } from "@std/bytes";

const KV_STORE = "kv";
const END_LIST = Symbol("end_list");

/** Implements {@linkcode KvDriver} on top of [IndexedDB](https://developer.mozilla.org/en-US/docs/Web/API/IndexedDB_API). */
export class KvDriverIndexedDB implements KvDriver {
  private db = Promise.withResolvers<IDBDatabase>();

  constructor(id: string) {
    const request = ((globalThis as any).indexedDB as IDBFactory).open(
      `willow_${id}`,
      1,
    );

    request.onerror = () => {
      throw new WillowError(
        `Could not open IndexedDB.`,
      );
    };

    request.onupgradeneeded = () => {
      const db = request.result;

      // Storing KV pairs.
      db.createObjectStore(KV_STORE);
    };

    request.onsuccess = () => {
      const db = request.result;

      this.db.resolve(db);
    };
  }

  async get<Value>(key: KvKey): Promise<Value | undefined> {
    const db = await this.db.promise;

    const kvStore = db.transaction([KV_STORE], "readonly").objectStore(
      KV_STORE,
    );

    const didGet = Promise.withResolvers<Value | undefined>();

    const packed = pack(key);

    const getOp = kvStore.get(packed.buffer);

    getOp.onsuccess = () => {
      didGet.resolve(getOp.result);
    };

    getOp.onerror = () => {
      didGet.reject();
    };

    return didGet.promise;
  }

  async set<Value>(key: KvKey, value: Value): Promise<void> {
    const db = await this.db.promise;

    const kvStore = db.transaction([KV_STORE], "readwrite").objectStore(
      KV_STORE,
    );

    const packed = pack(key);

    const request = kvStore.put(value, packed.buffer);

    const didSet = Promise.withResolvers<void>();

    request.onsuccess = () => {
      didSet.resolve();
    };

    return didSet.promise;
  }

  async delete(key: KvKey): Promise<boolean> {
    const db = await this.db.promise;

    const kvStore = db.transaction([KV_STORE], "readwrite").objectStore(
      KV_STORE,
    );

    const packed = pack(key);

    const deleteOp = kvStore.delete(packed.buffer);

    const didDelete = Promise.withResolvers<boolean>();

    deleteOp.onsuccess = () => {
      didDelete.resolve(deleteOp.result === undefined);
    };

    return didDelete.promise;
  }

  async *list<Value>(
    selector: {
      start?: KvKey | undefined;
      end?: KvKey | undefined;
      prefix?: KvKey | undefined;
    },
    opts: {
      reverse?: boolean | undefined;
      limit?: number | undefined;
      batchSize?: number | undefined;
    } | undefined = {},
  ): AsyncIterable<{ key: KvKey; value: Value }> {
    const db = await this.db.promise;

    const kvStore = db.transaction([KV_STORE], "readwrite").objectStore(
      KV_STORE,
    );

    const openCursorParam = selectorToIdbBound(selector);
    if (openCursorParam === "notMatchingAnything") {
      return;
    }

    const direction = opts?.reverse ? "prev" : "next";

    const resultFifo = new FIFO<
      { packedKey: Uint8Array; value: Value } | typeof END_LIST
    >();

    const cursor = kvStore.openCursor(openCursorParam, direction);

    let count = 0;

    cursor.onsuccess = () => {
      if (cursor.result) {
        if ((opts.limit !== undefined) && count >= opts.limit) {
          resultFifo.push(END_LIST);
        } else if (cursor.result.key instanceof ArrayBuffer) {
          resultFifo.push({
            packedKey: new Uint8Array(cursor.result.key),
            value: cursor.result.value,
          });

          count += 1;

          cursor.result.continue();
        } else {
          console.warn(
            "A malformed key-value pair was found in the IndexedDB: ",
            cursor.result,
          );
        }
      } else {
        resultFifo.push(END_LIST);
      }
    };

    for await (const res of resultFifo) {
      if (res === END_LIST) {
        break;
      }

      yield {
        key: unpack(res.packedKey) as KvKey,
        value: res.value,
      };
    }
  }

  async clear(
    opts?: {
      prefix?: KvKey | undefined;
      start?: KvKey | undefined;
      end?: KvKey | undefined;
    } | undefined,
  ): Promise<void> {
    const db = await this.db.promise;

    const kvStore = db.transaction([KV_STORE], "readwrite").objectStore(
      KV_STORE,
    );

    const openCursorParam = opts ? selectorToIdbBound(opts) : undefined;

    if (openCursorParam === "notMatchingAnything") {
      return Promise.resolve();
    }

    const cleared = Promise.withResolvers<void>();

    const cursor = kvStore.openCursor(openCursorParam);

    cursor.onsuccess = () => {
      if (cursor.result) {
        cursor.result.delete();
        cursor.result.continue();
      } else {
        cleared.resolve();
      }
    };

    cursor.onerror = () => {
      cleared.reject();
    };

    return cleared.promise;
  }

  batch(): KvBatch {
    return new BatchIndexedDB(this.db.promise);
  }
}

type BatchOp = {
  kind: "set";
  key: KvKey;
  value: unknown;
} | { kind: "delete"; key: KvKey };

class BatchIndexedDB implements KvBatch {
  private db: Promise<IDBDatabase>;

  private ops: BatchOp[] = [];

  constructor(dbPromise: Promise<IDBDatabase>) {
    this.db = dbPromise;
  }

  set<Value>(key: KvKey, value: Value): void {
    this.ops.push({ kind: "set", key, value });
  }

  delete(key: KvKey): void {
    this.ops.push({ kind: "delete", key });
  }

  async commit() {
    const db = await this.db;

    const transaction = db.transaction([KV_STORE], "readwrite");
    const store = transaction.objectStore(KV_STORE);

    for (const op of this.ops) {
      switch (op.kind) {
        case "delete": {
          const packed = pack(op.key);
          store.delete(packed);
          break;
        }
        case "set": {
          const packed = pack(op.key);
          store.put(op.value, packed);
          break;
        }
      }
    }

    this.ops = [];
  }
}

function selectorToIdbBound({ start, end, prefix }: {
  start?: KvKey;
  end?: KvKey;
  prefix?: KvKey;
}): IDBKeyRange | undefined | "notMatchingAnything" {
  function isPrefixRelevant(prefix?: KvKey): boolean {
    return prefix !== undefined && prefix.length !== 0;
  }

  if (
    !isPrefixRelevant(prefix) &&
    start === undefined && end === undefined
  ) {
    // No constraints at all.
    return undefined;
  }

  if (isPrefixRelevant(prefix)) {
    // We have a prefix. That complicates things a bit, because IndexDB has no built-in support for working with prefixes.

    // If start is strictly less than the prefix, then use the prefix instead of start. Else, if start is prefixed by the prefix, we can ignore the prefix (for start-purposes, it might still determine the end). Else (start greater than prefix and not prefixed by it), the range will not match anything.
    let actualStart = prefix;
    if (start !== undefined) {
      if (isFirstKeyPrefixOfSecondKey(prefix!, start)) {
        actualStart = start;
      } else if (compareKeys(start, prefix!) > 0) {
        return "notMatchingAnything";
      }
    }
    const actualPackedStart = actualStart === undefined
      ? undefined
      : pack(actualStart);

    // For the end, we might need to compute an upper bound from the prefix.
    // (To simplify the code, we just always compute it, but we might end up not using it).
    const packedPrefix = pack(prefix!);
    const exclusiveEndFromPrefix = successorBytesFixedWidth(packedPrefix);

    // Similar reasoning applies to end.
    let actualPackedEnd = exclusiveEndFromPrefix === null
      ? undefined
      : exclusiveEndFromPrefix;
    if (end !== undefined) {
      if (isFirstKeyPrefixOfSecondKey(prefix!, end)) {
        // Can ignore prefix, just use end instead.
        actualPackedEnd = pack(end);
      } else if (compareKeys(end, prefix!) < 0) {
        return "notMatchingAnything";
        // And else, if end is greater than prefix and but prefix is not a prefix o end, than we use exclusiveEndFromPrefix.
      }
    }

    if (
      actualPackedStart && actualPackedEnd &&
      equalsBytes(actualPackedStart, actualPackedEnd)
    ) {
      return "notMatchingAnything";
    }

    return IDBKeyRange.bound(
      actualPackedStart,
      actualPackedEnd,
      false,
      true,
    );
  } else {
    // The simple cases: no prefix to consider.
    if (
      start && end === undefined
    ) {
      const lowerKey = pack(start);
      return IDBKeyRange.lowerBound(lowerKey);
    } else if (
      start === undefined && end
    ) {
      const upperKey = pack(end);
      return IDBKeyRange.upperBound(upperKey, true);
    } else if (
      start && end
    ) {
      const lowerKey = pack(start);
      const upperKey = pack(end);

      return IDBKeyRange.bound(lowerKey, upperKey, false, true);
    }
  }
}
