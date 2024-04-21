import { KvBatch, KvDriver, KvKey } from "./types.ts";
import { pack, unpack } from "./key_codec/kv_key_codec.ts";

import { WillowError } from "../../../errors.ts";
import { deferred, FIFO, successorBytesFixedWidth } from "../../../../deps.ts";

const KV_STORE = "kv";
const END_LIST = Symbol("end_list");

export class KvDriverIndexedDB implements KvDriver {
  private db = deferred<IDBDatabase>();

  constructor() {
    const request = ((window as any).indexedDB as IDBFactory).open(
      `willow`,
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
    const db = await this.db;

    const kvStore = db.transaction([KV_STORE], "readonly").objectStore(
      KV_STORE,
    );

    const didGet = deferred<Value | undefined>();

    const packed = pack(key);

    const getOp = kvStore.get(packed.buffer);

    getOp.onsuccess = () => {
      didGet.resolve(getOp.result);
    };

    getOp.onerror = () => {
      didGet.reject();
    };

    return didGet;
  }

  async set<Value>(key: KvKey, value: Value): Promise<void> {
    const db = await this.db;

    const kvStore = db.transaction([KV_STORE], "readwrite").objectStore(
      KV_STORE,
    );

    const packed = pack(key);

    const request = kvStore.put(value, packed.buffer);

    const didSet = deferred<void>();

    request.onsuccess = () => {
      didSet.resolve();
    };

    return didSet;
  }

  async delete(key: KvKey): Promise<boolean> {
    const db = await this.db;

    const kvStore = db.transaction([KV_STORE], "readwrite").objectStore(
      KV_STORE,
    );

    const packed = pack(key);

    const deleteOp = kvStore.delete(packed.buffer);

    const didDelete = deferred<boolean>();

    deleteOp.onsuccess = () => {
      didDelete.resolve(deleteOp.result === undefined);
    };

    return didDelete;
  }

  async *list<Value>(
    selector: {
      start?: KvKey | undefined;
      end?: KvKey | undefined;
      prefix?: KvKey | undefined;
    },
    opts?: {
      reverse?: boolean | undefined;
      limit?: number | undefined;
      batchSize?: number | undefined;
    } | undefined,
  ): AsyncIterable<{ key: KvKey; value: Value }> {
    const db = await this.db;

    const kvStore = db.transaction([KV_STORE], "readwrite").objectStore(
      KV_STORE,
    );

    const openCursorParam = selectorToIdbBound(selector);

    const direction = opts?.reverse ? "prevunique" : "nextunique";

    const resultFifo = new FIFO<
      { packedKey: Uint8Array; value: Value } | typeof END_LIST
    >();

    const cursor = kvStore.openCursor(openCursorParam, direction);

    let count = 0;

    cursor.onsuccess = () => {
      if (cursor.result) {
        if (opts?.limit && count >= opts.limit) {
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
    const db = await this.db;

    const kvStore = db.transaction([KV_STORE], "readwrite").objectStore(
      KV_STORE,
    );

    const openCursorParam = opts ? selectorToIdbBound(opts) : undefined;

    const cleared = deferred<void>();

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

    return cleared;
  }

  batch(): KvBatch {
    return new BatchIndexedDB(this.db);
  }
}

type BatchOp = {
  kind: "set";
  key: KvKey;
  value: unknown;
} | { kind: "delete"; key: KvKey };

class BatchIndexedDB<Value> implements KvBatch {
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
}): typeof IDBKeyRange | undefined {
  if (
    prefix && prefix.length === 0 && start === undefined && end === undefined
  ) {
    return undefined;
  } else if (
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
  } else if (start === undefined && end === undefined && prefix) {
    const lowerKey = pack(prefix);
    const upperKey = successorBytesFixedWidth(lowerKey);

    return IDBKeyRange.bound(lowerKey, upperKey, false, true);
  }

  return undefined;
}
