import type { delay } from "https://deno.land/std@0.202.0/async/delay.ts";
import { deferred } from "../../../../deps.ts";
import { ValidationError, WillowError } from "../../../errors.ts";
import type { Payload, PayloadScheme } from "../../types.ts";
import type { PayloadDriver } from "../types.ts";
import { collectUint8Arrays } from "./util.ts";
import { concat } from "@std/bytes";

const PAYLOAD_STORE = "payload";

/** Stores and retrieves payloads from IndexedDB. */
export class PayloadDriverIndexedDb<PayloadDigest>
  implements PayloadDriver<PayloadDigest> {
  private db = deferred<IDBDatabase>();

  constructor(readonly payloadScheme: PayloadScheme<PayloadDigest>) {
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

      // Storing digest / blob kv pairs.
      db.createObjectStore(PAYLOAD_STORE);
    };

    request.onsuccess = () => {
      const db = request.result;

      this.db.resolve(db);
    };
  }

  private getKey(payloadHash: PayloadDigest) {
    return this.payloadScheme.encode(payloadHash);
  }

  private getPayload(key: Uint8Array): Payload {
    const getBytes = async () => {
      const db = await this.db;

      const payloadStore = db.transaction([PAYLOAD_STORE], "readonly")
        .objectStore(
          PAYLOAD_STORE,
        );

      const didGet = deferred<Uint8Array | undefined>();

      const getOp = payloadStore.get(key);

      getOp.onsuccess = () => {
        didGet.resolve(getOp.result);
      };

      getOp.onerror = () => {
        didGet.reject();
      };

      return didGet;
    };

    return {
      length: async () => {
        const bytes = await getBytes();

        if (!bytes) {
          throw new WillowError("Couldn't get expected payload");
        }

        return BigInt(bytes.byteLength);
      },
      bytes: async (offset) => {
        const bytes = await getBytes();

        if (!bytes) {
          throw new WillowError("Couldn't get expected payload");
        }

        return bytes.slice(offset);
      },
      stream: async (offset) => {
        const bytes = await getBytes();

        if (!bytes) {
          throw new WillowError("Couldn't get expected payload ");
        }

        const sliced = bytes.slice(offset);

        return new Blob([sliced]).stream();
      },
    };
  }

  async get(payloadHash: PayloadDigest): Promise<Payload | undefined> {
    const db = await this.db;

    const payloadStore = db.transaction([PAYLOAD_STORE], "readonly")
      .objectStore(
        PAYLOAD_STORE,
      );

    const exists = deferred<boolean>();

    const key = this.getKey(payloadHash);

    const countReq = payloadStore.count(key);

    countReq.onsuccess = () => {
      exists.resolve(countReq.result > 0);
    };

    const itExists = await exists;

    if (!itExists) {
      return undefined;
    }

    return this.getPayload(key);
  }

  async erase(digest: PayloadDigest): Promise<true | ValidationError> {
    const db = await this.db;

    const payloadStore = db.transaction([PAYLOAD_STORE], "readwrite")
      .objectStore(
        PAYLOAD_STORE,
      );

    const key = this.getKey(digest);

    const deleteOp = payloadStore.delete(key);

    const didDelete = deferred<boolean>();

    deleteOp.onsuccess = () => {
      didDelete.resolve(deleteOp.result === undefined);
    };

    const isDeleted = await didDelete;

    if (!isDeleted) {
      return new ValidationError("No payload with that digest found.");
    }

    return isDeleted;
  }

  async length(payloadHash: PayloadDigest): Promise<bigint> {
    const db = await this.db;

    const payloadStore = db.transaction([PAYLOAD_STORE], "readonly")
      .objectStore(
        PAYLOAD_STORE,
      );

    const exists = deferred<boolean>();

    const key = this.getKey(payloadHash);

    const countReq = payloadStore.count(key);

    countReq.onsuccess = () => {
      exists.resolve(countReq.result > 0);
    };

    const itExists = await exists;

    if (!itExists) {
      return 0n;
    }

    const payload = this.getPayload(key);

    return payload.length();
  }

  async set(
    payload: Uint8Array | AsyncIterable<Uint8Array>,
  ): Promise<{ digest: PayloadDigest; length: bigint; payload: Payload }> {
    const bytes = payload instanceof Uint8Array
      ? payload
      : await collectUint8Arrays(payload);

    const digest = await this.payloadScheme.fromBytes(bytes);

    const key = this.getKey(digest);

    const db = await this.db;

    const payloadStore = db.transaction([PAYLOAD_STORE], "readwrite")
      .objectStore(
        PAYLOAD_STORE,
      );

    const didSet = deferred<
      { digest: PayloadDigest; length: bigint; payload: Payload }
    >();

    const request = payloadStore.put(bytes, key);

    request.onsuccess = () => {
      didSet.resolve({
        digest,
        length: BigInt(bytes.byteLength),
        payload: this.getPayload(key),
      });
    };

    return didSet;
  }

  async receive(
    opts: {
      payload: Uint8Array | AsyncIterable<Uint8Array>;
      offset: number;
      knownLength: bigint;
      knownDigest: PayloadDigest;
    },
  ): Promise<{ digest: PayloadDigest; length: bigint }> {
    const db = await this.db;

    const key = this.getKey(opts.knownDigest);

    let existingBytes = new Uint8Array();

    if (opts.offset > 0) {
      // Get existing blob.
      const didGet = deferred<Uint8Array | undefined>();

      const payloadStore = db.transaction([PAYLOAD_STORE], "readwrite")
        .objectStore(
          PAYLOAD_STORE,
        );

      const getOp = payloadStore.get(key);

      getOp.onsuccess = () => {
        didGet.resolve(getOp.result);
      };

      getOp.onerror = () => {
        didGet.reject();
      };

      const justGotBytes = await didGet;

      if (justGotBytes) {
        existingBytes = justGotBytes.slice(0, opts.offset);
      }
    }

    const receivedBytes = opts.payload instanceof Uint8Array
      ? opts.payload
      : await collectUint8Arrays(opts.payload);

    const finalBytes = concat([existingBytes, receivedBytes]);

    const digest = await this.payloadScheme.fromBytes(
      new Uint8Array(finalBytes),
    );

    const payloadStore = db.transaction([PAYLOAD_STORE], "readwrite")
      .objectStore(
        PAYLOAD_STORE,
      );

    const didSet = deferred<
      { digest: PayloadDigest; length: bigint }
    >();

    const request = payloadStore.put(finalBytes, key);

    request.onsuccess = () => {
      didSet.resolve({
        digest,
        length: BigInt(finalBytes.byteLength),
      });
    };

    return didSet;
  }
}
