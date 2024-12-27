import { ValidationError, WillowError } from "../../../errors.ts";
import type { Payload, PayloadScheme } from "../../types.ts";
import type { PayloadDriver } from "../types.ts";
import { collectUint8Arrays } from "./util.ts";
import { concat } from "@std/bytes";

const PAYLOAD_STORE = "payload";
const PARTIAL_STORE = "partial";

/** Implements {@linkcode PayloadDriver} on top of [IndexedDB](https://developer.mozilla.org/en-US/docs/Web/API/IndexedDB_API). */
export class PayloadDriverIndexedDb<PayloadDigest>
  implements PayloadDriver<PayloadDigest> {
  private db = Promise.withResolvers<IDBDatabase>();

  constructor(
    id: string,
    readonly payloadScheme: PayloadScheme<PayloadDigest>,
  ) {
    const request = ((globalThis as any).indexedDB as IDBFactory).open(
      `willow_payloads_${id}`,
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

      // Storingi digest / partial pairs
      db.createObjectStore(PARTIAL_STORE);
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
      const db = await this.db.promise;

      const payloadStore = db.transaction([PAYLOAD_STORE], "readonly")
        .objectStore(
          PAYLOAD_STORE,
        );

      const didGet = Promise.withResolvers<Uint8Array | undefined>();

      const getOp = payloadStore.get(key);

      getOp.onsuccess = () => {
        didGet.resolve(getOp.result);
      };

      getOp.onerror = () => {
        didGet.reject();
      };

      return didGet.promise;
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
    const db = await this.db.promise;

    const payloadStore = db.transaction([PAYLOAD_STORE], "readonly")
      .objectStore(
        PAYLOAD_STORE,
      );

    const exists = Promise.withResolvers<boolean>();

    const key = this.getKey(payloadHash);

    const countReq = payloadStore.count(key);

    countReq.onsuccess = () => {
      exists.resolve(countReq.result > 0);
    };

    const itExists = await exists.promise;

    if (!itExists) {
      return undefined;
    }

    return this.getPayload(key);
  }

  async erase(digest: PayloadDigest): Promise<true | ValidationError> {
    const db = await this.db.promise;

    const payloadStore = db.transaction([PAYLOAD_STORE], "readwrite")
      .objectStore(
        PAYLOAD_STORE,
      );

    const key = this.getKey(digest);

    const deleteOp = payloadStore.delete(key);

    const didDelete = Promise.withResolvers<boolean>();

    deleteOp.onsuccess = () => {
      didDelete.resolve(deleteOp.result === undefined);
    };

    const isDeleted = await didDelete.promise;

    if (!isDeleted) {
      return new ValidationError("No payload with that digest found.");
    }

    return isDeleted;
  }

  async length(payloadHash: PayloadDigest): Promise<bigint> {
    const db = await this.db.promise;

    const payloadStore = db.transaction([PAYLOAD_STORE], "readonly")
      .objectStore(
        PAYLOAD_STORE,
      );

    const exists = Promise.withResolvers<boolean>();

    const key = this.getKey(payloadHash);

    const countReq = payloadStore.count(key);

    countReq.onsuccess = () => {
      exists.resolve(countReq.result > 0);
    };

    const itExists = await exists.promise;

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

    const db = await this.db.promise;

    const payloadStore = db.transaction([PAYLOAD_STORE], "readwrite")
      .objectStore(
        PAYLOAD_STORE,
      );

    const didSet = Promise.withResolvers<
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

    return didSet.promise;
  }

  async receive(
    opts: {
      payload: AsyncIterable<Uint8Array> | Uint8Array;
      offset: number;
      expectedLength: bigint;
      expectedDigest: PayloadDigest;
    },
  ): Promise<
    {
      digest: PayloadDigest;
      length: bigint;
      commit: (isCompletePayload: boolean) => Promise<void>;
      reject: () => Promise<void>;
    }
  > {
    const db = await this.db.promise;

    const key = this.getKey(opts.expectedDigest);

    let existingBytes = new Uint8Array();

    if (opts.offset > 0) {
      // Get existing blob.
      const didGet = Promise.withResolvers<Uint8Array | undefined>();

      const payloadStore = db.transaction([PARTIAL_STORE], "readwrite")
        .objectStore(
          PARTIAL_STORE,
        );

      const getOp = payloadStore.get(key);

      getOp.onsuccess = () => {
        didGet.resolve(getOp.result);
      };

      getOp.onerror = () => {
        didGet.reject();
      };

      const justGotBytes = await didGet.promise;

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

    return {
      digest,
      length: BigInt(finalBytes.byteLength),
      commit: (isCompletePayload) => {
        if (isCompletePayload) {
          const payloadStore = db.transaction([PAYLOAD_STORE], "readwrite")
            .objectStore(
              PAYLOAD_STORE,
            );

          const request = payloadStore.put(finalBytes, key);

          const didSet = Promise.withResolvers<void>();

          request.onsuccess = () => {
            didSet.resolve();
          };

          return didSet.promise;
        }

        const payloadStore = db.transaction([PARTIAL_STORE], "readwrite")
          .objectStore(
            PARTIAL_STORE,
          );

        const request = payloadStore.put(finalBytes, key);

        const didSet = Promise.withResolvers<void>();

        request.onsuccess = () => {
          didSet.resolve();
        };

        return didSet.promise;
      },
      reject: () => Promise.resolve(),
    };
  }
}
