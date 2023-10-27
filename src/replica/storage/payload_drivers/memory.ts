import { encodeBase64, toArrayBuffer } from "../../../../deps.ts";
import { ValidationError } from "../../../errors.ts";
import { EncodingScheme, Payload } from "../../types.ts";
import { PayloadDriver } from "../types.ts";

/** Store and retrieve payloads in memory. */
export class PayloadDriverMemory<PayloadDigest>
  implements PayloadDriver<PayloadDigest> {
  private stagingMap = new Map<string, Blob>();
  private payloadMap = new Map<string, Blob>();

  private getKey(payloadHash: PayloadDigest) {
    const encoded = this.payloadScheme.encode(payloadHash);

    return encodeBase64(encoded);
  }

  constructor(
    readonly payloadScheme: EncodingScheme<PayloadDigest> & {
      fromBytes: (bytes: Uint8Array | ReadableStream) => Promise<PayloadDigest>;
      order: (a: PayloadDigest, b: PayloadDigest) => -1 | 0 | 1;
    },
  ) {
  }

  get(
    payloadHash: PayloadDigest,
    opts?: { startOffset?: number | undefined } | undefined,
  ): Promise<Payload | undefined> {
    const key = this.getKey(payloadHash);

    const payloadBlob = this.payloadMap.get(key);

    if (!payloadBlob) {
      return Promise.resolve(undefined);
    }

    if (opts?.startOffset) {
      return Promise.resolve({
        bytes: async () =>
          new Uint8Array(
            await payloadBlob.slice(opts.startOffset).arrayBuffer(),
          ),
        stream:
          // Need to do this for Node's sake.
          payloadBlob.stream() as unknown as ReadableStream<Uint8Array>,
      });
    }

    return Promise.resolve({
      bytes: async () => new Uint8Array(await payloadBlob.arrayBuffer()),
      stream:
        // Need to do this for Node's sake.
        payloadBlob.stream() as unknown as ReadableStream<Uint8Array>,
    });
  }

  erase(payloadHash: PayloadDigest): Promise<true | ValidationError> {
    const key = this.getKey(payloadHash);
    if (this.payloadMap.has(key)) {
      this.payloadMap.delete(key);
      return Promise.resolve(true as true);
    }

    return Promise.resolve(
      new ValidationError("No attachment with that signature found."),
    );
  }

  async stage(
    payload: Uint8Array | ReadableStream<Uint8Array>,
  ): Promise<
    {
      hash: PayloadDigest;
      length: number;
      commit: () => Promise<Payload>;
      reject: () => Promise<void>;
    }
  > {
    const bytes = payload instanceof Uint8Array
      ? payload
      : new Uint8Array(await toArrayBuffer(payload));

    const hash = await this.payloadScheme.fromBytes(bytes);

    const newPayload = new Blob([bytes]);

    const key = this.getKey(hash);

    this.stagingMap.set(key, newPayload);

    return Promise.resolve({
      hash,
      length: newPayload.size,
      commit: () => {
        this.payloadMap.set(key, newPayload);
        this.stagingMap.delete(key);

        return Promise.resolve({
          bytes: async () => new Uint8Array(await newPayload.arrayBuffer()),
          stream: // Need to do this for Node's sake.
            newPayload.stream() as unknown as ReadableStream<Uint8Array>,
        });
      },
      reject: () => {
        this.stagingMap.delete(key);

        return Promise.resolve();
      },
    });
  }
}
