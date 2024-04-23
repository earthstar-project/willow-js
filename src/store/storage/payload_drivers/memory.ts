import { concat, encodeBase64 } from "../../../../deps.ts";
import { ValidationError } from "../../../errors.ts";
import { Payload, PayloadScheme } from "../../types.ts";
import { PayloadDriver } from "../types.ts";
import { collectUint8Arrays } from "./util.ts";

/** Store and retrieve payloads in memory. */
export class PayloadDriverMemory<PayloadDigest>
  implements PayloadDriver<PayloadDigest> {
  private payloadMap = new Map<string, Uint8Array>();

  private getKey(payloadHash: PayloadDigest) {
    const encoded = this.payloadScheme.encode(payloadHash);

    return encodeBase64(encoded);
  }

  constructor(
    readonly payloadScheme: PayloadScheme<PayloadDigest>,
  ) {
  }

  private getPayload(bytes: Uint8Array): Payload {
    return {
      bytes: (offset) => {
        if (offset === undefined || offset === 0) {
          return Promise.resolve(bytes);
        }

        return Promise.resolve(
          bytes.slice(offset),
        );
      },

      // Need to do this for Node's sake.
      stream: (offset) => {
        if (offset === undefined || offset === 0) {
          return Promise.resolve(
            new Blob([bytes.buffer]).stream() as unknown as ReadableStream<
              Uint8Array
            >,
          );
        }

        return Promise.resolve(new Blob([bytes.slice(offset).buffer])
          .stream() as unknown as ReadableStream<
            Uint8Array
          >);
      },
      length: () => Promise.resolve(BigInt(bytes.byteLength)),
    };
  }

  get(
    payloadHash: PayloadDigest,
  ): Promise<Payload | undefined> {
    const key = this.getKey(payloadHash);

    const bytes = this.payloadMap.get(key);

    if (!bytes) {
      return Promise.resolve(undefined);
    }

    return Promise.resolve(this.getPayload(bytes));
  }

  erase(payloadHash: PayloadDigest): Promise<true | ValidationError> {
    const key = this.getKey(payloadHash);

    if (this.payloadMap.has(key)) {
      this.payloadMap.delete(key);
      return Promise.resolve(true as true);
    }

    return Promise.resolve(
      new ValidationError("No payload with that digest found."),
    );
  }

  length(payloadHash: PayloadDigest): Promise<bigint> {
    const key = this.getKey(payloadHash);

    const bytes = this.payloadMap.get(key);

    if (!bytes) {
      return Promise.resolve(BigInt(0));
    }

    return Promise.resolve(BigInt(bytes.length));
  }

  async set(
    payload: Uint8Array | AsyncIterable<Uint8Array>,
  ): Promise<{ digest: PayloadDigest; payload: Payload; length: bigint }> {
    const bytes = payload instanceof Uint8Array
      ? payload
      : new Uint8Array(await collectUint8Arrays(payload));

    const digest = await this.payloadScheme.fromBytes(bytes);

    const key = this.getKey(digest);

    this.payloadMap.set(key, bytes);

    return {
      digest,
      length: BigInt(bytes.byteLength),
      payload: this.getPayload(bytes),
    };
  }

  async receive(
    opts: {
      payload: AsyncIterable<Uint8Array> | Uint8Array;
      offset: number;
      knownLength: bigint;
      knownDigest: PayloadDigest;
    },
  ): Promise<{ digest: PayloadDigest; length: bigint }> {
    const key = this.getKey(opts.knownDigest);
    const existingBytes = this.payloadMap.get(key) || new Uint8Array();

    const collectedBytes = opts.payload instanceof Uint8Array
      ? opts.payload
      : await collectUint8Arrays(opts.payload);

    const assembled = concat(
      existingBytes.slice(0, opts.offset),
      collectedBytes,
    );

    this.payloadMap.set(key, assembled);

    const digest = await this.payloadScheme.fromBytes(assembled);

    return {
      digest,
      length: BigInt(assembled.byteLength),
    };
  }
}
