import { concat, encodeBase64 } from "../../../../deps.ts";
import { ValidationError } from "../../../errors.ts";
import { Payload, PayloadScheme } from "../../types.ts";
import { PayloadDriver } from "../types.ts";

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
        if (!offset) {
          return Promise.resolve(bytes);
        }

        return Promise.resolve(
          new Uint8Array(
            bytes.slice(offset),
          ),
        );
      },

      // Need to do this for Node's sake.
      stream: (offset) => {
        if (!offset) {
          return Promise.resolve(
            new Blob([bytes.buffer]).stream() as unknown as ReadableStream<
              Uint8Array
            >,
          );
        }

        return Promise.resolve(new Blob([bytes.subarray(offset).buffer])
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
      new ValidationError("No attachment with that signature found."),
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
      payload: AsyncIterable<Uint8Array>;
      offset: number;
      knownLength: bigint;
      knownDigest: PayloadDigest;
    },
  ): Promise<{ digest: PayloadDigest; length: bigint }> {
    const key = this.getKey(opts.knownDigest);
    const existingBytes = this.payloadMap.get(key) || new Uint8Array();

    const collectedBytes: Uint8Array[] = [];

    for await (const chunk of opts.payload) {
      collectedBytes.push(chunk);
    }

    const assembled = concat(
      existingBytes.slice(0, opts.offset),
      ...collectedBytes,
    );

    this.payloadMap.set(key, assembled);

    const digest = await this.payloadScheme.fromBytes(assembled);

    return {
      digest,
      length: BigInt(assembled.byteLength),
    };
  }
}

export async function collectUint8Arrays(
  it: AsyncIterable<Uint8Array>,
): Promise<Uint8Array> {
  const chunks = [];
  let length = 0;
  for await (const chunk of it) {
    chunks.push(chunk);
    length += chunk.length;
  }
  if (chunks.length === 1) {
    // No need to copy.
    return chunks[0];
  }
  const collected = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    collected.set(chunk, offset);
    offset += chunk.length;
  }
  return collected;
}
