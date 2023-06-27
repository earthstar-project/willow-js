import { encodeBase64 } from "../../../../deps.ts";
import { ValidationError } from "../../../errors.ts";
import { Payload, ProtocolParameters } from "../../types.ts";
import { PayloadDriver } from "../types.ts";

export class PayloadDriverMemory<KeypairType> implements PayloadDriver {
  private stagingMap = new Map<string, Blob>();
  private payloadMap = new Map<string, Blob>();

  private getKey(payloadHash: Uint8Array) {
    return encodeBase64(payloadHash);
  }

  constructor(readonly protocolParameters: ProtocolParameters<KeypairType>) {
  }

  get(
    payloadHash: Uint8Array,
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

  erase(payloadHash: Uint8Array): Promise<true | ValidationError> {
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
      hash: Uint8Array;
      length: number;
      commit: () => Promise<void>;
      reject: () => Promise<void>;
    }
  > {
    const bytes = payload instanceof Uint8Array
      ? payload
      : await streamToBytes(payload);

    const hash = await this.protocolParameters.hash(bytes);

    const newPayload = new Blob([bytes]);

    const key = this.getKey(hash);

    this.stagingMap.set(key, newPayload);

    return Promise.resolve({
      hash,
      length: newPayload.size,
      commit: () => {
        this.payloadMap.set(key, newPayload);
        this.stagingMap.delete(key);

        return Promise.resolve();
      },
      reject: () => {
        this.stagingMap.delete(key);

        return Promise.resolve();
      },
    });
  }
}

export async function streamToBytes(
  stream: ReadableStream<Uint8Array>,
): Promise<Uint8Array> {
  let bytes = new Uint8Array();

  await stream.pipeTo(
    new WritableStream({
      write(chunk) {
        const nextBytes = new Uint8Array(bytes.byteLength + chunk.byteLength);
        nextBytes.set(bytes);
        nextBytes.set(chunk, bytes.byteLength);
        bytes = nextBytes;
      },
    }),
  );

  return bytes;
}
