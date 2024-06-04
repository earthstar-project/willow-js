import {
  decodeStreamEntryRelativeEntry,
  defaultEntry,
  GrowingBytes,
  type PathScheme,
  type StreamDecoder,
} from "@earthstar/willow-utils";
import type { Store } from "../store/store.ts";
import type { GetStoreFn } from "../wgps/wgps_messenger.ts";
import { ValidationError } from "../errors.ts";
import FIFO from "@korkje/fifo";
import type {
  NamespaceScheme,
  PayloadScheme,
  SubspaceScheme,
} from "../store/types.ts";

/** Required options for ingesting an encrypted [drop](https://willowprotocol.org/specs/sideloading/index.html#drop). */
export type IngestDropOpts<
  PreFingerprint,
  Fingerprint,
  AuthorisationToken,
  AuthorisationOpts,
  NamespaceId,
  SubspaceId,
  PayloadDigest,
> = {
  getStore: GetStoreFn<
    PreFingerprint,
    Fingerprint,
    AuthorisationToken,
    AuthorisationOpts,
    NamespaceId,
    SubspaceId,
    PayloadDigest
  >;
  dropStream: ReadableStream<Uint8Array>;
  schemes: {
    namespace: NamespaceScheme<NamespaceId>;
    subspace: SubspaceScheme<SubspaceId>;
    payload: PayloadScheme<PayloadDigest>;
    path: PathScheme;
  };
  decryptTransform: TransformStream<Uint8Array, Uint8Array>;
  decodeStreamAuthorisationToken: StreamDecoder<AuthorisationToken>;
};

/** Decrypt an encrypted [drop](https://willowprotocol.org/specs/sideloading/index.html#drop) and ingest its contents into a {@linkcode Store} of the corresponding namespace.
 *
 * @returns The {@linkcode Store} which ingested the drop's contents.
 */
export async function ingestDrop<
  PreFingerprint,
  Fingerprint,
  AuthorisationToken,
  AuthorisationOpts,
  NamespaceId,
  SubspaceId,
  PayloadDigest,
>(
  opts: IngestDropOpts<
    PreFingerprint,
    Fingerprint,
    AuthorisationToken,
    AuthorisationOpts,
    NamespaceId,
    SubspaceId,
    PayloadDigest
  >,
): Promise<
  | Store<
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    AuthorisationOpts,
    AuthorisationToken,
    PreFingerprint,
    Fingerprint
  >
  | ValidationError
> {
  opts.dropStream.pipeTo(opts.decryptTransform.writable);

  const bytes = new GrowingBytes(opts.decryptTransform.readable);

  await bytes.nextAbsolute(8);

  let remaining = new DataView(bytes.array.buffer).getBigUint64(0);

  bytes.prune(8);

  let prevEntry = defaultEntry(
    opts.schemes.namespace.defaultNamespaceId,
    opts.schemes.subspace.minimalSubspaceId,
    opts.schemes.payload.defaultDigest,
  );

  let store:
    | Store<
      NamespaceId,
      SubspaceId,
      PayloadDigest,
      AuthorisationOpts,
      AuthorisationToken,
      PreFingerprint,
      Fingerprint
    >
    | null = null;

  while (remaining > 0) {
    const entry = await decodeStreamEntryRelativeEntry(
      {
        decodeStreamNamespace: opts.schemes.namespace.decodeStream,
        decodeStreamSubspace: opts.schemes.subspace.decodeStream,
        decodeStreamPayloadDigest: opts.schemes.payload.decodeStream,
        pathScheme: opts.schemes.path,
      },
      bytes,
      prevEntry,
    );

    prevEntry = entry;

    if (store === null) {
      store = await opts.getStore(entry.namespaceId);
    }

    const token = await opts.decodeStreamAuthorisationToken(bytes);

    const ingestResult = await store.ingestEntry(entry, token);

    if (ingestResult.kind === "failure") {
      throw new ValidationError(
        "Drop included entry which could not be ingested",
      );
    }

    const fifo = new FIFO<Uint8Array>();

    const payloadIngestPromise = store.ingestPayload({
      path: entry.path,
      subspace: entry.subspaceId,
      timestamp: entry.timestamp,
    }, fifo);

    let remainingBytes = entry.payloadLength;

    const chunkSize = 2;

    while (remainingBytes > 0) {
      if (remainingBytes > chunkSize) {
        await bytes.nextAbsolute(chunkSize);

        fifo.push(bytes.array.slice(0, chunkSize));
        bytes.prune(chunkSize);

        remainingBytes -= BigInt(chunkSize);
      } else {
        await bytes.nextAbsolute(Number(remainingBytes));

        fifo.push(bytes.array.slice(0, Number(remainingBytes))),
          bytes.prune(Number(remainingBytes));

        remainingBytes = 0n;
      }
    }

    const payloadIngestResult = await payloadIngestPromise;

    if (payloadIngestResult.kind === "failure") {
      throw new ValidationError(
        "Drop included payload which could not be ingested",
      );
    }

    remaining -= 1n;
  }

  if (store === null) {
    return new ValidationError("The drop contained no entries.");
  }

  return store;
}
