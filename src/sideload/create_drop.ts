import {
  type AreaOfInterest,
  bigintToBytes,
  defaultEntry,
  encodeEntryRelativeEntry,
  type PathScheme,
} from "@earthstar/willow-utils";
import type { Store } from "../store/store.ts";
import type {
  NamespaceScheme,
  PayloadScheme,
  SubspaceScheme,
} from "../store/types.ts";

/** Get the size of all the entries included by an {@linkcode AreaOfInterest} with complete payloads. */
async function getDropContentSize<
  NamespaceId,
  SubspaceId,
  PayloadDigest,
  AuthorisationOpts,
  AuthorisationToken,
  Prefingerprint,
  Fingerprint,
>(
  store: Store<
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    AuthorisationOpts,
    AuthorisationToken,
    Prefingerprint,
    Fingerprint
  >,
  areaOfInterest: AreaOfInterest<SubspaceId>,
): Promise<bigint> {
  let count = 0n;

  for await (
    const [entry, payload] of store.query(
      areaOfInterest,
      "subspace",
    )
  ) {
    if (!payload) {
      continue;
    }

    if (await payload.length() !== entry.payloadLength) {
      continue;
    }

    count += 1n;
  }

  return count;
}

export type DropContentsStreamOpts<
  NamespaceId,
  SubspaceId,
  PayloadDigest,
  AuthorisationOpts,
  AuthorisationToken,
  Prefingerprint,
  Fingerprint,
> = {
  store: Store<
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    AuthorisationOpts,
    AuthorisationToken,
    Prefingerprint,
    Fingerprint
  >;
  areaOfInterest: AreaOfInterest<SubspaceId>;
  schemes: {
    namespace: NamespaceScheme<NamespaceId>;
    subspace: SubspaceScheme<SubspaceId>;
    payload: PayloadScheme<PayloadDigest>;
    path: PathScheme;
  };
  encodeAuthorisationToken: (token: AuthorisationToken) => Uint8Array;
};

/** Produces the **unencrypted** encoded `contents` for a [drop](https://willowprotocol.org/specs/sideloading/index.html#drop) including the contents of a particular {@linkcode AreaOfInterest}. */
export class DropContentsStream<
  NamespaceId,
  SubspaceId,
  PayloadDigest,
  AuthorisationOpts,
  AuthorisationToken,
  Prefingerprint,
  Fingerprint,
> extends ReadableStream<Uint8Array> {
  constructor(
    opts: DropContentsStreamOpts<
      NamespaceId,
      SubspaceId,
      PayloadDigest,
      AuthorisationOpts,
      AuthorisationToken,
      Prefingerprint,
      Fingerprint
    >,
  ) {
    super({
      start: async (controller) => {
        const size = await getDropContentSize(opts.store, opts.areaOfInterest);

        controller.enqueue(bigintToBytes(size));

        let prevEntry = defaultEntry(
          opts.schemes.namespace.defaultNamespaceId,
          opts.schemes.subspace.minimalSubspaceId,
          opts.schemes.payload.defaultDigest,
        );

        for await (
          const [entry, payload, token] of opts.store.query(
            opts.areaOfInterest,
            "subspace",
          )
        ) {
          if (!payload) {
            continue;
          }

          if (await payload.length() !== entry.payloadLength) {
            continue;
          }

          const encodedEntry = encodeEntryRelativeEntry(
            {
              encodeNamespace: opts.schemes.namespace.encode,
              encodeSubspace: opts.schemes.subspace.encode,
              encodePayloadDigest: opts.schemes.payload.encode,
              pathScheme: opts.schemes.path,
              orderSubspace: opts.schemes.subspace.order,
              isEqualNamespace: opts.schemes.namespace.isEqual,
            },
            entry,
            prevEntry,
          );

          prevEntry = entry;

          controller.enqueue(encodedEntry);

          const encodedToken = opts.encodeAuthorisationToken(token);

          controller.enqueue(encodedToken);

          for await (const chunk of await payload.stream()) {
            controller.enqueue(chunk);
          }
        }

        controller.close();
      },
    });
  }
}

/** Required options for creating an encrypted [drop](https://willowprotocol.org/specs/sideloading/index.html#drop). */
export type DropOpts<
  NamespaceId,
  SubspaceId,
  PayloadDigest,
  AuthorisationOpts,
  AuthorisationToken,
  Prefingerprint,
  Fingerprint,
> =
  & DropContentsStreamOpts<
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    AuthorisationOpts,
    AuthorisationToken,
    Prefingerprint,
    Fingerprint
  >
  & {
    encryptTransform: TransformStream<Uint8Array, Uint8Array>;
  };

/** Creates an encrypted [drop](https://willowprotocol.org/specs/sideloading/index.html#drop) containing all entries for a given {@linkcode AreaOfInterest} within a {@linkcode Store}.
 *
 * @returns A {@linkcode ReadableStream} which outputs the bytes of the drop.
 */
export function createDrop<
  NamespaceId,
  SubspaceId,
  PayloadDigest,
  AuthorisationOpts,
  AuthorisationToken,
  Prefingerprint,
  Fingerprint,
>(
  opts: DropOpts<
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    AuthorisationOpts,
    AuthorisationToken,
    Prefingerprint,
    Fingerprint
  >,
): ReadableStream<Uint8Array> {
  const dropContentStream = new DropContentsStream(opts);

  dropContentStream.pipeTo(opts.encryptTransform.writable);

  return opts.encryptTransform.readable;
}
