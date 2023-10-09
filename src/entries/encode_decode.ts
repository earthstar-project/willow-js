import { EncodingScheme } from "../replica/types.ts";
import { Entry } from "./types.ts";

export function encodeEntry<
  NamespacePublicKey,
  SubspacePublicKey,
  PayloadDigest,
>(
  entry: Entry<NamespacePublicKey, SubspacePublicKey, PayloadDigest>,
  opts: {
    namespacePublicKeyEncoding: EncodingScheme<NamespacePublicKey>;
    subspacePublicKeyEncoding: EncodingScheme<SubspacePublicKey>;
    pathEncoding: EncodingScheme<Uint8Array>;
    payloadDigestEncoding: EncodingScheme<PayloadDigest>;
  },
): Uint8Array {
  if (entry.identifier.path.byteLength > 256) {
    throw new Error("Record identifier path is longer than 2048 bits");
  }

  // Namespace pubkey + Author pubkey + 64 bit uint + path bytelength
  const encodedNamespace = opts.namespacePublicKeyEncoding.encode(
    entry.identifier.namespace,
  );
  const encodedSubspace = opts.subspacePublicKeyEncoding.encode(
    entry.identifier.subspace,
  );
  const encodedPath = opts.pathEncoding.encode(entry.identifier.path);

  const encodedPayloadDigest = opts.payloadDigestEncoding.encode(
    entry.record.hash,
  );

  const recordIdentifierLength = encodedNamespace.byteLength +
    encodedSubspace.byteLength +
    encodedPath.byteLength;

  // time (uint64) + length (uint64) + digest
  const recordLength = 8 + 8 + encodedPayloadDigest.byteLength;

  const totalLength = recordIdentifierLength + recordLength;

  const ui8 = new Uint8Array(totalLength);
  const dataView = new DataView(ui8.buffer);

  let currentPosition = 0;

  // Record identifier

  // Namespace pubkey
  ui8.set(encodedNamespace, currentPosition);

  currentPosition += encodedNamespace.byteLength;

  // Subspace pubkey

  ui8.set(encodedSubspace, currentPosition);

  currentPosition += encodedSubspace.byteLength;

  // Path
  ui8.set(encodedPath, currentPosition);

  currentPosition += encodedPath.byteLength;

  // Record

  // Timestamp
  dataView.setBigUint64(currentPosition, entry.record.timestamp);

  currentPosition += 8;

  // Length
  dataView.setBigUint64(currentPosition, entry.record.length);

  currentPosition += 8;

  // Hash
  ui8.set(encodedPayloadDigest, currentPosition);

  return ui8;
}

export function decodeEntry<
  NamespacePublicKey,
  SubspacePublicKey,
  PayloadDigest,
>(
  encodedEntry: Uint8Array,
  opts: {
    namespacePublicKeyEncoding: EncodingScheme<NamespacePublicKey>;
    subspacePublicKeyEncoding: EncodingScheme<SubspacePublicKey>;
    pathEncoding: EncodingScheme<Uint8Array>;
    payloadDigestEncoding: EncodingScheme<PayloadDigest>;
  },
): Entry<NamespacePublicKey, SubspacePublicKey, PayloadDigest> {
  const dataView = new DataView(encodedEntry.buffer);

  const namespaceKey = opts.namespacePublicKeyEncoding.decode(
    encodedEntry.subarray(0),
  );
  const encodedNamespaceLength = opts.namespacePublicKeyEncoding.encodedLength(
    namespaceKey,
  );

  const subspaceKey = opts.subspacePublicKeyEncoding.decode(
    encodedEntry.subarray(encodedNamespaceLength),
  );
  const encodedSubspaceLength = opts.subspacePublicKeyEncoding.encodedLength(
    subspaceKey,
  );

  const path = opts.pathEncoding.decode(
    encodedEntry.subarray(encodedNamespaceLength + encodedSubspaceLength),
  );
  const encodedPathLength = opts.pathEncoding.encodedLength(path);

  const identifierLength = encodedNamespaceLength + encodedSubspaceLength +
    encodedPathLength;

  return {
    identifier: {
      namespace: namespaceKey,
      subspace: subspaceKey,
      path: path,
    },
    record: {
      timestamp: dataView.getBigUint64(identifierLength),
      length: dataView.getBigUint64(identifierLength + 8),
      hash: opts.payloadDigestEncoding.decode(
        encodedEntry.subarray(identifierLength + 8 + 8),
      ),
    },
  };
}
