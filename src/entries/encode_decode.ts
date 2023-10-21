import {
  NamespaceScheme,
  PathLengthScheme,
  PayloadScheme,
  SubspaceScheme,
} from "../replica/types.ts";
import { Entry } from "./types.ts";

export function encodeEntry<
  NamespacePublicKey,
  SubspacePublicKey,
  PayloadDigest,
>(
  entry: Entry<NamespacePublicKey, SubspacePublicKey, PayloadDigest>,
  opts: {
    namespaceScheme: NamespaceScheme<NamespacePublicKey>;
    subspaceScheme: SubspaceScheme<SubspacePublicKey>;
    pathLengthScheme: PathLengthScheme;
    payloadScheme: PayloadScheme<PayloadDigest>;
  },
): Uint8Array {
  if (entry.identifier.path.byteLength > 256) {
    throw new Error("Record identifier path is longer than 2048 bits");
  }

  // Namespace pubkey + Author pubkey + 64 bit uint + path bytelength
  const encodedNamespace = opts.namespaceScheme.encode(
    entry.identifier.namespace,
  );
  const encodedSubspace = opts.subspaceScheme.encode(
    entry.identifier.subspace,
  );
  const encodedPathLength = opts.pathLengthScheme.encode(
    entry.identifier.path.byteLength,
  );

  //const encodedPath = concat(encodedPathLength, entry.identifier.path);

  const encodedPayloadDigest = opts.payloadScheme.encode(
    entry.record.hash,
  );

  const recordIdentifierLength = encodedNamespace.byteLength +
    encodedSubspace.byteLength +
    +encodedPathLength.byteLength +
    entry.identifier.path.byteLength;

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
  ui8.set(encodedPathLength, currentPosition);

  currentPosition += encodedPathLength.byteLength;

  ui8.set(entry.identifier.path, currentPosition);

  currentPosition += entry.identifier.path.byteLength;

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
    namespaceScheme: NamespaceScheme<NamespacePublicKey>;
    subspaceScheme: SubspaceScheme<SubspacePublicKey>;
    pathLengthScheme: PathLengthScheme;
    payloadScheme: PayloadScheme<PayloadDigest>;
  },
): Entry<NamespacePublicKey, SubspacePublicKey, PayloadDigest> {
  const dataView = new DataView(encodedEntry.buffer);

  const namespaceKey = opts.namespaceScheme.decode(
    encodedEntry.subarray(0),
  );
  const encodedNamespaceLength = opts.namespaceScheme.encodedLength(
    namespaceKey,
  );

  const subspaceKey = opts.subspaceScheme.decode(
    encodedEntry.subarray(encodedNamespaceLength),
  );
  const encodedSubspaceLength = opts.subspaceScheme.encodedLength(
    subspaceKey,
  );

  const pathLength = opts.pathLengthScheme.decode(
    encodedEntry.subarray(encodedNamespaceLength + encodedSubspaceLength),
  );

  const encodedPathLengthLength = opts.pathLengthScheme.encodedLength(
    pathLength,
  );

  const path = encodedEntry.subarray(
    encodedNamespaceLength + encodedSubspaceLength + encodedPathLengthLength,
    encodedNamespaceLength + encodedSubspaceLength + encodedPathLengthLength +
      pathLength,
  );

  const identifierLength = encodedNamespaceLength + encodedSubspaceLength +
    encodedPathLengthLength +
    pathLength;

  return {
    identifier: {
      namespace: namespaceKey,
      subspace: subspaceKey,
      path: path,
    },
    record: {
      timestamp: dataView.getBigUint64(identifierLength),
      length: dataView.getBigUint64(identifierLength + 8),
      hash: opts.payloadScheme.decode(
        encodedEntry.subarray(identifierLength + 8 + 8),
      ),
    },
  };
}
