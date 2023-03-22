import { Entry } from "./types.ts";

export function encodeEntry(
  entry: Entry,
): Uint8Array {
  if (entry.identifier.path.byteLength > 256) {
    throw new Error("Record identifier path is longer than 2048 bits");
  }

  // Namespace pubkey + Author pubkey + 64 bit uint + path bytelength
  const recordIdentifierLength = entry.identifier.namespace.byteLength +
    entry.identifier.author.byteLength +
    8 + entry.identifier.path.byteLength;
  // 64 bit uint + digest
  const recordLength = 8 + entry.record.hash.byteLength;

  const totalLength = recordIdentifierLength + recordLength;

  const ui8 = new Uint8Array(totalLength);
  const dataView = new DataView(ui8.buffer);

  let currentPosition = 0;

  // Record identifier

  // Namespace pubkey
  ui8.set(entry.identifier.namespace, currentPosition);

  currentPosition += entry.identifier.namespace.byteLength;

  // Author pubkey
  ui8.set(entry.identifier.author, currentPosition);

  currentPosition += entry.identifier.author.byteLength;

  // Timestamp
  dataView.setBigUint64(currentPosition, entry.identifier.timestamp);

  currentPosition += 8;

  // Path
  ui8.set(entry.identifier.path, currentPosition);

  currentPosition += entry.identifier.path.byteLength;

  // Record

  // Length
  dataView.setBigUint64(currentPosition, entry.record.length);

  currentPosition += 8;

  // Hash

  ui8.set(entry.record.hash, currentPosition);

  return ui8;
}

export function decodeEntry(
  encodedEntry: Uint8Array,
  opts: {
    pubKeyLength: number;
    digestLength: number;
  },
): Entry {
  const dataView = new DataView(encodedEntry.buffer);

  const pathLength = encodedEntry.byteLength - (8 + opts.digestLength) -
    (opts.pubKeyLength * 2 + 8);

  return {
    identifier: {
      namespace: encodedEntry.subarray(0, opts.pubKeyLength),
      author: encodedEntry.subarray(opts.pubKeyLength, opts.pubKeyLength * 2),
      timestamp: dataView.getBigUint64(opts.pubKeyLength * 2),
      path: encodedEntry.subarray(
        opts.pubKeyLength * 2 + 8,
        opts.pubKeyLength * 2 + 8 + pathLength,
      ),
    },
    record: {
      length: dataView.getBigUint64(opts.pubKeyLength * 2 + 8 + pathLength),
      hash: encodedEntry.subarray(opts.pubKeyLength * 2 + 8 + pathLength + 8),
    },
  };
}
