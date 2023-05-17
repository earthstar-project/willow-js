import { Entry } from "./types.ts";

export function encodeEntry(
  entry: Entry,
): ArrayBuffer {
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
  const namespaceUi8 = new Uint8Array(entry.identifier.namespace);
  ui8.set(namespaceUi8, currentPosition);

  currentPosition += entry.identifier.namespace.byteLength;

  // Author pubkey
  const authorUi8 = new Uint8Array(entry.identifier.author);
  ui8.set(authorUi8, currentPosition);

  currentPosition += entry.identifier.author.byteLength;

  // Path
  const pathUi8 = new Uint8Array(entry.identifier.path);
  ui8.set(pathUi8, currentPosition);

  currentPosition += entry.identifier.path.byteLength;

  // Record

  // Timestamp
  dataView.setBigUint64(currentPosition, entry.record.timestamp);

  currentPosition += 8;

  // Length
  dataView.setBigUint64(currentPosition, entry.record.length);

  currentPosition += 8;

  // Hash
  const hashUi8 = new Uint8Array(entry.record.hash);
  ui8.set(hashUi8, currentPosition);

  return ui8.buffer;
}

export function decodeEntry(
  encodedEntry: ArrayBuffer,
  opts: {
    pubKeyLength: number;
    digestLength: number;
  },
): Entry {
  const encodedUi8 = new Uint8Array(encodedEntry);
  const dataView = new DataView(encodedEntry);

  const pathLength = encodedEntry.byteLength - (8 + opts.digestLength) -
    (opts.pubKeyLength * 2 + 8);

  return {
    identifier: {
      namespace: encodedUi8.subarray(0, opts.pubKeyLength).buffer,
      author:
        encodedUi8.subarray(opts.pubKeyLength, opts.pubKeyLength * 2).buffer,
      path: encodedUi8.subarray(
        opts.pubKeyLength * 2,
        opts.pubKeyLength * 2 + pathLength,
      ).buffer,
    },
    record: {
      timestamp: dataView.getBigUint64(opts.pubKeyLength * 2 + pathLength),
      length: dataView.getBigUint64(opts.pubKeyLength * 2 + pathLength + 8),
      hash:
        encodedUi8.subarray(opts.pubKeyLength * 2 + 8 + pathLength + 8).buffer,
    },
  };
}
