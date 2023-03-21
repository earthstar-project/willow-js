export type RecordIdentifier = {
  /** The namespace's public key as a fixed-width integer */
  namespace: Uint8Array;
  /** The author's public key as a fixed-width integer*/
  author: Uint8Array;
  /** 64 bit integer (interpreted as microseconds since the Unix epoch). Big-endian. */
  timestamp: bigint;
  /** Bit string of length at most 2048 */
  path: Uint8Array;
};

export type Record = {
  /** 64 bit integer */
  length: bigint;
  /** digest-length bit integer*/
  hash: Uint8Array;
};

export type Entry = {
  identifier: RecordIdentifier;
  record: Record;
};

export type SignedEntry = {
  recordIdentifier: RecordIdentifier;
  entry: Entry;
  /** Bit string */
  authorSignature: Uint8Array;
  /** Bit string */
  namespaceSignature: Uint8Array;
};
