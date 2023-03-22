export type RecordIdentifier = {
  /** The namespace's public key as a fixed-width integer */
  namespace: ArrayBuffer;
  /** The author's public key as a fixed-width integer*/
  author: ArrayBuffer;
  /** 64 bit integer (interpreted as microseconds since the Unix epoch). Big-endian. */
  timestamp: bigint;
  /** Bit string of length at most 2048 */
  path: ArrayBuffer;
};

export type Record = {
  /** 64 bit integer */
  length: bigint;
  /** digest-length bit integer*/
  hash: ArrayBuffer;
};

export type Entry = {
  identifier: RecordIdentifier;
  record: Record;
};

export type SignedEntry = {
  entry: Entry;
  /** Bit string */
  authorSignature: ArrayBuffer;
  /** Bit string */
  namespaceSignature: ArrayBuffer;
};
