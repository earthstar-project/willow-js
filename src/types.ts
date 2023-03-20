export type AuthorId = {
  /** a sequence of four ascii characters (lower-case letters and numbers only) */
  // TS is not adequately capable of expressing this,
  // And JS doesn't even have chars...
  // is this a tuple of 4 strings, or 4 numbers 0-255?
  shortname: [string, string, string, string];
  /** Bit string */
  publicKey: Uint8Array;
};

export type AuthorKeypair = {
  address: AuthorId;
  /** Bit string */
  secret: Uint8Array;
};

export type NamespaceId = {
  // Should namespace 'names' have fixed length, like authors?
  // Alternatively they are encoded with their length.
  name: string;
  /** Bit string */
  publicKey: Uint8Array;
};

export type NamespaceKeypair = {
  address: NamespaceId;
  /** Bit string */
  secret: Uint8Array;
};

export type RecordIdentifier = {
  namespace: NamespaceId;
  author: AuthorId;
  /** 64 bit integer (interpreted as microseconds since the Unix epoch). Big-endian. */
  timestamp: Uint8Array;
  /** Bit string of length at most 2048 */
  path: Uint8Array;
};

export type Record = {
  /** 64 bit integer */
  length: Uint8Array;
  /** 256 bit integer*/
  hash: Uint8Array;
};

export type Entry = {
  identifier: RecordIdentifier;
  record: Record;
};

export type SignedEntry = {
  entry: Entry;
  /** Bit string */
  authorSignature: Uint8Array;
  /** Bit string */
  namespaceSignature: Uint8Array;
};
