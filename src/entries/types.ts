export type RecordIdentifier<NamespacePublicKey, SubspacePublicKey> = {
  /** The namespace's public key as a fixed-width integer */
  namespace: NamespacePublicKey;
  /** The author's public key as a fixed-width integer*/
  subspace: SubspacePublicKey;
  /** Bit string of length at most 2048 */
  path: Uint8Array;
};

export type Record<PayloadDigest> = {
  /** 64 bit integer (interpreted as microseconds since the Unix epoch). Big-endian. */
  timestamp: bigint;
  /** 64 bit integer */
  length: bigint;
  /** digest-length bit integer*/
  hash: PayloadDigest;
};

export type Entry<NamespacePublicKey, SubspacePublicKey, PayloadDigest> = {
  identifier: RecordIdentifier<NamespacePublicKey, SubspacePublicKey>;
  record: Record<PayloadDigest>;
};
