import { Capability } from "$meadowcap/mod.ts";

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

/** A valid capability and an accompanying signature. */
export type AuthorisationToken<
  NamespacePublicKey,
  NamespaceSignature,
  SubspacePublicKey,
  SubspaceSignature,
> = [
  Capability<
    NamespacePublicKey,
    NamespaceSignature,
    SubspacePublicKey,
    SubspaceSignature
  >,
  NamespaceSignature | SubspaceSignature,
];

export type EncodingScheme<ValueType> = {
  /** A function to encode a given `ValueType`. */
  encode(value: ValueType): Uint8Array;
  /** A function to decode a given `ValueType` */
  decode(encoded: Uint8Array): ValueType;
  /** A function which returns the bytelength for a given `ValueType` when encoded. */
  encodedLength(value: ValueType): number;
};

export type KeypairEncodingScheme<PublicKey, Signature> = {
  /** The encoding scheme for a key pair's public key type. */
  publicKey: EncodingScheme<PublicKey>;
  /** The encoding scheme for a key pair's signature type. */
  signature: EncodingScheme<Signature>;
};

/** A scheme for signing and verifying data using key pairs. */
export type SignatureScheme<PublicKey, SecretKey, Signature> = {
  sign: (secretKey: SecretKey, bytestring: Uint8Array) => Promise<Signature>;
  verify: (
    publicKey: PublicKey,
    signature: Signature,
    bytestring: Uint8Array,
  ) => Promise<boolean>;
};

export type KeypairScheme<PublicKey, SecretKey, Signature> = {
  signatureScheme: SignatureScheme<PublicKey, SecretKey, Signature>;
  encodingScheme: KeypairEncodingScheme<PublicKey, Signature>;
};
