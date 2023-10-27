import { Products } from "../../deps.ts";
import { Entry } from "../entries/types.ts";
import { EntryDriver, PayloadDriver } from "./storage/types.ts";

export type PathLengthScheme = EncodingScheme<number> & {
  maxLength: number;
};

export type NamespaceScheme<NamespaceKey> = EncodingScheme<NamespaceKey> & {
  isEqual: EqualityFn<NamespaceKey>;
};

export type SubspaceScheme<SubspaceKey> = EncodingScheme<SubspaceKey> & {
  isEqual: EqualityFn<SubspaceKey>;
  order: Products.TotalOrder<SubspaceKey>;
  successor: Products.SuccessorFn<SubspaceKey>;
  minimalSubspaceKey: SubspaceKey;
};

export type PayloadScheme<PayloadDigest> = EncodingScheme<PayloadDigest> & {
  fromBytes: (bytes: Uint8Array | ReadableStream) => Promise<PayloadDigest>;
  order: (a: PayloadDigest, b: PayloadDigest) => -1 | 0 | 1;
};

export type AuthorisationScheme<
  NamespaceKey,
  SubspaceKey,
  PayloadDigest,
  AuthorisationOpts,
  AuthorisationToken,
> = {
  authorise(
    entry: Entry<NamespaceKey, SubspaceKey, PayloadDigest>,
    opts: AuthorisationOpts,
  ): Promise<AuthorisationToken>;
  isAuthorised: (
    entry: Entry<NamespaceKey, SubspaceKey, PayloadDigest>,
    token: AuthorisationToken,
  ) => Promise<boolean>;
  tokenEncoding: EncodingScheme<AuthorisationToken>;
};

export type FingerprintScheme<
  NamespaceKey,
  SubspaceKey,
  PayloadDigest,
  Fingerprint,
> = {
  fingerprintSingleton(
    entry: Entry<NamespaceKey, SubspaceKey, PayloadDigest>,
  ): Promise<Fingerprint>;
  fingerprintCombine(
    a: Fingerprint,
    b: Fingerprint,
  ): Fingerprint;
  neutral: Fingerprint;
};

/** Concrete parameters peculiar to a specific usage of Willow. */
export interface ProtocolParameters<
  NamespaceKey,
  SubspaceKey,
  PayloadDigest,
  AuthorisationOpts,
  AuthorisationToken,
  Fingerprint,
> {
  pathLengthScheme: PathLengthScheme;

  // Namespace encoding scheme
  namespaceScheme: NamespaceScheme<NamespaceKey>;

  // Learn what
  subspaceScheme: SubspaceScheme<SubspaceKey>;

  // Learn about payloads and producing them from bytes
  payloadScheme: PayloadScheme<PayloadDigest>;

  authorisationScheme: AuthorisationScheme<
    NamespaceKey,
    SubspaceKey,
    PayloadDigest,
    AuthorisationOpts,
    AuthorisationToken
  >;

  fingerprintScheme: FingerprintScheme<
    NamespaceKey,
    SubspaceKey,
    PayloadDigest,
    Fingerprint
  >;
}

export type ReplicaOpts<
  NamespaceKey,
  SubspaceKey,
  PayloadDigest,
  AuthorisationOpts,
  AuthorisationToken,
  Fingerprint,
> = {
  /** The public key of the namespace this replica is a snapshot of. */
  namespace: NamespaceKey;
  /** The protocol parameters this replica should use. */
  protocolParameters: ProtocolParameters<
    NamespaceKey,
    SubspaceKey,
    PayloadDigest,
    AuthorisationOpts,
    AuthorisationToken,
    Fingerprint
  >;
  /** An optional driver used to store and retrieve a replica's entries. */
  entryDriver?: EntryDriver<
    NamespaceKey,
    SubspaceKey,
    PayloadDigest,
    Fingerprint
  >;
  /** An option driver used to store and retrieve a replica's payloads.  */
  payloadDriver?: PayloadDriver<PayloadDigest>;
};

export type QueryOrder =
  /** By path, then timestamp, then subspace */
  | "path"
  /** By timestamp, then subspace, then path */
  | "timestamp"
  /** By subspace, then path, then timestamp */
  | "subspace";

export type OptionalBounds<ValueType> = {
  /** The value to start returning results from, inclusive. Starts from the first entry in the replica if left undefined. */
  lowerBound?: ValueType;
  /** The value to stop returning results at, exclusive. Stops after the last entry in the replica if  undefined. */
  upperBound: ValueType;
} | {
  /** The value to start returning results from, inclusive. Starts from the first entry in the replica if left undefined. */
  lowerBound: ValueType;
  /** The value to stop returning results at, exclusive. Stops after the last entry in the replica if  undefined. */
  upperBound?: ValueType;
};

export interface Query<SubspaceKey> {
  order: QueryOrder;
  subspace?: OptionalBounds<SubspaceKey>;
  path?: OptionalBounds<Uint8Array>;
  time?: OptionalBounds<bigint>;
  /** The maximum number of results to return. */
  limit?: number;
  /** Whether the results should be returned in reverse order. */
  reverse?: boolean;
}

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

export type EqualityFn<ValueType> = (a: ValueType, b: ValueType) => boolean;

// Events

export type IngestEventFailure = {
  kind: "failure";
  reason: "write_failure" | "invalid_entry";
  message: string;
  err: Error | null;
};

export type IngestEventNoOp = {
  kind: "no_op";
  reason: "obsolete_from_same_subspace" | "newer_prefix_found";
};

export type IngestEventSuccess<
  NamespacePublicKey,
  SubspacePublicKey,
  PayloadDigest,
  AuthorisationToken,
> = {
  kind: "success";
  /** The successfully ingested signed entry. */
  entry: Entry<NamespacePublicKey, SubspacePublicKey, PayloadDigest>;
  authToken: AuthorisationToken;

  /** An ID representing the source of this ingested entry. */
  externalSourceId?: string;
};

export type IngestEvent<
  NamespacePublicKey,
  SubspacePublicKey,
  PayloadDigest,
  AuthorisationToken,
> =
  | IngestEventFailure
  | IngestEventNoOp
  | IngestEventSuccess<
    NamespacePublicKey,
    SubspacePublicKey,
    PayloadDigest,
    AuthorisationToken
  >;

/** The data associated with a {@link SignedEntry}. */
export type Payload = {
  /** Retrieves the payload's data all at once in a single {@link Uint8Array}. */
  bytes: () => Promise<Uint8Array>;
  /** A {@link ReadableStream} of the payload's data which can be read chunk by chunk. */
  stream: ReadableStream<Uint8Array>;
};

export type EntryInput<SubspacePublicKey> = {
  path: Uint8Array;
  subspace: SubspacePublicKey;
  payload: Uint8Array | ReadableStream<Uint8Array>;
  /** The desired timestamp for the new entry. If left undefined, uses the current time, OR if another entry exists at the same path will be that entry's timestamp + 1. */
  timestamp?: bigint;
};

export type IngestPayloadEventFailure = {
  kind: "failure";
  reason: "no_entry" | "mismatched_hash";
};

export type IngestPayloadEventNoOp = {
  kind: "no_op";
  reason: "already_have_it";
};

export type IngestPayloadEventSuccess = {
  kind: "success";
};

export type IngestPayloadEvent =
  | IngestPayloadEventFailure
  | IngestPayloadEventNoOp
  | IngestPayloadEventSuccess;
