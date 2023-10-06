import { Entry } from "../entries/types.ts";
import { EntryDriver, PayloadDriver } from "./storage/types.ts";

/** Concrete parameters peculiar to a specific usage of Willow. */
export interface ProtocolParameters<
  NamespacePublicKey,
  SubspacePublicKey,
  PayloadDigest,
  AuthorisationOpts,
  AuthorisationToken,
> {
  // The path encoding scheme.
  /** The encoding scheme used for paths.
   *
   * An encoded path **must** be the concatenation of a big-endian k-bit integer (where k is the number of bits needed to represent all numbers from 0 to your max path length, inclusive) and the bytes of the paths themselves.
   */
  pathEncoding: EncodingScheme<Uint8Array>;

  // Namespace encoding scheme
  namespaceScheme: EncodingScheme<NamespacePublicKey> & {
    isEqual: EqualityFn<NamespacePublicKey>;
  };

  // Learn what
  subspaceScheme: EncodingScheme<SubspacePublicKey> & {
    isEqual: EqualityFn<SubspacePublicKey>;
  };

  // Learn about payloads and producing them from bytes
  payloadScheme: EncodingScheme<PayloadDigest> & {
    fromBytes: (bytes: Uint8Array | ReadableStream) => Promise<PayloadDigest>;
    order: (a: PayloadDigest, b: PayloadDigest) => -1 | 0 | 1;
  };

  authorisationScheme: {
    authorise(
      entry: Entry<NamespacePublicKey, SubspacePublicKey, PayloadDigest>,
      opts: AuthorisationOpts,
    ): Promise<AuthorisationToken>;
    isAuthorised: (
      entry: Entry<NamespacePublicKey, SubspacePublicKey, PayloadDigest>,
      token: AuthorisationToken,
    ) => Promise<boolean>;
    tokenEncoding: EncodingScheme<AuthorisationToken>;
  };
}

export type ReplicaOpts<
  NamespacePublicKey,
  SubspacePublicKey,
  PayloadDigest,
  AuthorisationOpts,
  AuthorisationToken,
> = {
  /** The public key of the namespace this replica is a snapshot of. */
  namespace: NamespacePublicKey;
  /** The protocol parameters this replica should use. */
  protocolParameters: ProtocolParameters<
    NamespacePublicKey,
    SubspacePublicKey,
    PayloadDigest,
    AuthorisationOpts,
    AuthorisationToken
  >;
  /** An optional driver used to store and retrieve a replica's entries. */
  entryDriver?: EntryDriver;
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

export interface QueryBase {
  /** The order to return results in. */
  order: QueryOrder;
  /** The maximum number of results to return. */
  limit?: number;
  /** Whether the results should be returned in reverse order. */
  reverse?: boolean;
}

export interface PathQuery extends QueryBase {
  order: "path";
  /** The path to start returning results from, inclusive. Starts from the first entry in the replica if left undefined. */
  lowerBound?: Uint8Array;
  /** The path to stop returning results at, exclusive. Stops after the last entry in the replica if  undefined. */
  upperBound?: Uint8Array;
}

export interface SubspaceQuery<SubspacePublicKey> extends QueryBase {
  order: "subspace";
  /** The subspace public key to start returning results from, inclusive. Starts from the first entry in the replica if left undefined. */
  lowerBound?: SubspacePublicKey;
  /** The subspace public key to stop returning results at, exclusive. Stops after the last entry in the replica if  undefined. */
  upperBound?: SubspacePublicKey;
}

export interface TimestampQuery extends QueryBase {
  order: "timestamp";
  /** The timestamp to start returning results from, inclusive. Starts from the first entry in the replica if left undefined. */
  lowerBound?: bigint;
  /** The timestamp to stop returning results at, exclusive. Stops after the last entry in the replica if  undefined. */
  upperBound?: bigint;
}

export type Query<SubspacePublicKey> =
  | PathQuery
  | SubspaceQuery<SubspacePublicKey>
  | TimestampQuery;

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
