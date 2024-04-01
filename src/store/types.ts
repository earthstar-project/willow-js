import {
  EncodingScheme,
  Entry,
  Path,
  PathScheme,
  SuccessorFn,
  TotalOrder,
} from "../../deps.ts";
import { EntryDriver, PayloadDriver } from "./storage/types.ts";

export type NamespaceScheme<NamespaceId> = EncodingScheme<NamespaceId> & {
  isEqual: EqualityFn<NamespaceId>;
  defaultNamespaceId: NamespaceId;
};

export type SubspaceScheme<SubspaceId> = EncodingScheme<SubspaceId> & {
  successor: SuccessorFn<SubspaceId>;
  order: TotalOrder<SubspaceId>;
  minimalSubspaceId: SubspaceId;
};

export type PayloadScheme<PayloadDigest> = EncodingScheme<PayloadDigest> & {
  fromBytes: (
    bytes: Uint8Array | AsyncIterable<Uint8Array>,
  ) => Promise<PayloadDigest>;
  order: (a: PayloadDigest, b: PayloadDigest) => -1 | 0 | 1;
  defaultDigest: PayloadDigest;
};

export type AuthorisationScheme<
  NamespaceId,
  SubspaceId,
  PayloadDigest,
  AuthorisationOpts,
  AuthorisationToken,
> = {
  /** Produce an authorisation token from an entry */
  authorise(
    entry: Entry<NamespaceId, SubspaceId, PayloadDigest>,
    opts: AuthorisationOpts,
  ): Promise<AuthorisationToken>;
  /** Verify if an entry is authorised to be written */
  isAuthorisedWrite: (
    entry: Entry<NamespaceId, SubspaceId, PayloadDigest>,
    token: AuthorisationToken,
  ) => Promise<boolean>;
  tokenEncoding: EncodingScheme<AuthorisationToken>;
};

export type FingerprintScheme<
  NamespaceId,
  SubspaceId,
  PayloadDigest,
  Fingerprint,
> = {
  fingerprintSingleton(
    entry: LengthyEntry<NamespaceId, SubspaceId, PayloadDigest>,
  ): Promise<Fingerprint>;
  fingerprintCombine(
    a: Fingerprint,
    b: Fingerprint,
  ): Fingerprint;
  neutral: Fingerprint;
  isEqual: (a: Fingerprint, b: Fingerprint) => boolean;
  encoding: EncodingScheme<Fingerprint>;
};

/** Concrete parameters peculiar to a specific usage of Willow. */
export interface StoreSchemes<
  NamespaceId,
  SubspaceId,
  PayloadDigest,
  AuthorisationOpts,
  AuthorisationToken,
  Fingerprint,
> {
  path: PathScheme;

  namespace: NamespaceScheme<NamespaceId>;

  subspace: SubspaceScheme<SubspaceId>;

  // Learn about payloads and producing them from bytes
  payload: PayloadScheme<PayloadDigest>;

  authorisation: AuthorisationScheme<
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    AuthorisationOpts,
    AuthorisationToken
  >;

  fingerprint: FingerprintScheme<
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    Fingerprint
  >;
}

export type StoreOpts<
  NamespaceId,
  SubspaceId,
  PayloadDigest,
  AuthorisationOpts,
  AuthorisationToken,
  Fingerprint,
> = {
  /** The public key of the namespace this store holds entries for. */
  namespace: NamespaceId;
  /** The protocol schemes this store should use. */
  schemes: StoreSchemes<
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    AuthorisationOpts,
    AuthorisationToken,
    Fingerprint
  >;
  /** An optional driver used to store and retrieve a store's entries. */
  entryDriver?: EntryDriver<
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    Fingerprint
  >;
  /** An optional driver used to store and retrieve a store's payloads.  */
  payloadDriver?: PayloadDriver<PayloadDigest>;
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

export type QueryOrder =
  /** By path, then timestamp, then subspace */
  | "path"
  /** By timestamp, then subspace, then path */
  | "timestamp"
  /** By subspace, then path, then timestamp */
  | "subspace";

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

/** The (possibly partial) data associated with a {@link SignedEntry}. */
export type Payload = {
  /** Retrieves the payload's available data all at once in a single {@link Uint8Array}. */
  bytes: (offset?: number) => Promise<Uint8Array>;
  /** A {@link ReadableStream} of the payload's available data which can be read chunk by chunk. */
  stream: (offset?: number) => Promise<AsyncIterable<Uint8Array>>;
  /** The length in bytes of the available payload. */
  length: () => Promise<bigint>;
};

export type EntryInput<SubspacePublicKey> = {
  path: Path;
  subspace: SubspacePublicKey;
  payload: Uint8Array | ReadableStream<Uint8Array>;
  /** The desired timestamp for the new entry. If left undefined, uses the current time, OR if another entry exists at the same path will be that entry's timestamp + 1. */
  timestamp?: bigint;
};

export type IngestPayloadEventFailure = {
  kind: "failure";
  reason: "no_entry" | "data_mismatch";
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

export type LengthyEntry<NamespaceId, SubspaceId, PayloadDigest> = {
  entry: Entry<NamespaceId, SubspaceId, PayloadDigest>;
  available: bigint;
};
