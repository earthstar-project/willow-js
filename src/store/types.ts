import {
  EncodingScheme,
  Entry,
  Path,
  PathScheme,
  SuccessorFn,
  TotalOrder,
} from "../../deps.ts";
import { EntryDriver, PayloadDriver } from "./storage/types.ts";

/** Configures the [`NamespaceId` parameter](https://willowprotocol.org/specs/data-model/index.html#NamespaceId) of the [Willow data model](https://willowprotocol.org/specs/data-model/index.html#data_model), as well as some associated parameters for the [Willow General Sync Protocol](https://willowprotocol.org/specs/sync/index.html#sync). */
export type NamespaceScheme<NamespaceId> = EncodingScheme<NamespaceId> & {
  /** Compare two `NamespaceId` for equality. */
  isEqual: EqualityFn<NamespaceId>;
  /** Used to initialise some variables used during a WGPS sync session. */
  defaultNamespaceId: NamespaceId;
};

/** Configures the [`SubspaceId` parameter](https://willowprotocol.org/specs/data-model/index.html#SubspaceId) of the [Willow data model](https://willowprotocol.org/specs/data-model/index.html#data_model), as well as some associated parameters for the [Willow General Sync Protocol](https://willowprotocol.org/specs/sync/index.html#sync). */
export type SubspaceScheme<SubspaceId> = EncodingScheme<SubspaceId> & {
  /** Produce the next `SubspaceId` in the total order of `SubspaceId` from a given `SubspaceId`. May return null if no successor exists. */
  successor: SuccessorFn<SubspaceId>;
  /** Apply a [total ordering](https://en.wikipedia.org/wiki/Total_order) to two `SubspaceId`. */
  order: TotalOrder<SubspaceId>;
  /** Used to initialise some variables used during a WGPS sync session.  */
  minimalSubspaceId: SubspaceId;
};

/** Configures the [`PayloadDigest` parameter](https://willowprotocol.org/specs/data-model/index.html#PayloadDigest) of the [Willow data model](https://willowprotocol.org/specs/data-model/index.html#data_model), as well as some associated parameters for the [Willow General Sync Protocol](https://willowprotocol.org/specs/sync/index.html#sync).  */
export type PayloadScheme<PayloadDigest> = EncodingScheme<PayloadDigest> & {
  /** Produce a `PayloadDigest` from given data. */
  fromBytes: (
    bytes: Uint8Array | AsyncIterable<Uint8Array>,
  ) => Promise<PayloadDigest>;
  /** Apply a [total ordering](https://en.wikipedia.org/wiki/Total_order) to two `PayloadDigest`. */
  order: (a: PayloadDigest, b: PayloadDigest) => -1 | 0 | 1;
  /** Used to initialise some variables used during a WGPS sync session.  */
  defaultDigest: PayloadDigest;
};

/** Configures the [`AuthorisationToken`](https://willowprotocol.org/specs/data-model/index.html#AuthorisationToken) and [`is_authorised_write`](https://willowprotocol.org/specs/data-model/index.html#is_authorised_write) parameters of the [Willow data model](https://willowprotocol.org/specs/data-model/index.html#data_model), used to restrict write access to a Willow data store. */
export type AuthorisationScheme<
  NamespaceId,
  SubspaceId,
  PayloadDigest,
  AuthorisationOpts,
  AuthorisationToken,
> = {
  /** Produce a valid authorisation token from an entry. */
  authorise(
    entry: Entry<NamespaceId, SubspaceId, PayloadDigest>,
    /** The type which a user can provide to produce a valid `AuthorisationToken` for a new entry, e.g. a keypair. */
    opts: AuthorisationOpts,
  ): Promise<AuthorisationToken>;
  /** Verify if an entry is authorised to be written */
  isAuthorisedWrite: (
    /** The entry being tested against the given `AuthorisationToken`. */
    entry: Entry<NamespaceId, SubspaceId, PayloadDigest>,
    /** A type proving write permission, e.g. a cryptographic signature produced from a keypair.. */
    token: AuthorisationToken,
  ) => Promise<boolean>;
  /** An encoding scheme for `AuthorisationToken`. */
  tokenEncoding: EncodingScheme<AuthorisationToken>;
};

/** Configures the [`Fingerprint`](https://willowprotocol.org/specs/3d-range-based-set-reconciliation/index.html#d3rbsr_fp) and [`PreFingerprint`](https://willowprotocol.org/specs/3d-range-based-set-reconciliation/index.html#d3rbsr_prefp) parameters used by [3d range-based set reconciliation](https://willowprotocol.org/specs/3d-range-based-set-reconciliation/index.html#d3_range_based_set_reconciliation).*/
export type FingerprintScheme<
  NamespaceId,
  SubspaceId,
  PayloadDigest,
  PreFingerprint,
  Fingerprint,
> = {
  /** Configures the [`fingerprint_singleton`](https://willowprotocol.org/specs/3d-range-based-set-reconciliation/index.html#d3rbsr_fp_singleton) variable for [3d range-based set reconciliation](https://willowprotocol.org/specs/3d-range-based-set-reconciliation/index.html#d3_range_based_set_reconciliation). Hashes a `LengthyEntry` into the set `PreFingerprint`. */
  fingerprintSingleton(
    entry: LengthyEntry<NamespaceId, SubspaceId, PayloadDigest>,
  ): Promise<PreFingerprint>;
  /** Configures the [`fingerprint_combine`](https://willowprotocol.org/specs/3d-range-based-set-reconciliation/index.html#d3rbsr_fp_combine) variable for [3d range-based set reconciliation](https://willowprotocol.org/specs/3d-range-based-set-reconciliation/index.html#d3_range_based_set_reconciliation). Maps two `PreFingerprint` to a single new `PreFingerprint`. */
  fingerprintCombine(
    a: PreFingerprint,
    b: PreFingerprint,
  ): PreFingerprint;
  /** Configures the [`fingerprint_finalise`](https://willowprotocol.org/specs/3d-range-based-set-reconciliation/index.html#d3rbsr_fp_finalise) variable for [3d range-based set reconciliation](https://willowprotocol.org/specs/3d-range-based-set-reconciliation/index.html#d3_range_based_set_reconciliation). Maps a `PreFingerprint` to a corresponding `Fingerprint`. */
  fingerprintFinalise(
    prefingerprint: PreFingerprint,
  ): Promise<Fingerprint>;
  /** Configures the [`fingerprint_neutral`](https://willowprotocol.org/specs/3d-range-based-set-reconciliation/index.html#d3rbsr_neutral) variable for for [3d range-based set reconciliation](https://willowprotocol.org/specs/3d-range-based-set-reconciliation/index.html#d3_range_based_set_reconciliation). A [neutral element](https://en.wikipedia.org/wiki/Identity_element) for `PreFingerprint`. */
  neutral: PreFingerprint;
  /** A [neutral element](https://en.wikipedia.org/wiki/Identity_element) for `Fingerprint`. */
  neutralFinalised: Fingerprint;
  /** Compare two `Fingerprint` for equality. */
  isEqual: (a: Fingerprint, b: Fingerprint) => boolean;
  /** An encoding scheme for `Fingerprint`. */
  encoding: EncodingScheme<Fingerprint>;
};

/** The parameter schemes required to instantiate a `Store`. */
export interface StoreSchemes<
  NamespaceId,
  SubspaceId,
  PayloadDigest,
  AuthorisationOpts,
  AuthorisationToken,
  Prefingerprint,
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
    Prefingerprint,
    Fingerprint
  >;
}

export type StoreOpts<
  NamespaceId,
  SubspaceId,
  PayloadDigest,
  AuthorisationOpts,
  AuthorisationToken,
  Prefingerprint,
  Fingerprint,
> = {
  /** The `NamespaceId` of the namespace this `Store` holds entries for. */
  namespace: NamespaceId;
  /** The parameter schemes this store should use. */
  schemes: StoreSchemes<
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    AuthorisationOpts,
    AuthorisationToken,
    Prefingerprint,
    Fingerprint
  >;
  /** An optional driver used to store and retrieve a store's entries. */
  entryDriver?: EntryDriver<
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    Prefingerprint
  >;
  /** An optional driver used to store and retrieve a store's payloads.  */
  payloadDriver?: PayloadDriver<PayloadDigest>;
};

/** Compare two `ValueType` for equality. */
export type EqualityFn<ValueType> = (a: ValueType, b: ValueType) => boolean;

// Events

/** Emitted after an entry fails to be ingested by a store, either due to error or entry invalidity. */
export type IngestEventFailure = {
  kind: "failure";
  reason: "write_failure" | "invalid_entry";
  message: string;
  err: Error | null;
};

/** Emitted after an entry is not ingested by a store due to it being obsoleted by a more recent entry. */
export type IngestEventNoOp = {
  kind: "no_op";
  reason: "obsolete_from_same_subspace" | "newer_prefix_found";
};

/** Emitted after a successuful entry ingestion. */
export type IngestEventSuccess<
  NamespacePublicKey,
  SubspacePublicKey,
  PayloadDigest,
  AuthorisationToken,
> = {
  kind: "success";
  /** The successfully ingested entry. */
  entry: Entry<NamespacePublicKey, SubspacePublicKey, PayloadDigest>;
  /** Entries which were pruned by this ingestion via [prefix pruning](https://willowprotocol.org/specs/data-model/index.html#prefix_pruning). */
  pruned: Entry<NamespacePublicKey, SubspacePublicKey, PayloadDigest>[];
  /** The `AuthorisationToken` generated for this entry. */
  authToken: AuthorisationToken;
  /** An ID representing the source of this ingested entry. */
  externalSourceId?: string;
};

/** THe order in which to return entries. */
export type QueryOrder =
  /** By path, then timestamp, then subspace */
  | "path"
  /** By timestamp, then subspace, then path */
  | "timestamp"
  /** By subspace, then path, then timestamp */
  | "subspace";

/** Emitted after entry ingestion. */
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
  bytes: (
    /** An optional offset (in bytes) from which to read the payload. */
    offset?: number,
  ) => Promise<Uint8Array>;
  /** A {@link AsyncIterable} of the payload's available data which can be read chunk by chunk. */
  stream: (
    /** An optional offset (in bytes) from which to read the payload. */
    offset?: number,
  ) => Promise<AsyncIterable<Uint8Array>>;
  /** The length in bytes of the available payload. */
  length: () => Promise<bigint>;
};

export type EntryInput<SubspacePublicKey> = {
  /** The desired [`Path`](https://willowprotocol.org/specs/data-model/index.html#Path) for the new entry. */
  path: Path;
  /** The desired [`SubspaceId`](https://willowprotocol.org/specs/data-model/index.html#SubspaceId) for the new entry. */
  subspace: SubspacePublicKey;
  /** Arbitrary data to be associated with the new entry. */
  payload: Uint8Array | AsyncIterable<Uint8Array>;
  /** The desired timestamp for the new entry. If left undefined, uses the current system time, OR if another entry exists at the same path will be that entry's timestamp + 1. */
  timestamp?: bigint;
};

/** Emitted after a payload fails to be ingested, either because there is no entry corresponding to it, or because the given payload did not match the entry's `PayloadDigest`. */
export type IngestPayloadEventFailure = {
  kind: "failure";
  reason: "no_entry" | "data_mismatch";
};

/** Emitted after a payload is not ingested because it is already held by the `Store`. */
export type IngestPayloadEventNoOp = {
  kind: "no_op";
  reason: "already_have_it";
};

/** Emitted after the succesful ingestion of a payload. */
export type IngestPayloadEventSuccess = {
  kind: "success";
};

/** Emitted after payload entry. */
export type IngestPayloadEvent =
  | IngestPayloadEventFailure
  | IngestPayloadEventNoOp
  | IngestPayloadEventSuccess;

/** Describes an entry and the amount of locally available payload bytes. https://willowprotocol.org/specs/3d-range-based-set-reconciliation/index.html#LengthyEntry */
export type LengthyEntry<NamespaceId, SubspaceId, PayloadDigest> = {
  entry: Entry<NamespaceId, SubspaceId, PayloadDigest>;
  available: bigint;
};
