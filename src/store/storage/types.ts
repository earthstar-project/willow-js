import type { Entry } from "@earthstar/willow-utils";
import type { ValidationError } from "../../errors.ts";
import type { Payload } from "../types.ts";
import type { PrefixIterator } from "./prefix_iterators/types.ts";
import type { Storage3d } from "./storage_3d/types.ts";

/** Writes and reads flags indicating write operations to the store, in order to recover from errors mid-write. */
export interface WriteAheadFlag<
  NamespaceId,
  SubspaceId,
  PayloadDigest,
> {
  wasInserting(): Promise<
    {
      entry: Entry<NamespaceId, SubspaceId, PayloadDigest>;
      authTokenHash: PayloadDigest;
    } | undefined
  >;
  wasRemoving(): Promise<
    Entry<NamespaceId, SubspaceId, PayloadDigest> | undefined
  >;
  flagInsertion(
    entry: Entry<NamespaceId, SubspaceId, PayloadDigest>,
    authTokenHash: PayloadDigest,
  ): Promise<void>;
  unflagInsertion: () => Promise<void>;
  flagRemoval(
    entry: Entry<NamespaceId, SubspaceId, PayloadDigest>,
  ): Promise<void>;
  unflagRemoval(): Promise<void>;
}

/** Provides methods for storing and retrieving entries for a {@link Store}. */
export interface EntryDriver<
  NamespaceId,
  SubspaceId,
  PayloadDigest,
  Prefingerprint,
> {
  /** Produce a new {@link Storage3d} to be directly used by the {@link Store}. */
  makeStorage: (namespace: NamespaceId) => Storage3d<
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    Prefingerprint
  >;
  /** Helps a Store recover from unexpected shutdowns mid-write. */
  writeAheadFlag: WriteAheadFlag<
    NamespaceId,
    SubspaceId,
    PayloadDigest
  >;
  /** Used to find paths that are prefixes of, or prefixed by, another path. */
  prefixIterator: PrefixIterator<Uint8Array>;
  /** Used to keep track of how many entries are referring to a single payload  */
  payloadReferenceCounter: PayloadReferenceCounter<PayloadDigest>;
}

/** Keeps count of how many entries refer to a given payload, and is ablo to modify that count. */
export interface PayloadReferenceCounter<PayloadDigest> {
  increment(digest: PayloadDigest): Promise<number>;
  decrement(digest: PayloadDigest): Promise<number>;
  count(digest: PayloadDigest): Promise<number>;
}

/** Provides methods for storing and retrieving {@link Payload}s. */
export interface PayloadDriver<PayloadDigest> {
  /** Returns a payload for a given format and hash.*/
  get(
    payloadHash: PayloadDigest,
  ): Promise<Payload | undefined>;

  /** Stores a complete payload with an unknown digest, intended for a new entry. */
  set(
    payload: Uint8Array | AsyncIterable<Uint8Array>,
  ): Promise<{
    digest: PayloadDigest;
    length: bigint;
    payload: Payload;
  }>;

  /** Stores a possibly partial payload with an known digest, intended for an existing entry. */
  receive(
    opts: {
      payload: Uint8Array | AsyncIterable<Uint8Array>;
      offset: number;
      expectedLength: bigint;
      expectedDigest: PayloadDigest;
    },
  ): Promise<
    {
      /** The digest after ingestion */
      digest: PayloadDigest;
      /** the length after ingestion */
      length: bigint;
      /** Commits the payload, saving it. */
      commit: (isCompletePayload: boolean) => Promise<void>;
      /** Rejects the payload, deleting it. */
      reject: () => Promise<void>;
    }
  >;

  /** Get the length of a stored payload */
  length(
    payloadHash: PayloadDigest,
  ): Promise<bigint>;

  /** Erases an payload for a given format and hash.*/
  erase(
    digest: PayloadDigest,
  ): Promise<true | ValidationError>;
}
