import { Entry } from "../../../deps.ts";
import { ValidationError } from "../../errors.ts";
import { Payload } from "../types.ts";

import { PrefixIterator } from "./prefix_iterators/types.ts";
import { Storage3d } from "./storage_3d/types.ts";

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
  Fingerprint,
> {
  makeStorage: (namespace: NamespaceId) => Storage3d<
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    Fingerprint
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

export interface PayloadReferenceCounter<PayloadDigest> {
  increment(digest: PayloadDigest): Promise<number>;
  decrement(digest: PayloadDigest): Promise<number>;
  count(digest: PayloadDigest): Promise<number>;
}

/**  */
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
      knownLength: bigint;
      knownDigest: PayloadDigest;
    },
  ): Promise<
    {
      /** The digest after ingestion */
      digest: PayloadDigest;
      /** the length after ingestion */
      length: bigint;
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
