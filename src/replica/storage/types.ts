import { Entry } from "../../../deps.ts";
import { ValidationError } from "../../errors.ts";
import { Payload } from "../types.ts";

import { PrefixIterator } from "./prefix_iterators/types.ts";
import { Storage3d } from "./storage_3d/types.ts";

export interface WriteAheadFlag<
  NamespaceKey,
  SubspaceKey,
  PayloadDigest,
> {
  wasInserting(): Promise<
    {
      entry: Entry<NamespaceKey, SubspaceKey, PayloadDigest>;
      authTokenHash: PayloadDigest;
    } | undefined
  >;
  wasRemoving(): Promise<
    Entry<NamespaceKey, SubspaceKey, PayloadDigest> | undefined
  >;
  flagInsertion(
    entry: Entry<NamespaceKey, SubspaceKey, PayloadDigest>,
    authTokenHash: PayloadDigest,
  ): Promise<void>;
  unflagInsertion: () => Promise<void>;
  flagRemoval(
    entry: Entry<NamespaceKey, SubspaceKey, PayloadDigest>,
  ): Promise<void>;
  unflagRemoval(): Promise<void>;
}

/** Provides methods for storing and retrieving entries for a {@link Replica}. */
export interface EntryDriver<
  NamespaceKey,
  SubspaceKey,
  PayloadDigest,
  Fingerprint,
> {
  makeStorage: (namespace: NamespaceKey) => Storage3d<
    NamespaceKey,
    SubspaceKey,
    PayloadDigest,
    Fingerprint
  >;
  /** Helps a Replica recover from unexpected shutdowns mid-write. */
  writeAheadFlag: WriteAheadFlag<
    NamespaceKey,
    SubspaceKey,
    PayloadDigest
  >;
  /** Used to find paths that are prefixes of, or prefixed by, another path. */
  prefixIterator: PrefixIterator<Uint8Array>;
}

/**  */
export interface PayloadDriver<PayloadDigest> {
  /** Returns a payload for a given format and hash.*/
  get(
    payloadHash: PayloadDigest,
    opts?: {
      startOffset?: number;
    },
  ): Promise<Payload | undefined>;

  /** Stores the payload in a staging area, and returns an object used to assess whether it is what we're expecting, and if so, commit it to canonical storage. */
  stage(
    payload: Uint8Array | ReadableStream<Uint8Array>,
  ): Promise<
    {
      hash: PayloadDigest;
      length: bigint;
      /** Commit the staged attachment to storage. */
      commit: () => Promise<Payload>;
      /** Reject the staged attachment, erasing it. */
      reject: () => Promise<void>;
    }
  >;

  /** Erases an payload for a given format and hash.*/
  erase(
    hash: PayloadDigest,
  ): Promise<true | ValidationError>;
}
