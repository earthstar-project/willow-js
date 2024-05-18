import type { AreaOfInterest, Entry, Path, Range3d } from "@earthstar/willow-utils";
import type { QueryOrder } from "../../types.ts";

/** A type exclusive to this implementation, used to make our lives easier. */
export type RangeOfInterest<SubspaceId> = {
  range: Range3d<SubspaceId>;
  maxCount: number;
  maxSize: bigint;
};

/** Low-level driver for writing and reading Entries in a three dimensional space. */
export interface Storage3d<
  NamespaceId,
  SubspaceId,
  PayloadDigest,
  PreFingerprint,
> {
  /** Retrieve an entry at a subspace and path. */
  get(
    subspace: SubspaceId,
    path: Path,
  ): Promise<
    {
      entry: Entry<NamespaceId, SubspaceId, PayloadDigest>;
      authTokenHash: PayloadDigest;
    } | undefined
  >;

  /** Insert a new entry. */
  insert(opts: {
    path: Path;
    subspace: SubspaceId;
    payloadDigest: PayloadDigest;
    timestamp: bigint;
    length: bigint;
    authTokenDigest: PayloadDigest;
  }): Promise<void>;

  /** Update the available payload bytes for a given entry. */
  updateAvailablePayload(
    subspace: SubspaceId,
    path: Path,
  ): Promise<boolean>;

  /** Remove an entry. */
  remove(
    entry: Entry<NamespaceId, SubspaceId, PayloadDigest>,
  ): Promise<boolean>;

  // Used during sync.

  /** Summarise a given `Range3d` by mapping the included set of `Entry` to ` PreFingerprint`.  */
  summarise(
    range: Range3d<SubspaceId>,
  ): Promise<{ fingerprint: PreFingerprint; size: number }>;

  /** Split a range into two smaller ranges. */
  splitRange(
    range: Range3d<SubspaceId>,
    knownSize: number,
  ): Promise<[Range3d<SubspaceId>, Range3d<SubspaceId>]>;

  /** Return the smallest `Range3d` which includes all entries included by a given `AreaOfInterest`. */
  removeInterest(
    areaOfInterest: AreaOfInterest<SubspaceId>,
  ): Promise<Range3d<SubspaceId>>;

  /** Return an async iterator of entries included by a `RangeOfInterest`. */
  query(
    rangeOfInterest: RangeOfInterest<SubspaceId>,
    order: QueryOrder,
    reverse?: boolean,
  ): AsyncIterable<
    {
      entry: Entry<NamespaceId, SubspaceId, PayloadDigest>;
      authTokenHash: PayloadDigest;
    }
  >;
}
