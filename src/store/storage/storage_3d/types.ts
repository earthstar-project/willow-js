import { AreaOfInterest, Entry, Path, Range3d } from "../../../../deps.ts";
import { QueryOrder } from "../../types.ts";

export type RangeOfInterest<SubspaceId> = {
  range: Range3d<SubspaceId>;
  maxCount: number;
  maxSize: bigint;
};

export interface Storage3d<
  NamespaceId,
  SubspaceId,
  PayloadDigest,
  Fingerprint,
> {
  /** Retrieve a value */
  get(
    subspace: SubspaceId,
    path: Path,
  ): Promise<
    {
      entry: Entry<NamespaceId, SubspaceId, PayloadDigest>;
      authTokenHash: PayloadDigest;
    } | undefined
  >;

  insert(opts: {
    path: Path;
    subspace: SubspaceId;
    payloadDigest: PayloadDigest;
    timestamp: bigint;
    length: bigint;
    authTokenDigest: PayloadDigest;
  }): Promise<void>;

  updateAvailablePayload(
    subspace: SubspaceId,
    path: Path,
  ): Promise<boolean>;

  remove(
    entry: Entry<NamespaceId, SubspaceId, PayloadDigest>,
  ): Promise<boolean>;

  // Used during sync
  summarise(
    range: Range3d<SubspaceId>,
  ): Promise<{ fingerprint: Fingerprint; size: number }>;

  splitRange(
    range: Range3d<SubspaceId>,
    knownSize: number,
  ): Promise<[Range3d<SubspaceId>, Range3d<SubspaceId>]>;

  removeInterest(
    areaOfInterest: AreaOfInterest<SubspaceId>,
  ): Promise<Range3d<SubspaceId>>;

  // Used to fetch entries for transfer during sync.
  // All three dimensions are defined
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
