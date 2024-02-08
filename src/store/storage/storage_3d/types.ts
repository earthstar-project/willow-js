import { AreaOfInterest, Entry, Path } from "../../../../deps.ts";
import { QueryOrder } from "../../types.ts";

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

  remove(
    entry: Entry<NamespaceId, SubspaceId, PayloadDigest>,
  ): Promise<boolean>;

  // Used during sync
  summarise(
    areaOfInterest: AreaOfInterest<SubspaceId>,
  ): Promise<{ fingerprint: Fingerprint; size: number }>;

  // Used to fetch entries for transfer during sync.
  // All three dimensions are defined
  query(
    areaOfInterest: AreaOfInterest<SubspaceId>,
    order: QueryOrder,
    reverse?: boolean,
  ): AsyncIterable<
    {
      entry: Entry<NamespaceId, SubspaceId, PayloadDigest>;
      authTokenHash: PayloadDigest;
    }
  >;
}
