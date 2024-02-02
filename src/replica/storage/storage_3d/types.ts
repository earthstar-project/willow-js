import { AreaOfInterest, Entry, Path } from "../../../../deps.ts";
import { QueryOrder } from "../../types.ts";

export interface Storage3d<
  NamespaceKey,
  SubspaceKey,
  PayloadDigest,
  Fingerprint,
> {
  /** Retrieve a value */
  get(
    subspace: SubspaceKey,
    path: Path,
  ): Promise<
    {
      entry: Entry<NamespaceKey, SubspaceKey, PayloadDigest>;
      authTokenHash: PayloadDigest;
    } | undefined
  >;

  insert(opts: {
    path: Path;
    subspace: SubspaceKey;
    payloadDigest: PayloadDigest;
    timestamp: bigint;
    length: bigint;
    authTokenDigest: PayloadDigest;
  }): Promise<void>;

  remove(
    entry: Entry<NamespaceKey, SubspaceKey, PayloadDigest>,
  ): Promise<boolean>;

  // Used during sync
  summarise(
    areaOfInterest: AreaOfInterest<SubspaceKey>,
  ): Promise<{ fingerprint: Fingerprint; size: number }>;

  // Used to fetch entries for transfer during sync.
  // All three dimensions are defined
  query(
    areaOfInterest: AreaOfInterest<SubspaceKey>,
    order: QueryOrder,
    reverse?: boolean,
  ): AsyncIterable<
    {
      entry: Entry<NamespaceKey, SubspaceKey, PayloadDigest>;
      authTokenHash: PayloadDigest;
    }
  >;
}
