import { Products } from "../../../../deps.ts";
import { Entry } from "../../../entries/types.ts";
import { Query } from "../../types.ts";

export interface Storage3d<
  NamespaceKey,
  SubspaceKey,
  PayloadDigest,
  Fingerprint,
> {
  /** Retrieve a value */
  get(
    subspace: SubspaceKey,
    path: Uint8Array,
  ): Promise<
    {
      entry: Entry<NamespaceKey, SubspaceKey, PayloadDigest>;
      authTokenHash: PayloadDigest;
    } | undefined
  >;

  insert(opts: {
    path: Uint8Array;
    subspace: SubspaceKey;
    payloadHash: PayloadDigest;
    timestamp: bigint;
    length: bigint;
    authTokenHash: PayloadDigest;
  }): Promise<void>;

  remove(
    entry: Entry<NamespaceKey, SubspaceKey, PayloadDigest>,
  ): Promise<boolean>;

  // Used during sync
  summarise(
    product: Products.CanonicProduct<SubspaceKey>,
    countLimits?: { subspace?: number; path?: number; time?: number },
    sizeLimits?: { subspace?: bigint; path?: bigint; time?: bigint },
  ): Promise<{ fingerprint: Fingerprint; size: number }>;

  // Used to fetch entries for transfer during sync.
  // All three dimensions are defined
  entriesByProduct(
    product: Products.CanonicProduct<SubspaceKey>,
    countLimit?: number,
    sizeLimit?: bigint,
  ): AsyncIterable<
    {
      entry: Entry<NamespaceKey, SubspaceKey, PayloadDigest>;
      authTokenHash: PayloadDigest;
    }
  >;

  // Used to fetch entries when user is making query through replica
  // 0 - 3 dimensions may be defined
  entriesByQuery(
    query: Query<SubspaceKey>,
  ): AsyncIterable<
    {
      entry: Entry<NamespaceKey, SubspaceKey, PayloadDigest>;
      authTokenHash: PayloadDigest;
    }
  >;
}
