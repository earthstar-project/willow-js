import {
  FingerprintScheme,
  PayloadScheme,
  SubspaceScheme,
} from "../../types.ts";
import { LiftingMonoid } from "../summarisable_storage/lifting_monoid.ts";
import { MonoidRbTree } from "../summarisable_storage/monoid_rbtree.ts";
import { EntryDriver } from "../types.ts";
import { Storage3d } from "../storage_3d/types.ts";
import { TripleStorage } from "../storage_3d/triple_storage.ts";
import { Entry, orderBytes, PathScheme } from "../../../../deps.ts";
import { RadixTree } from "../prefix_iterators/radix_tree.ts";

type EntryDriverMemoryOpts<
  NamespaceId,
  SubspaceId,
  PayloadDigest,
  Fingerprint,
> = {
  subspaceScheme: SubspaceScheme<SubspaceId>;
  payloadScheme: PayloadScheme<PayloadDigest>;
  pathScheme: PathScheme;
  fingerprintScheme: FingerprintScheme<
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    Fingerprint
  >;
};

/** Store and retrieve entries in memory. */
export class EntryDriverMemory<
  NamespaceId,
  SubspaceId,
  PayloadDigest,
  Fingerprint,
> implements EntryDriver<NamespaceId, SubspaceId, PayloadDigest, Fingerprint> {
  constructor(
    readonly opts: EntryDriverMemoryOpts<
      NamespaceId,
      SubspaceId,
      PayloadDigest,
      Fingerprint
    >,
  ) {}

  private wafInsert:
    | [Entry<NamespaceId, SubspaceId, PayloadDigest>, PayloadDigest]
    | undefined;
  private wafRemove:
    | Entry<NamespaceId, SubspaceId, PayloadDigest>
    | undefined;

  makeStorage(
    namespace: NamespaceId,
  ): Storage3d<NamespaceId, SubspaceId, PayloadDigest, Fingerprint> {
    return new TripleStorage({
      namespace,
      createSummarisableStorage: (
        monoid: LiftingMonoid<Uint8Array, Fingerprint>,
      ) => {
        return new MonoidRbTree({
          monoid,
          compare: orderBytes,
        });
      },
      fingerprintScheme: this.opts.fingerprintScheme,
      pathScheme: this.opts.pathScheme,
      payloadScheme: this.opts.payloadScheme,
      subspaceScheme: this.opts.subspaceScheme,
    });
  }
  writeAheadFlag = {
    wasInserting: () => {
      if (!this.wafInsert) {
        return Promise.resolve(undefined);
      }

      const [entry, authTokenHash] = this.wafInsert;

      return Promise.resolve({ entry, authTokenHash });
    },
    wasRemoving: () => {
      return Promise.resolve(this.wafRemove);
    },
    flagInsertion: (
      entry: Entry<NamespaceId, SubspaceId, PayloadDigest>,
      authTokenHash: PayloadDigest,
    ) => {
      this.wafInsert = [entry, authTokenHash];

      return Promise.resolve();
    },
    flagRemoval: (entry: Entry<NamespaceId, SubspaceId, PayloadDigest>) => {
      this.wafRemove = entry;

      return Promise.resolve();
    },
    unflagInsertion: () => {
      this.wafInsert = undefined;
      return Promise.resolve();
    },
    unflagRemoval: () => {
      this.wafRemove = undefined;
      return Promise.resolve();
    },
  };
  prefixIterator = new RadixTree<Uint8Array>();
}
