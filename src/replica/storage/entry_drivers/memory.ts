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
  NamespaceKey,
  SubspaceKey,
  PayloadDigest,
  Fingerprint,
> = {
  subspaceScheme: SubspaceScheme<SubspaceKey>;
  payloadScheme: PayloadScheme<PayloadDigest>;
  pathScheme: PathScheme;
  fingerprintScheme: FingerprintScheme<
    NamespaceKey,
    SubspaceKey,
    PayloadDigest,
    Fingerprint
  >;
};

/** Store and retrieve entries in memory. */
export class EntryDriverMemory<
  NamespaceKey,
  SubspaceKey,
  PayloadDigest,
  Fingerprint,
> implements
  EntryDriver<NamespaceKey, SubspaceKey, PayloadDigest, Fingerprint> {
  constructor(
    readonly opts: EntryDriverMemoryOpts<
      NamespaceKey,
      SubspaceKey,
      PayloadDigest,
      Fingerprint
    >,
  ) {}

  private wafInsert:
    | [Entry<NamespaceKey, SubspaceKey, PayloadDigest>, PayloadDigest]
    | undefined;
  private wafRemove:
    | Entry<NamespaceKey, SubspaceKey, PayloadDigest>
    | undefined;

  makeStorage(
    namespace: NamespaceKey,
  ): Storage3d<NamespaceKey, SubspaceKey, PayloadDigest, Fingerprint> {
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
      entry: Entry<NamespaceKey, SubspaceKey, PayloadDigest>,
      authTokenHash: PayloadDigest,
    ) => {
      this.wafInsert = [entry, authTokenHash];

      return Promise.resolve();
    },
    flagRemoval: (entry: Entry<NamespaceKey, SubspaceKey, PayloadDigest>) => {
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
