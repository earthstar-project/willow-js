import {
  FingerprintScheme,
  PayloadScheme,
  SubspaceScheme,
} from "../../types.ts";
import { LiftingMonoid } from "../summarisable_storage/lifting_monoid.ts";
import { KvDriverInMemory } from "../kv/kv_driver_in_memory.ts";
import { EntryDriver } from "../types.ts";
import { Storage3d } from "../storage_3d/types.ts";
import { TripleStorage } from "../storage_3d/triple_storage.ts";
import { encodeBase64, Entry, PathScheme } from "../../../../deps.ts";
import { RadixTree } from "../prefix_iterators/radix_tree.ts";
import { KvKey } from "../kv/types.ts";
import { LinearStorage } from "../summarisable_storage/linear_summarisable_storage.ts";

type EntryDriverMemoryOpts<
  NamespaceId,
  SubspaceId,
  PayloadDigest,
  Prefingerprint,
  Fingerprint,
> = {
  subspaceScheme: SubspaceScheme<SubspaceId>;
  payloadScheme: PayloadScheme<PayloadDigest>;
  pathScheme: PathScheme;
  fingerprintScheme: FingerprintScheme<
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    Prefingerprint,
    Fingerprint
  >;
  getPayloadLength: (digest: PayloadDigest) => Promise<bigint>;
};

/** Store and retrieve entries in memory. */
export class EntryDriverMemory<
  NamespaceId,
  SubspaceId,
  PayloadDigest,
  Prefingerprint,
  Fingerprint,
> implements
  EntryDriver<NamespaceId, SubspaceId, PayloadDigest, Prefingerprint> {
  constructor(
    readonly opts: EntryDriverMemoryOpts<
      NamespaceId,
      SubspaceId,
      PayloadDigest,
      Prefingerprint,
      Fingerprint
    >,
  ) {}

  private wafInsert:
    | [Entry<NamespaceId, SubspaceId, PayloadDigest>, PayloadDigest]
    | undefined;
  private wafRemove:
    | Entry<NamespaceId, SubspaceId, PayloadDigest>
    | undefined;

  private payloadRefCounts = new Map<string, number>();

  makeStorage(
    namespace: NamespaceId,
  ): Storage3d<NamespaceId, SubspaceId, PayloadDigest, Prefingerprint> {
    return new TripleStorage({
      namespace,
      createSummarisableStorage: (
        monoid: LiftingMonoid<[KvKey, Uint8Array], Prefingerprint>,
      ) => {
        return new LinearStorage({
          monoid,
          kv: new KvDriverInMemory(),
        });
      },
      fingerprintScheme: this.opts.fingerprintScheme,
      pathScheme: this.opts.pathScheme,
      payloadScheme: this.opts.payloadScheme,
      subspaceScheme: this.opts.subspaceScheme,
      getPayloadLength: this.opts.getPayloadLength,
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
  payloadReferenceCounter = {
    count: (digest: PayloadDigest) => {
      const encoded = this.opts.payloadScheme.encode(digest);

      const b64 = encodeBase64(encoded);

      const result = this.payloadRefCounts.get(b64);

      return Promise.resolve(result || 0);
    },
    increment: (digest: PayloadDigest) => {
      const encoded = this.opts.payloadScheme.encode(digest);

      const b64 = encodeBase64(encoded);

      const result = this.payloadRefCounts.get(b64);

      const next = result ? result + 1 : 1;

      this.payloadRefCounts.set(b64, next);

      return Promise.resolve(next);
    },
    decrement: (digest: PayloadDigest) => {
      const encoded = this.opts.payloadScheme.encode(digest);

      const b64 = encodeBase64(encoded);

      const result = this.payloadRefCounts.get(b64);

      if (!result) {
        return Promise.resolve(0);
      }

      const next = result - 1;

      if (next === 0) {
        this.payloadRefCounts.delete(b64);

        return Promise.resolve(0);
      }

      this.payloadRefCounts.set(b64, next);

      return Promise.resolve(next);
    },
  };
}
