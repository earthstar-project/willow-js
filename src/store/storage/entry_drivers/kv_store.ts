import {
  decodeEntry,
  encodeEntry,
  Entry,
  equalsBytes,
  PathScheme,
} from "../../../../deps.ts";
import {
  FingerprintScheme,
  NamespaceScheme,
  PayloadScheme,
  SubspaceScheme,
} from "../../types.ts";
import { PrefixedDriver } from "../kv/prefixed_driver.ts";
import { KvDriver } from "../kv/types.ts";
import { SimpleKeyIterator } from "../prefix_iterators/simple_key_iterator.ts";
import { PrefixIterator } from "../prefix_iterators/types.ts";
import { TripleStorage } from "../storage_3d/triple_storage.ts";
import { Storage3d } from "../storage_3d/types.ts";
import { LinearStorage } from "../summarisable_storage/linear_summarisable_storage.ts";
import { Skiplist } from "../summarisable_storage/monoid_skiplist.ts";
import { EntryDriver, PayloadReferenceCounter } from "../types.ts";

type EntryDriverKvOpts<
  NamespaceId,
  SubspaceId,
  PayloadDigest,
  Prefingerprint,
  Fingerprint,
> = {
  kvDriver: KvDriver;
  namespaceScheme: NamespaceScheme<NamespaceId>;
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

/** Store and retrieve entries in a key-value store. */
export class EntryDriverKvStore<
  NamespaceId,
  SubspaceId,
  PayloadDigest,
  Prefingerprint,
  Fingerprint,
> implements
  EntryDriver<
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    Prefingerprint
  > {
  private namespaceScheme: NamespaceScheme<NamespaceId>;
  private subspaceScheme: SubspaceScheme<SubspaceId>;
  private payloadScheme: PayloadScheme<PayloadDigest>;
  private pathScheme: PathScheme;
  private fingerprintScheme: FingerprintScheme<
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    Prefingerprint,
    Fingerprint
  >;

  private kvDriver: KvDriver;
  prefixIterator: PrefixIterator<Uint8Array>;

  private getPayloadLength: (digest: PayloadDigest) => Promise<bigint>;

  payloadReferenceCounter: PayloadReferenceCounter<PayloadDigest>;

  constructor(
    opts: EntryDriverKvOpts<
      NamespaceId,
      SubspaceId,
      PayloadDigest,
      Prefingerprint,
      Fingerprint
    >,
  ) {
    this.namespaceScheme = opts.namespaceScheme;
    this.subspaceScheme = opts.subspaceScheme;
    this.payloadScheme = opts.payloadScheme;
    this.pathScheme = opts.pathScheme;
    this.fingerprintScheme = opts.fingerprintScheme;

    this.kvDriver = opts.kvDriver;

    const prefixedKvDriver = new PrefixedDriver(["prefix"], this.kvDriver);

    this.prefixIterator = new SimpleKeyIterator<Uint8Array>(prefixedKvDriver);

    this.getPayloadLength = opts.getPayloadLength;

    const refKvDriver = new PrefixedDriver(["payloadRefCount"], this.kvDriver);

    this.payloadReferenceCounter = {
      count: async (digest) => {
        const encoded = this.payloadScheme.encode(digest);

        const count = await refKvDriver.get<number>([encoded]);

        return count || 0;
      },
      increment: async (digest) => {
        const encoded = this.payloadScheme.encode(digest);

        const count = await refKvDriver.get<number>([encoded]);

        const next = count ? count + 1 : 1;

        await refKvDriver.set([encoded], next);

        return Promise.resolve(next);
      },
      decrement: async (digest) => {
        const encoded = this.payloadScheme.encode(digest);

        const count = await refKvDriver.get<number>([encoded]);
        if (!count) {
          return Promise.resolve(0);
        }

        const next = count - 1;

        if (next === 0) {
          await refKvDriver.delete([encoded]);

          return Promise.resolve(0);
        }

        await refKvDriver.set([encoded], next);

        return Promise.resolve(next);
      },
    };
  }

  makeStorage(
    namespace: NamespaceId,
  ): Storage3d<NamespaceId, SubspaceId, PayloadDigest, Prefingerprint> {
    const prefixedStorageDriver = new PrefixedDriver(
      ["entries"],
      this.kvDriver,
    );

    return new TripleStorage({
      namespace,
      createSummarisableStorage: (
        monoid,
        id,
      ) => {
        return new LinearStorage({
          monoid,
          kv: new PrefixedDriver([id], prefixedStorageDriver),
          // logicalValueEq: equalsBytes,
        });
      },
      fingerprintScheme: this.fingerprintScheme,
      pathScheme: this.pathScheme,
      payloadScheme: this.payloadScheme,
      subspaceScheme: this.subspaceScheme,
      getPayloadLength: this.getPayloadLength,
    });
  }

  writeAheadFlag = {
    wasInserting: async () => {
      const maybeInsertion = await this.kvDriver.get<Uint8Array>([
        "waf",
        "insert",
      ]);

      if (!maybeInsertion) {
        return;
      }

      const probablyAuthTokenHash = await this.kvDriver.get<Uint8Array>([
        "waf",
        "insert",
        "authTokenHash",
      ]);

      if (!probablyAuthTokenHash) {
        console.warn(
          "Write ahead flag: an insertion was detected, but no corresponding auth token was found.",
        );

        return;
      }

      const entry = decodeEntry({
        namespaceScheme: this.namespaceScheme,
        subspaceScheme: this.subspaceScheme,
        payloadScheme: this.payloadScheme,
        pathScheme: this.pathScheme,
      }, maybeInsertion);

      const authTokenHash = this.payloadScheme.decode(probablyAuthTokenHash);

      return {
        entry,
        authTokenHash,
      };
    },
    wasRemoving: async () => {
      const maybeRemoval = await this.kvDriver.get<Uint8Array>([
        "waf",
        "remove",
      ]);

      if (!maybeRemoval) {
        return;
      }

      const entry = decodeEntry({
        namespaceScheme: this.namespaceScheme,
        subspaceScheme: this.subspaceScheme,
        payloadScheme: this.payloadScheme,
        pathScheme: this.pathScheme,
      }, maybeRemoval);

      return entry;
    },
    flagInsertion: async (
      entry: Entry<NamespaceId, SubspaceId, PayloadDigest>,
      authTokenHash: PayloadDigest,
    ) => {
      const entryEncoded = encodeEntry({
        namespaceScheme: this.namespaceScheme,
        subspaceScheme: this.subspaceScheme,
        pathScheme: this.pathScheme,
        payloadScheme: this.payloadScheme,
      }, entry);

      const authHashEncoded = this.payloadScheme.encode(authTokenHash);

      await this.kvDriver.set(
        ["waf", "insert"],
        entryEncoded,
      );

      await this.kvDriver.set(
        ["waf", "insert", "authTokenHash"],
        authHashEncoded,
      );
    },

    flagRemoval: (entry: Entry<NamespaceId, SubspaceId, PayloadDigest>) => {
      const entryEncoded = encodeEntry({
        namespaceScheme: this.namespaceScheme,
        subspaceScheme: this.subspaceScheme,
        pathScheme: this.pathScheme,
        payloadScheme: this.payloadScheme,
      }, entry);

      return this.kvDriver.set(["waf", "remove"], entryEncoded);
    },

    unflagInsertion: async () => {
      await this.kvDriver.delete(["waf", "insert"]);
      await this.kvDriver.delete(["waf", "insert", "authTokenHash"]);
    },
    unflagRemoval: async () => {
      await this.kvDriver.delete(["waf", "remove"]);
    },
  };
}
