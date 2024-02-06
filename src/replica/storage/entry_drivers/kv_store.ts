import {
  decodeEntry,
  encodeEntry,
  Entry,
  orderBytes,
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
import { LiftingMonoid } from "../summarisable_storage/lifting_monoid.ts";
import { Skiplist } from "../summarisable_storage/monoid_skiplist.ts";
import { EntryDriver } from "../types.ts";

type EntryDriverKvOpts<
  NamespaceKey,
  SubspaceKey,
  PayloadDigest,
  Fingerprint,
> = {
  kvDriver: KvDriver;
  namespaceScheme: NamespaceScheme<NamespaceKey>;
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

/** Store and retrieve entries in a key-value store. */
export class EntryDriverKvStore<
  NamespaceKey,
  SubspaceKey,
  PayloadDigest,
  Fingerprint,
> implements
  EntryDriver<
    NamespaceKey,
    SubspaceKey,
    PayloadDigest,
    Fingerprint
  > {
  private namespaceScheme: NamespaceScheme<NamespaceKey>;
  private subspaceScheme: SubspaceScheme<SubspaceKey>;
  private payloadScheme: PayloadScheme<PayloadDigest>;
  private pathScheme: PathScheme;
  private fingerprintScheme: FingerprintScheme<
    NamespaceKey,
    SubspaceKey,
    PayloadDigest,
    Fingerprint
  >;

  private kvDriver: KvDriver;
  prefixIterator: PrefixIterator<Uint8Array>;

  constructor(
    opts: EntryDriverKvOpts<
      NamespaceKey,
      SubspaceKey,
      PayloadDigest,
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
  }

  makeStorage(
    namespace: NamespaceKey,
  ): Storage3d<NamespaceKey, SubspaceKey, PayloadDigest, Fingerprint> {
    const prefixedStorageDriver = new PrefixedDriver(
      ["entries"],
      this.kvDriver,
    );

    return new TripleStorage({
      namespace,
      createSummarisableStorage: (
        monoid: LiftingMonoid<Uint8Array, Fingerprint>,
      ) => {
        return new Skiplist({
          kv: prefixedStorageDriver,
          monoid,
          compare: orderBytes,
        });
      },
      fingerprintScheme: this.fingerprintScheme,
      pathScheme: this.pathScheme,
      payloadScheme: this.payloadScheme,
      subspaceScheme: this.subspaceScheme,
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
      entry: Entry<NamespaceKey, SubspaceKey, PayloadDigest>,
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

    flagRemoval: (entry: Entry<NamespaceKey, SubspaceKey, PayloadDigest>) => {
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
    unflagRemoval: () => {
      return this.kvDriver.delete(["waf", "remove"]);
    },
  };
}
