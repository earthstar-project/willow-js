import { decodeEntry, encodeEntry } from "../../../entries/encode_decode.ts";
import { Entry } from "../../../entries/types.ts";
import { compareBytes } from "../../../util/bytes.ts";
import {
  FingerprintScheme,
  NamespaceScheme,
  PathLengthScheme,
  PayloadScheme,
  SubspaceScheme,
} from "../../types.ts";
import { KvDriver } from "../kv/types.ts";
import { KeyHopTree } from "../prefix_iterators/key_hop_tree.ts";
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
  pathLengthScheme: PathLengthScheme;
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
  private pathLengthScheme: PathLengthScheme;
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
    this.pathLengthScheme = opts.pathLengthScheme;
    this.fingerprintScheme = opts.fingerprintScheme;

    this.kvDriver = opts.kvDriver;
    this.prefixIterator = new KeyHopTree<Uint8Array>(this.kvDriver);
  }

  makeStorage(
    namespace: NamespaceKey,
  ): Storage3d<NamespaceKey, SubspaceKey, PayloadDigest, Fingerprint> {
    return new TripleStorage({
      namespace,
      createSummarisableStorage: (
        monoid: LiftingMonoid<Uint8Array, Fingerprint>,
      ) => {
        return new Skiplist({
          kv: this.kvDriver,
          monoid,
          compare: compareBytes,
        });
      },
      fingerprintScheme: this.fingerprintScheme,
      pathLengthScheme: this.pathLengthScheme,
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

      const entry = decodeEntry(maybeInsertion, {
        namespaceScheme: this.namespaceScheme,
        subspaceScheme: this.subspaceScheme,
        payloadScheme: this.payloadScheme,
        pathLengthScheme: this.pathLengthScheme,
      });

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

      const entry = decodeEntry(maybeRemoval, {
        namespaceScheme: this.namespaceScheme,
        subspaceScheme: this.subspaceScheme,
        payloadScheme: this.payloadScheme,
        pathLengthScheme: this.pathLengthScheme,
      });

      return entry;
    },
    flagInsertion: async (
      entry: Entry<NamespaceKey, SubspaceKey, PayloadDigest>,
      authTokenHash: PayloadDigest,
    ) => {
      const entryEncoded = encodeEntry(entry, {
        namespaceScheme: this.namespaceScheme,
        subspaceScheme: this.subspaceScheme,
        pathLengthScheme: this.pathLengthScheme,
        payloadScheme: this.payloadScheme,
      });

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
      const entryEncoded = encodeEntry(entry, {
        namespaceScheme: this.namespaceScheme,
        subspaceScheme: this.subspaceScheme,
        pathLengthScheme: this.pathLengthScheme,
        payloadScheme: this.payloadScheme,
      });

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
