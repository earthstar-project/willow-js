import type { EntryDriver, PayloadDriver } from "./storage/types.ts";
import type {
  EntryInput,
  IngestEvent,
  IngestPayloadEvent,
  Payload,
  QueryOrder,
  StoreOpts,
  StoreSchemes,
} from "./types.ts";
import { PayloadDriverMemory } from "./storage/payload_drivers/memory.ts";
import {
  EntryIngestEvent,
  EntryPayloadSetEvent,
  EntryRemoveEvent,
  PayloadIngestEvent,
  PayloadRemoveEvent,
} from "./events.ts";
import type { Storage3d } from "./storage/storage_3d/types.ts";
import { WillowError } from "../errors.ts";
import Mutex from "./mutex.ts";
import { EntryDriverKvStore } from "./storage/entry_drivers/kv_store.ts";
import { KvDriverInMemory } from "./storage/kv/kv_driver_in_memory.ts";
import {
  type AreaOfInterest,
  areaTo3dRange,
  bigintToBytes,
  type Entry,
  OPEN_END,
  orderPath,
  type Path,
  type Range3d,
  successorPath,
  successorPrefix,
} from "@earthstar/willow-utils";

/** A local set of a particular namespace's authorised entries to be written to, read from, and synced with other `Store`s. Applies the concepts of the [Willow Data Model](https://willowprotocol.org/specs/data-model/index.html#data_model) to the set of entries stored inside.
 *
 * Keeps all data in memory unless persisted entry / payload drivers are specified.
 *
 * https://willowprotocol.org/specs/data-model/index.html#store
 */
export class Store<
  NamespaceId,
  SubspaceId,
  PayloadDigest,
  AuthorisationOpts,
  AuthorisationToken,
  Prefingerprint,
  Fingerprint,
> extends EventTarget {
  namespace: NamespaceId;

  private schemes: StoreSchemes<
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    AuthorisationOpts,
    AuthorisationToken,
    Prefingerprint,
    Fingerprint
  >;

  private entryDriver: EntryDriver<
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    Prefingerprint
  >;
  private payloadDriver: PayloadDriver<PayloadDigest>;

  private storage: Storage3d<
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    Prefingerprint
  >;

  private checkedWriteAheadFlag = Promise.withResolvers<void>();

  private ingestionMutex = new Mutex();

  constructor(
    opts: StoreOpts<
      NamespaceId,
      SubspaceId,
      PayloadDigest,
      AuthorisationOpts,
      AuthorisationToken,
      Prefingerprint,
      Fingerprint
    >,
  ) {
    super();

    this.namespace = opts.namespace;
    this.schemes = opts.schemes;

    const payloadDriver = opts.payloadDriver ||
      new PayloadDriverMemory(opts.schemes.payload);

    const entryDriver = opts.entryDriver || new EntryDriverKvStore({
      namespaceScheme: opts.schemes.namespace,
      pathScheme: opts.schemes.path,
      payloadScheme: opts.schemes.payload,
      subspaceScheme: opts.schemes.subspace,
      fingerprintScheme: opts.schemes.fingerprint,
      getPayloadLength: (digest) => {
        return this.payloadDriver.length(digest);
      },
      kvDriver: new KvDriverInMemory(),
    });

    this.entryDriver = entryDriver;

    this.storage = entryDriver.makeStorage(this.namespace);

    this.payloadDriver = payloadDriver;

    this.checkWriteAheadFlag();
  }

  private async checkWriteAheadFlag() {
    const existingInsert = await this.entryDriver.writeAheadFlag.wasInserting();
    const existingRemove = await this.entryDriver.writeAheadFlag.wasRemoving();

    if (existingInsert) {
      const encodedAuthToken = await this.payloadDriver.get(
        existingInsert.authTokenHash,
      );

      if (encodedAuthToken) {
        const decodedToken = this.schemes.authorisation
          .tokenEncoding.decode(await encodedAuthToken?.bytes());

        await this.insertEntry({
          path: existingInsert.entry.path,
          subspace: existingInsert.entry.subspaceId,
          hash: existingInsert.entry.payloadDigest,
          length: existingInsert.entry.payloadLength,
          timestamp: existingInsert.entry.timestamp,
          authToken: decodedToken,
        });
      }

      await this.entryDriver.writeAheadFlag.unflagInsertion();
    }

    if (existingRemove) {
      await this.storage.remove(existingRemove);

      // Unflag remove
      await this.entryDriver.writeAheadFlag.unflagRemoval();
    }

    this.checkedWriteAheadFlag.resolve();
  }

  /** Create a new authorised entry for a given payload, and store both in the store.
   * An entry will not be ingested if it is unauthorised; if a newer entry with the same path and subspace are present; or if a newer entry with a path that is a prefix of the given entry exists. See the Willow Data Model's [Concepts](https://willowprotocol.org/specs/data-model/index.html#data_model_concepts) for more information.
   *
   * Additionally, if the new entry's path is a prefix of already-held older entries, those entries will be removed from the `Store`. See [prefix pruning](https://willowprotocol.org/specs/data-model/index.html#prefix_pruning) for more information.
   */
  async set(
    /** Parameters for the new entry being set. */
    input: EntryInput<SubspaceId>,
    /** The `AuthorisationOpts` configured by `AuthorisationScheme` to produce a valid `AuthorisationToken`, e.g. a keypair for signing.  */
    authorisation: AuthorisationOpts,
  ): Promise<
    IngestEvent<NamespaceId, SubspaceId, PayloadDigest, AuthorisationToken>
  > {
    const timestamp = input.timestamp !== undefined
      ? input.timestamp
      : BigInt(Date.now() * 1000);

    // Stage it with the driver
    const { digest, payload, length } = await this.payloadDriver.set(
      input.payload,
    );

    const entry: Entry<NamespaceId, SubspaceId, PayloadDigest> = {
      namespaceId: this.namespace,
      subspaceId: input.subspace,
      path: input.path,
      timestamp: timestamp,
      payloadLength: length,
      payloadDigest: digest,
    };

    const authToken = await this.schemes.authorisation.authorise(
      entry,
      authorisation,
    );

    const ingestResult = await this.ingestEntry(entry, authToken);

    if (ingestResult.kind !== "success") {
      const count = await this.entryDriver.payloadReferenceCounter.count(
        digest,
      );

      if (count === 0) {
        await this.payloadDriver.erase(digest);
      }

      return ingestResult;
    }

    this.dispatchEvent(new EntryPayloadSetEvent(entry, authToken, payload));

    return ingestResult;
  }

  /** Attempt to store an authorised entry in the `Store`.
   *
   * An entry will not be ingested if it is unauthorised; if a newer entry with the same path and subspace are present; or if a newer entry with a path that is a prefix of the given entry exists. See the Willow Data Model's [Concepts](https://willowprotocol.org/specs/data-model/index.html#data_model_concepts) for more information.
   *
   * Additionally, if the entry's path is a prefix of already-held older entries, those entries will be removed from the `Store`. See [prefix pruning](https://willowprotocol.org/specs/data-model/index.html#prefix_pruning) for more information.
   */
  async ingestEntry(
    entry: Entry<NamespaceId, SubspaceId, PayloadDigest>,
    authorisation: AuthorisationToken,
    externalSourceId?: string,
  ): Promise<
    IngestEvent<
      NamespaceId,
      SubspaceId,
      PayloadDigest,
      AuthorisationToken
    >
  > {
    await this.checkedWriteAheadFlag.promise;

    const acquisitionId = await this.ingestionMutex.acquire();

    // Check if the entry belongs to this namespace.
    if (
      !this.schemes.namespace.isEqual(
        this.namespace,
        entry.namespaceId,
      )
    ) {
      this.ingestionMutex.release(acquisitionId);

      return {
        kind: "failure",
        reason: "invalid_entry",
        message: "Entry's namespace did not match store's namespace.",
        err: null,
      };
    }

    if (
      await this.schemes.authorisation.isAuthorisedWrite(
        entry,
        authorisation,
      ) === false
    ) {
      this.ingestionMutex.release(acquisitionId);

      return {
        kind: "failure",
        reason: "invalid_entry",
        message: "Authorisation token does not permit writing of entry",
        err: null,
      };
    }

    const subspacePath = [
      this.schemes.subspace.encode(entry.subspaceId),
      ...entry.path,
    ];

    // Check if we have any newer entries with this prefix.
    for await (
      const [_path, timestampBytes] of this.entryDriver.prefixIterator
        .prefixesOf(subspacePath)
    ) {
      const view = new DataView(timestampBytes.buffer);
      const prefixTimestamp = view.getBigUint64(0);

      if (prefixTimestamp >= entry.timestamp) {
        this.ingestionMutex.release(acquisitionId);

        return {
          kind: "no_op",
          reason: "newer_prefix_found",
        };
      }
    }

    // Check for collisions with stored entries

    for await (
      const { entry: otherEntry } of this
        .storage.query(
          {
            range: {
              pathRange: {
                start: entry.path,
                end: successorPrefix(entry.path, this.schemes.path) ||
                  OPEN_END,
              },
              subspaceRange: {
                start: entry.subspaceId,
                end: this.schemes.subspace.successor(
                  entry.subspaceId,
                ) || OPEN_END,
              },
              timeRange: {
                start: BigInt(0),
                end: OPEN_END,
              },
            },
            maxCount: 1,
            maxSize: BigInt(0),
          },
          "subspace",
        )
    ) {
      if (
        orderPath(
          entry.path,
          otherEntry.path,
        ) !== 0
      ) {
        break;
      }

      //  If there is something existing and the timestamp is greater than ours, we have a no-op.
      if (otherEntry.timestamp > entry.timestamp) {
        this.ingestionMutex.release(acquisitionId);

        return {
          kind: "no_op",
          reason: "obsolete_from_same_subspace",
        };
      }

      const payloadDigestOrder = this.schemes.payload.order(
        entry.payloadDigest,
        otherEntry.payloadDigest,
      );

      // If the timestamps are the same, and our hash is less, we have a no-op.
      if (
        otherEntry.timestamp === entry.timestamp &&
        payloadDigestOrder === -1
      ) {
        this.ingestionMutex.release(acquisitionId);

        return {
          kind: "no_op",
          reason: "obsolete_from_same_subspace",
        };
      }

      const otherPayloadLengthIsGreater =
        entry.payloadLength < otherEntry.payloadLength;

      // If the timestamps and hashes are the same, and the other payload's length is greater, we have a no-op.
      if (
        otherEntry.timestamp === entry.timestamp &&
        payloadDigestOrder === 0 && otherPayloadLengthIsGreater
      ) {
        this.ingestionMutex.release(acquisitionId);

        return {
          kind: "no_op",
          reason: "obsolete_from_same_subspace",
        };
      }

      await this.storage.remove(otherEntry);

      const toRemovePrefixPath = [
        this.schemes.subspace.encode(
          otherEntry.subspaceId,
        ),
        ...otherEntry.path,
      ];

      await this.entryDriver.prefixIterator.remove(
        toRemovePrefixPath,
      );

      this.dispatchEvent(
        new EntryRemoveEvent(otherEntry, { entry, authToken: authorisation }),
      );
    }

    const pruned = await this.insertEntry({
      path: entry.path,
      subspace: entry.subspaceId,
      hash: entry.payloadDigest,
      timestamp: entry.timestamp,
      length: entry.payloadLength,
      authToken: authorisation,
    });

    // Indicates that this ingestion is not being triggered by a local set,
    // so the payload will arrive separately.
    if (externalSourceId) {
      this.dispatchEvent(new EntryIngestEvent(entry, authorisation));
    }

    this.ingestionMutex.release(acquisitionId);

    return {
      kind: "success",
      entry: entry,
      pruned,
      authToken: authorisation,
      externalSourceId: externalSourceId,
    };
  }

  /** Return an array of `Entry` which _would_ be pruned by a given `Entry` were it to be ingested. Can be used to prevent potentially undesirable destructive operations. */
  async prunableEntries({
    path,
    subspace,
    timestamp,
  }: {
    path: Path;
    subspace: SubspaceId;
    timestamp: bigint;
  }): Promise<
    {
      entry: Entry<NamespaceId, SubspaceId, PayloadDigest>;
      authTokenHash: PayloadDigest;
    }[]
  > {
    const prefixKey = [
      this.schemes.subspace.encode(subspace),
      ...path,
    ];

    const prefixedIterator = this.entryDriver
      .prefixIterator
      .prefixedBy(
        prefixKey,
      );

    const prunableEntries = [];

    for await (
      const [subspacePathOfPrefixed, u64TimestampOfPrefixed] of prefixedIterator
    ) {
      const view = new DataView(u64TimestampOfPrefixed.buffer);
      const prefixTimestamp = view.getBigUint64(
        u64TimestampOfPrefixed.byteOffset,
      );

      const [subspacePrefixed, ...pathPrefixed] = subspacePathOfPrefixed;

      if (prefixTimestamp < timestamp) {
        const subspace = this.schemes.subspace.decode(
          subspacePrefixed,
        );

        const toDeleteResult = await this.storage.get(subspace, pathPrefixed);

        if (!toDeleteResult) {
          throw new WillowError(
            "Malformed storage: could not fetch entry stored in prefix index.",
          );
        }

        prunableEntries.push(toDeleteResult);
      }
    }

    return prunableEntries;
  }

  // Put a new entry into storage. Used by several methods in this class.
  private async insertEntry(
    {
      path,
      subspace,
      timestamp,
      hash,
      length,
      authToken,
    }: {
      path: Path;
      subspace: SubspaceId;
      timestamp: bigint;
      hash: PayloadDigest;
      length: bigint;
      authToken: AuthorisationToken;
    },
  ): Promise<Entry<NamespaceId, SubspaceId, PayloadDigest>[]> {
    const encodedToken = this.schemes.authorisation
      .tokenEncoding.encode(authToken);

    const { digest } = await this.payloadDriver.set(encodedToken);

    await this.entryDriver.writeAheadFlag.flagInsertion({
      namespaceId: this.namespace,
      subspaceId: subspace,
      path: path,
      payloadDigest: hash,
      payloadLength: length,
      timestamp,
    }, digest);

    const prefixKey = [
      this.schemes.subspace.encode(subspace),
      ...path,
    ];

    await Promise.all([
      this.storage.insert({
        payloadDigest: hash,
        authTokenDigest: digest,
        length,
        path,
        subspace,
        timestamp,
      }),
      this.entryDriver.prefixIterator.insert(
        prefixKey,
        bigintToBytes(timestamp),
      ),
      this.entryDriver.payloadReferenceCounter.increment(hash),
    ]);

    // And remove all prefixes with smaller timestamps.
    const prunableEntries = await this.prunableEntries({
      path,
      subspace,
      timestamp,
    });

    const prunedEntries = [];

    for (const { entry, authTokenHash } of prunableEntries) {
      await this.entryDriver.writeAheadFlag.flagRemoval(
        entry,
      );

      await Promise.all([
        this.storage.remove(entry),
        async () => {
          const count = await this.entryDriver.payloadReferenceCounter
            .decrement(entry.payloadDigest);

          if (count === 0) {
            await this.payloadDriver.erase(
              entry.payloadDigest,
            );
          }
        },
        ,
        this.entryDriver.prefixIterator.remove(
          [this.schemes.subspace.encode(entry.subspaceId), ...entry.path],
        ),
      ]);

      this.dispatchEvent(
        new PayloadRemoveEvent({ entry, authToken }),
      );

      await this.payloadDriver.erase(authTokenHash);

      await this.entryDriver.writeAheadFlag.unflagRemoval();

      this.dispatchEvent(
        new EntryRemoveEvent(entry, {
          entry: {
            namespaceId: this.namespace,
            subspaceId: subspace,
            path,
            payloadDigest: hash,
            payloadLength: length,
            timestamp,
          },
          authToken,
        }),
      );

      prunedEntries.push(entry);
    }

    await this.entryDriver.writeAheadFlag.unflagInsertion();

    return prunedEntries;
  }

  /** Attempt to store the corresponding payload for one of the store's entries.
   *
   * A payload will not be ingested if the given entry is not stored in the store; if the hash of the payload does not match the entry's; or if it is already held.
   *
   * @param entryDetails - The attributes of the entry corresponding to this payload.
   * @param payload - An {@linkcode AsyncIterable} of the bytes to be verified (and possibly ingested).
   * @param [allowPartial=false] - Whether to allow partial payloads. If enabled, does not reject if the ingested data is of a smaller length than the entry's. Defaults to `false`.
   * @param [offset=0] - The offset at which to begin writing the ingested data.
   */
  async ingestPayload(
    entryDetails: {
      path: Path;
      timestamp: bigint;
      subspace: SubspaceId;
    },
    payload: AsyncIterable<Uint8Array>,
    allowPartial = false,
    offset = 0,
  ): Promise<IngestPayloadEvent> {
    const getResult = await this.storage.get(
      entryDetails.subspace,
      entryDetails.path,
    );

    if (!getResult) {
      return {
        kind: "failure",
        reason: "no_entry",
      };
    }

    const { entry, authTokenHash } = getResult;

    const existingPayload = await this.payloadDriver.get(entry.payloadDigest);

    if (existingPayload) {
      return {
        kind: "no_op",
        reason: "already_have_it",
      };
    }

    const result = await this.payloadDriver.receive({
      payload: payload,
      offset,
      expectedDigest: entry.payloadDigest,
      expectedLength: entry.payloadLength,
    });

    if (
      (result.length > entry.payloadLength) ||
      (allowPartial === false && entry.payloadLength !== result.length) ||
      result.length === entry.payloadLength &&
        this.schemes.payload.order(
            result.digest,
            entry.payloadDigest,
          ) !== 0
    ) {
      await result.reject();

      return {
        kind: "failure",
        reason: "data_mismatch",
      };
    }

    await result.commit(result.length === entry.payloadLength);

    if (
      result.length === entry.payloadLength &&
      this.schemes.payload.order(
          result.digest,
          entry.payloadDigest,
        ) === 0
    ) {
      const complete = await this.payloadDriver.get(entry.payloadDigest);

      if (!complete) {
        throw new WillowError(
          "Could not get payload for a payload that was just ingested.",
        );
      }

      const authToken = await this.getauthTokenByHash(authTokenHash);

      if (!authToken) {
        throw new WillowError(
          "Could not get authorisation token for a stored entry.",
        );
      }

      this.dispatchEvent(
        new PayloadIngestEvent(entry, authToken, complete),
      );
    }

    const acquisitionId = await this.ingestionMutex.acquire();

    await this.storage.updateAvailablePayload(entry.subspaceId, entry.path);

    this.ingestionMutex.release(acquisitionId);
    return {
      kind: "success",
    };
  }

  /** Retrieve an asynchronous iterator of entry-payload-authorisation triples from the store for a given [`AreaOfInterest`](https://willowprotocol.org/specs/grouping-entries/index.html#aois). */
  async *query(
    areaOfInterest: AreaOfInterest<SubspaceId>,
    order: QueryOrder,
    /** Whether to return entries in reverse (descending) order or not. */
    reverse = false,
  ): AsyncIterable<
    [
      Entry<NamespaceId, SubspaceId, PayloadDigest>,
      Payload | undefined,
      AuthorisationToken,
    ]
  > {
    for await (
      const { entry, authTokenHash } of this.storage.query(
        {
          range: areaTo3dRange({
            maxComponentCount: this.schemes.path.maxComponentCount,
            maxPathComponentLength: this.schemes.path.maxComponentLength,
            maxPathLength: this.schemes.path.maxPathLength,
            minimalSubspace: this.schemes.subspace.minimalSubspaceId,
            successorSubspace: this.schemes.subspace.successor,
          }, areaOfInterest.area),
          maxCount: areaOfInterest.maxCount,
          maxSize: areaOfInterest.maxSize,
        },
        order,
        reverse,
      )
    ) {
      const payload = await this.payloadDriver.get(entry.payloadDigest);

      const authToken = await this.getauthTokenByHash(authTokenHash);

      if (!authToken) {
        continue;
      }

      yield [entry, payload, authToken];
    }
  }

  /** Summarise a given [`Range`](https://willowprotocol.org/specs/grouping-entries/index.html#ranges) into a `PreFingerprint` mapping to the set of entries included by that range.
   *
   * This is mostly used during sync for [3d range-based set reconciliation](https://willowprotocol.org/specs/3d-range-based-set-reconciliation/index.html#d3_range_based_set_reconciliation).
   */
  summarise(
    range: Range3d<SubspaceId>,
  ): Promise<{ fingerprint: Prefingerprint; size: number }> {
    return this.storage.summarise(range);
  }

  splitRange(
    range: Range3d<SubspaceId>,
    knownSize: number,
  ): Promise<
    [Range3d<SubspaceId>, Range3d<SubspaceId>]
  > {
    return this.storage.splitRange(range, knownSize);
  }

  areaOfInterestToRange(
    areaOfInterest: AreaOfInterest<SubspaceId>,
  ): Promise<Range3d<SubspaceId>> {
    return this.storage.removeInterest(areaOfInterest);
  }

  /** Retrieve an asynchronous iterator of entry-payload-authorisation triples from the store for a given [`Range`](https://willowprotocol.org/specs/grouping-entries/index.html#ranges).
   *
   * Returns entries in order by subspace, then path, then timestamp.
   */
  async *queryRange(
    range: Range3d<SubspaceId>,
    /** Whether to return entries descending or ascending. */
    order: "descending" | "ascending",
  ): AsyncIterable<
    [
      Entry<NamespaceId, SubspaceId, PayloadDigest>,
      Payload | undefined,
      AuthorisationToken,
    ]
  > {
    for await (
      const { entry, authTokenHash } of this.storage.query(
        {
          range: range,
          maxCount: 0,
          maxSize: BigInt(0),
        },
        "subspace",
        order === "descending" ? true : false,
      )
    ) {
      const payload = await this.payloadDriver.get(entry.payloadDigest);

      const authToken = await this.getauthTokenByHash(authTokenHash);

      if (!authToken) {
        throw new WillowError(
          "Malformed storage. No authorisation token for stored entry.",
        );
      }

      yield [entry, payload, authToken];
    }
  }

  /** Retrieve a `Payload` for a given entry, if held in storage. */
  getPayload(
    entry: Entry<NamespaceId, SubspaceId, PayloadDigest>,
  ): Promise<Payload | undefined> {
    return this.payloadDriver.get(entry.payloadDigest);
  }

  /** Retrieve a `AuthorisationToken` for a given entry, if held in storage. */
  async getAuthToken(
    entry: Entry<NamespaceId, SubspaceId, PayloadDigest>,
  ): Promise<AuthorisationToken | undefined> {
    let authToken: AuthorisationToken | undefined;

    for await (
      const [, , token] of this.queryRange({
        subspaceRange: {
          start: entry.subspaceId,
          end: this.schemes.subspace.successor(entry.subspaceId) || OPEN_END,
        },
        pathRange: {
          start: entry.path,
          end: successorPath(entry.path, this.schemes.path) || OPEN_END,
        },
        timeRange: {
          start: entry.timestamp,
          end: entry.timestamp + 1n,
        },
      }, "descending")
    ) {
      authToken = token;
    }

    return authToken;
  }

  /** Retrieve an `AuthorisationToken` by hash, if held in storage. */
  private async getauthTokenByHash(
    authTokenHash: PayloadDigest,
  ): Promise<AuthorisationToken | undefined> {
    const authTokenPayload = await this.payloadDriver.get(authTokenHash);

    if (!authTokenPayload) {
      return;
    }
    const authTokenEncoded = await authTokenPayload.bytes();
    const authToken = this.schemes.authorisation.tokenEncoding
      .decode(authTokenEncoded);

    return authToken;
  }
}
