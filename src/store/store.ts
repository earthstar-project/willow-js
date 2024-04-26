import { EntryDriverMemory } from "./storage/entry_drivers/memory.ts";
import { EntryDriver, PayloadDriver } from "./storage/types.ts";
import {
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
import {
  AreaOfInterest,
  areaTo3dRange,
  bigintToBytes,
  deferred,
  Entry,
  OPEN_END,
  orderPath,
  Path,
  Range3d,
  successorPath,
  successorPrefix,
} from "../../deps.ts";
import { Storage3d } from "./storage/storage_3d/types.ts";
import { WillowError } from "../errors.ts";
import Mutex from "./mutex.ts";

/** A local set of a particular namespace's entries to be written to, read from, and synced with other `Store`s.
 *
 * Keeps data in memory unless persisted entry / payload drivers are specified.
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

  private checkedWriteAheadFlag = deferred();

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

    const entryDriver = opts.entryDriver || new EntryDriverMemory({
      pathScheme: opts.schemes.path,
      payloadScheme: opts.schemes.payload,
      subspaceScheme: opts.schemes.subspace,
      fingerprintScheme: opts.schemes.fingerprint,
      getPayloadLength: (digest) => {
        return this.payloadDriver.length(digest);
      },
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

  /** Create a new authorised entry for a payload, and store both in the store. */
  async set(
    //
    input: EntryInput<SubspaceId>,
    authorisation: AuthorisationOpts,
  ) {
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
   * An entry will not be ingested if it is unauthorised; if a newer entry with the same path and subspace are present; or if a newer entry with a path that is a prefix of the given entry exists.
   *
   * Additionally, if the entry's path is a prefix of already-held older entries, those entries will be removed from the `Store`.
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
    await this.checkedWriteAheadFlag;

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
        message: "One or more of the entry's signatures was invalid.",
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
                end: successorPrefix(entry.path) ||
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
   */
  async ingestPayload(
    entryDetails: {
      path: Path;
      timestamp: bigint;
      subspace: SubspaceId;
    },
    payload: AsyncIterable<Uint8Array>,
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
      knownDigest: entry.payloadDigest,
      knownLength: entry.payloadLength,
    });

    if (
      result.length !== entry.payloadLength ||
      (result.length === entry.payloadLength &&
        this.schemes.payload.order(
            result.digest,
            entry.payloadDigest,
          ) !== 0)
    ) {
      return {
        kind: "failure",
        reason: "data_mismatch",
      };
    }

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

      const authToken = await this.payloadDriver.get(authTokenHash);

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

  /** Retrieve an asynchronous iterator of entry-payload-authorisation triples from the store for a given `Area`. */
  async *query(
    areaOfInterest: AreaOfInterest<SubspaceId>,
    order: QueryOrder,
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

      const authTokenPayload = await this.payloadDriver.get(authTokenHash);

      if (!authTokenPayload) {
        continue;
      }
      const authTokenEncoded = await authTokenPayload.bytes();
      const authToken = this.schemes.authorisation.tokenEncoding
        .decode(authTokenEncoded);

      yield [entry, payload, authToken];
    }
  }

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

  async *queryRange(
    range: Range3d<SubspaceId>,
    order: "newest" | "oldest",
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
        "timestamp",
        order === "newest" ? true : false,
      )
    ) {
      const payload = await this.payloadDriver.get(entry.payloadDigest);

      const authTokenPayload = await this.payloadDriver.get(authTokenHash);

      if (!authTokenPayload) {
        continue;
      }
      const authTokenEncoded = await authTokenPayload.bytes();
      const authToken = this.schemes.authorisation.tokenEncoding
        .decode(authTokenEncoded);

      yield [entry, payload, authToken];
    }
  }

  getPayload(
    entry: Entry<NamespaceId, SubspaceId, PayloadDigest>,
  ): Promise<Payload | undefined> {
    return this.payloadDriver.get(entry.payloadDigest);
  }

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
      }, "newest")
    ) {
      authToken = token;
    }

    return authToken;
  }
}
