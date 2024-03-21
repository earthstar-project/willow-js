import { EntryDriverMemory } from "./storage/entry_drivers/memory.ts";
import { EntryDriver, PayloadDriver } from "./storage/types.ts";
import {
  EntryInput,
  IngestEvent,
  IngestPayloadEvent,
  Payload,
  ProtocolParameters,
  QueryOrder,
  StoreOpts,
} from "./types.ts";
import { PayloadDriverMemory } from "./storage/payload_drivers/memory.ts";
import {
  EntryIngestEvent,
  EntryPayloadSetEvent,
  EntryRemoveEvent,
  PayloadIngestEvent,
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
  successorPrefix,
} from "../../deps.ts";
import { RangeOfInterest, Storage3d } from "./storage/storage_3d/types.ts";
import { WillowError } from "../errors.ts";

/** A local set of a particular namespace's entries to be written to, read from, and synced with other `Store`s.
 *
 * Keeps data in memory unless persisted entry / payload drivers are specified.
 *
 * https://willowprotocol.org/specs/data-model/index.html#store
 */
export class Store<
  NamespacePublicKey,
  SubspacePublicKey,
  PayloadDigest,
  AuthorisationOpts,
  AuthorisationToken,
  Fingerprint,
> extends EventTarget {
  namespace: NamespacePublicKey;

  private protocolParams: ProtocolParameters<
    NamespacePublicKey,
    SubspacePublicKey,
    PayloadDigest,
    AuthorisationOpts,
    AuthorisationToken,
    Fingerprint
  >;

  private entryDriver: EntryDriver<
    NamespacePublicKey,
    SubspacePublicKey,
    PayloadDigest,
    Fingerprint
  >;
  private payloadDriver: PayloadDriver<PayloadDigest>;

  private storage: Storage3d<
    NamespacePublicKey,
    SubspacePublicKey,
    PayloadDigest,
    Fingerprint
  >;

  private checkedWriteAheadFlag = deferred();

  constructor(
    opts: StoreOpts<
      NamespacePublicKey,
      SubspacePublicKey,
      PayloadDigest,
      AuthorisationOpts,
      AuthorisationToken,
      Fingerprint
    >,
  ) {
    super();

    this.namespace = opts.namespace;
    this.protocolParams = opts.protocolParameters;

    const payloadDriver = opts.payloadDriver ||
      new PayloadDriverMemory(opts.protocolParameters.payloadScheme);

    const entryDriver = opts.entryDriver || new EntryDriverMemory({
      pathScheme: opts.protocolParameters.pathScheme,
      payloadScheme: opts.protocolParameters.payloadScheme,
      subspaceScheme: opts.protocolParameters.subspaceScheme,
      fingerprintScheme: opts.protocolParameters.fingerprintScheme,
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
      // TODO(AUTH): Get the encoded authtoken out of payload driver.
      const encodedAuthToken = await this.payloadDriver.get(
        existingInsert.authTokenHash,
      );

      if (encodedAuthToken) {
        const decodedToken = this.protocolParams.authorisationScheme
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
    input: EntryInput<SubspacePublicKey>,
    authorisation: AuthorisationOpts,
  ) {
    const timestamp = input.timestamp !== undefined
      ? input.timestamp
      : BigInt(Date.now() * 1000);

    // Stage it with the driver
    const { digest, payload, length } = await this.payloadDriver.set(
      input.payload,
    );

    const entry: Entry<NamespacePublicKey, SubspacePublicKey, PayloadDigest> = {
      namespaceId: this.namespace,
      subspaceId: input.subspace,
      path: input.path,
      timestamp: timestamp,
      payloadLength: length,
      payloadDigest: digest,
    };

    const authToken = await this.protocolParams.authorisationScheme.authorise(
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
    entry: Entry<NamespacePublicKey, SubspacePublicKey, PayloadDigest>,
    authorisation: AuthorisationToken,
    externalSourceId?: string,
  ): Promise<
    IngestEvent<
      NamespacePublicKey,
      SubspacePublicKey,
      PayloadDigest,
      AuthorisationToken
    >
  > {
    await this.checkedWriteAheadFlag;

    // TODO: Add prefix lock.
    // The idea: a lock for items with a common prefix.

    // Check if the entry belongs to this namespace.
    if (
      !this.protocolParams.namespaceScheme.isEqual(
        this.namespace,
        entry.namespaceId,
      )
    ) {
      return {
        kind: "failure",
        reason: "invalid_entry",
        message: "Entry's namespace did not match store's namespace.",
        err: null,
      };
    }

    if (
      await this.protocolParams.authorisationScheme.isAuthorisedWrite(
        entry,
        authorisation,
      ) === false
    ) {
      return {
        kind: "failure",
        reason: "invalid_entry",
        message: "One or more of the entry's signatures was invalid.",
        err: null,
      };
    }

    const subspacePath = [
      this.protocolParams.subspaceScheme.encode(entry.subspaceId),
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
        return {
          kind: "no_op",
          reason: "newer_prefix_found",
        };
      }
    }

    // Check for collisions with stored entries

    for await (
      const { entry: otherEntry } of this.storage.query(
        {
          range: {
            pathRange: {
              start: entry.path,
              end: successorPrefix(entry.path) ||
                OPEN_END,
            },
            subspaceRange: {
              start: entry.subspaceId,
              end: this.protocolParams.subspaceScheme.successor(
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
        return {
          kind: "no_op",
          reason: "obsolete_from_same_subspace",
        };
      }

      const payloadDigestOrder = this.protocolParams.payloadScheme.order(
        entry.payloadDigest,
        otherEntry.payloadDigest,
      );

      // If the timestamps are the same, and our hash is less, we have a no-op.
      if (
        otherEntry.timestamp === entry.timestamp &&
        payloadDigestOrder === -1
      ) {
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
        return {
          kind: "no_op",
          reason: "obsolete_from_same_subspace",
        };
      }

      await this.storage.remove(otherEntry);

      const toRemovePrefixPath = [
        this.protocolParams.subspaceScheme.encode(
          otherEntry.subspaceId,
        ),
        ...otherEntry.path,
      ];

      await this.entryDriver.prefixIterator.remove(
        toRemovePrefixPath,
      );

      this.dispatchEvent(
        new EntryRemoveEvent(otherEntry),
      );
    }

    await this.insertEntry({
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

    return {
      kind: "success",
      entry: entry,
      authToken: authorisation,
      externalSourceId: externalSourceId,
    };
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
      subspace: SubspacePublicKey;
      timestamp: bigint;
      hash: PayloadDigest;
      length: bigint;
      authToken: AuthorisationToken;
    },
  ) {
    const encodedToken = this.protocolParams.authorisationScheme
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
      this.protocolParams.subspaceScheme.encode(subspace),
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
    for await (
      const [prefixedBySubspacePath, prefixedByTimestamp] of this.entryDriver
        .prefixIterator
        .prefixedBy(
          prefixKey,
        )
    ) {
      const view = new DataView(prefixedByTimestamp.buffer);
      const prefixTimestamp = view.getBigUint64(prefixedByTimestamp.byteOffset);

      const [prefixedBySubspace, ...prefixedByPath] = prefixedBySubspacePath;

      if (prefixTimestamp < timestamp) {
        const subspace = this.protocolParams.subspaceScheme.decode(
          prefixedBySubspace,
        );

        const toDeleteResult = await this.storage.get(subspace, prefixedByPath);

        if (toDeleteResult) {
          await this.entryDriver.writeAheadFlag.flagRemoval(
            toDeleteResult.entry,
          );

          await Promise.all([
            this.storage.remove(toDeleteResult.entry),
            async () => {
              const count = await this.entryDriver.payloadReferenceCounter
                .decrement(toDeleteResult.entry.payloadDigest);

              if (count === 0) {
                await this.payloadDriver.erase(
                  toDeleteResult.entry.payloadDigest,
                );
              }
            },

            this.entryDriver.prefixIterator.remove(prefixedBySubspacePath),
          ]);

          await this.entryDriver.writeAheadFlag.unflagRemoval();

          this.dispatchEvent(
            new EntryRemoveEvent(toDeleteResult.entry),
          );
        }
      }
    }

    await this.entryDriver.writeAheadFlag.unflagInsertion();
  }

  /** Attempt to store the corresponding payload for one of the store's entries.
   *
   * A payload will not be ingested if the given entry is not stored in the store; if the hash of the payload does not match the entry's; or if it is already held.
   */
  async ingestPayload(
    entryDetails: {
      path: Path;
      timestamp: bigint;
      subspace: SubspacePublicKey;
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

    const { entry } = getResult;

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
      result.length > entry.payloadLength ||
      (result.length === entry.payloadLength &&
        this.protocolParams.payloadScheme.order(
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
      this.protocolParams.payloadScheme.order(
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

      this.dispatchEvent(
        new PayloadIngestEvent(entry, complete),
      );
    }

    await this.storage.updateAvailablePayload(entry.subspaceId, entry.path);

    return {
      kind: "success",
    };
  }

  /** Retrieve an asynchronous iterator of entry-payload-authorisation triples from the store for a given `Area`. */
  async *query(
    areaOfInterest: AreaOfInterest<SubspacePublicKey>,
    order: QueryOrder,
    reverse = false,
  ): AsyncIterable<
    [
      Entry<NamespacePublicKey, SubspacePublicKey, PayloadDigest>,
      Payload | undefined,
      AuthorisationToken,
    ]
  > {
    for await (
      const { entry, authTokenHash } of this.storage.query(
        {
          range: areaTo3dRange({
            maxComponentCount: this.protocolParams.pathScheme.maxComponentCount,
            maxPathComponentLength:
              this.protocolParams.pathScheme.maxComponentLength,
            maxPathLength: this.protocolParams.pathScheme.maxPathLength,
            minimalSubspace:
              this.protocolParams.subspaceScheme.minimalSubspaceId,
            successorSubspace: this.protocolParams.subspaceScheme.successor,
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
      const authToken = this.protocolParams.authorisationScheme.tokenEncoding
        .decode(authTokenEncoded);

      yield [entry, payload, authToken];
    }
  }

  summarise(
    range: Range3d<SubspacePublicKey>,
  ): Promise<{ fingerprint: Fingerprint; size: number }> {
    return this.storage.summarise(range);
  }

  splitRange(
    range: Range3d<SubspacePublicKey>,
    knownSize: number,
  ): Promise<
    [Range3d<SubspacePublicKey>, Range3d<SubspacePublicKey>]
  > {
    return this.storage.splitRange(range, knownSize);
  }

  areaOfInterestToRange(
    areaOfInterest: AreaOfInterest<SubspacePublicKey>,
  ): Promise<Range3d<SubspacePublicKey>> {
    return this.storage.removeInterest(areaOfInterest);
  }

  async *queryRange(
    range: Range3d<SubspacePublicKey>,
  ): AsyncIterable<
    [
      Entry<NamespacePublicKey, SubspacePublicKey, PayloadDigest>,
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
        true,
      )
    ) {
      const payload = await this.payloadDriver.get(entry.payloadDigest);

      const authTokenPayload = await this.payloadDriver.get(authTokenHash);

      if (!authTokenPayload) {
        continue;
      }
      const authTokenEncoded = await authTokenPayload.bytes();
      const authToken = this.protocolParams.authorisationScheme.tokenEncoding
        .decode(authTokenEncoded);

      yield [entry, payload, authToken];
    }
  }
}
