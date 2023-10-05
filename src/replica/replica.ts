import { EntryDriverMemory } from "./storage/entry_drivers/memory.ts";
import { EntryDriver, PayloadDriver } from "./storage/types.ts";
import {
  bigintToBytes,
  compareBytes,
  incrementLastByte,
} from "../util/bytes.ts";
import {
  EntryInput,
  IngestEvent,
  IngestPayloadEvent,
  Payload,
  ProtocolParameters,
  Query,
  ReplicaOpts,
} from "./types.ts";
import { PayloadDriverMemory } from "./storage/payload_drivers/memory.ts";
import { SummarisableStorage } from "./storage/summarisable_storage/types.ts";
import { Entry } from "../entries/types.ts";
import {
  EntryIngestEvent,
  EntryPayloadSetEvent,
  EntryRemoveEvent,
  PayloadIngestEvent,
} from "./events.ts";
import { deferred } from "$std/async/deferred.ts";
import { concat } from "$std/bytes/concat.ts";
import { equals as equalsBytes } from "$std/bytes/equals.ts";
import {
  decodeEntryKey,
  decodeSummarisableStorageValue,
  encodeEntryKeys,
  encodeSummarisableStorageValue,
} from "./util.ts";

/** A local snapshot of a namespace to be written to, queried from, and synced with other replicas.
 *
 * Data is stored as many {@link SignedEntry} with a corresponding {@link Payload}, which the replica may or may not possess.
 *
 * Keeps data in memory unless persisted entry / payload drivers are specified.
 */
export class Replica<
  NamespacePublicKey,
  SubspacePublicKey,
  PayloadDigest,
  AuthorisationOpts,
  AuthorisationToken,
> extends EventTarget {
  namespace: NamespacePublicKey;

  private protocolParams: ProtocolParameters<
    NamespacePublicKey,
    SubspacePublicKey,
    PayloadDigest,
    AuthorisationOpts,
    AuthorisationToken
  >;

  private ptsStorage: SummarisableStorage<Uint8Array, Uint8Array>;
  private sptStorage: SummarisableStorage<Uint8Array, Uint8Array>;
  private tspStorage: SummarisableStorage<Uint8Array, Uint8Array>;

  private entryDriver: EntryDriver;
  private payloadDriver: PayloadDriver<PayloadDigest>;

  private checkedWriteAheadFlag = deferred();

  constructor(
    opts: ReplicaOpts<
      NamespacePublicKey,
      SubspacePublicKey,
      PayloadDigest,
      AuthorisationOpts,
      AuthorisationToken
    >,
  ) {
    super();

    this.namespace = opts.namespace;
    this.protocolParams = opts.protocolParameters;

    const entryDriver = opts.entryDriver || new EntryDriverMemory();
    const payloadDriver = opts.payloadDriver ||
      new PayloadDriverMemory(opts.protocolParameters.payloadScheme);

    this.entryDriver = entryDriver;
    this.payloadDriver = payloadDriver;

    this.ptsStorage = entryDriver.createSummarisableStorage("pts");
    this.sptStorage = entryDriver.createSummarisableStorage("spt");
    this.tspStorage = entryDriver.createSummarisableStorage("tsp");

    this.checkWriteAheadFlag();
  }

  private async checkWriteAheadFlag() {
    const existingInsert = await this.entryDriver.writeAheadFlag.wasInserting();
    const existingRemove = await this.entryDriver.writeAheadFlag.wasRemoving();

    if (existingInsert) {
      const ptsKey = existingInsert[0];

      const details = decodeEntryKey(
        ptsKey,
        "path",
        this.protocolParams.subspaceScheme,
        this.protocolParams.pathEncoding,
      );

      const keys = encodeEntryKeys(
        {
          path: details.path,
          timestamp: details.timestamp,
          subspace: details.subspace,
          pathEncoding: this.protocolParams.pathEncoding,
          subspaceEncoding: this.protocolParams.subspaceScheme,
        },
      );

      // Remove key for each storage.
      await Promise.all([
        this.ptsStorage.remove(keys.pts),
        this.tspStorage.remove(keys.tsp),
        this.sptStorage.remove(keys.spt),
      ]);

      const values = decodeSummarisableStorageValue(
        existingInsert[1],
        this.protocolParams.payloadScheme,
      );

      // TODO(AUTH): Get the encoded authtoken out of payload driver.
      const encodedAuthToken = await this.payloadDriver.get(
        values.authTokenHash,
      );

      if (encodedAuthToken) {
        const decodedToken = this.protocolParams.authorisationScheme
          .tokenEncoding.decode(await encodedAuthToken?.bytes());

        await this.insertEntry({
          path: details.path,
          subspace: details.subspace,
          hash: values.payloadHash,
          length: values.payloadLength,
          timestamp: details.timestamp,
          authToken: decodedToken,
        });
      }
    }

    if (existingRemove) {
      // Derive TAP, APT keys from PTA.
      const ptsKey = existingRemove;

      const details = decodeEntryKey(
        ptsKey,
        "path",
        this.protocolParams.subspaceScheme,
        this.protocolParams.pathEncoding,
      );
      const keys = encodeEntryKeys(
        {
          path: details.path,
          timestamp: details.timestamp,
          subspace: details.subspace,
          pathEncoding: this.protocolParams.pathEncoding,
          subspaceEncoding: this.protocolParams.subspaceScheme,
        },
      );

      // Remove key for each storage.
      await Promise.all([
        this.ptsStorage.remove(keys.pts),
        this.tspStorage.remove(keys.tsp),
        this.sptStorage.remove(keys.spt),
      ]);

      // Unflag remove
      await this.entryDriver.writeAheadFlag.unflagRemoval();
    }

    this.checkedWriteAheadFlag.resolve();
  }

  /** Create a new {@link SignedEntry} for some data and store both in the replica. */
  async set(
    //
    input: EntryInput<SubspacePublicKey>,
    authorisation: AuthorisationOpts,
  ) {
    const identifier = {
      namespace: this.namespace,
      subspace: input.subspace,
      path: input.path,
    };

    const timestamp = input.timestamp !== undefined
      ? input.timestamp
      : BigInt(Date.now() * 1000);

    // Stage it with the driver
    const stagedResult = await this.payloadDriver.stage(input.payload);

    const record = {
      timestamp,
      length: BigInt(stagedResult.length),
      hash: stagedResult.hash,
    };

    const entry = { identifier, record };

    const authToken = await this.protocolParams.authorisationScheme.authorise(
      entry,
      authorisation,
    );

    const ingestResult = await this.ingestEntry(entry, authToken);

    if (ingestResult.kind !== "success") {
      await stagedResult.reject();

      return ingestResult;
    }

    const payload = await stagedResult.commit();

    this.dispatchEvent(new EntryPayloadSetEvent(entry, authToken, payload));

    return ingestResult;
  }

  /** Attempt to store a {@link SignedEntry} in the replica.
   *
   * An entry will not be ingested if it is found to have an invalid signature; if a newer entry with the same path and author are present; or if a newer entry with a path that is a prefix of the given entry exists.
   *
   * Additionally, if the entry's path is a prefix of already-held older entries, those entries will be removed from the replica.
   */
  async ingestEntry(
    entry: Entry<NamespacePublicKey, SubspacePublicKey, PayloadDigest>,
    authorisation: AuthorisationToken,
    externalSourceId?: string,
  ): Promise<
    IngestEvent<NamespacePublicKey, SubspacePublicKey, PayloadDigest>
  > {
    await this.checkedWriteAheadFlag;

    // TODO: Add prefix lock.
    // The idea: a lock for items with a common prefix.

    // Check if the entry belongs to this namespace.
    if (
      !this.protocolParams.namespaceScheme.isEqual(
        this.namespace,
        entry.identifier.namespace,
      )
    ) {
      return {
        kind: "failure",
        reason: "invalid_entry",
        message: "Entry's namespace did not match replica's namespace.",
        err: null,
      };
    }

    if (
      await this.protocolParams.authorisationScheme.isAuthorised(
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

    // Check for entries at the same path from the same author.
    const entryAuthorPathKey = concat(
      this.protocolParams.subspaceScheme.encode(entry.identifier.subspace),
      this.protocolParams.pathEncoding.encode(entry.identifier.path),
    );
    const entryAuthorPathKeyUpper = incrementLastByte(entryAuthorPathKey);

    const prefixKey = concat(
      this.protocolParams.subspaceScheme.encode(entry.identifier.subspace),
      entry.identifier.path,
    );

    // Check if we have any newer entries with this prefix.
    for await (
      const [_path, timestampBytes] of this.entryDriver.prefixIterator
        .prefixesOf(prefixKey)
    ) {
      const view = new DataView(timestampBytes.buffer);
      const prefixTimestamp = view.getBigUint64(0);

      if (prefixTimestamp >= entry.record.timestamp) {
        return {
          kind: "no_op",
          reason: "newer_prefix_found",
        };
      }
    }

    for await (
      const otherEntry of this.sptStorage.entries(
        entryAuthorPathKey,
        entryAuthorPathKeyUpper,
      )
    ) {
      // The new entry will overwrite the one we just found.
      // Remove it.
      const otherDetails = decodeEntryKey(
        otherEntry.key,
        "subspace",
        this.protocolParams.subspaceScheme,
        this.protocolParams.pathEncoding,
      );

      if (
        compareBytes(
          entry.identifier.path,
          otherDetails.path,
        ) !== 0
      ) {
        break;
      }

      const otherEntryTimestampBytesView = new DataView(
        otherEntry.key.buffer,
      );

      const otherEntryTimestamp = otherEntryTimestampBytesView.getBigUint64(
        otherEntry.key.byteLength - 8,
      );

      //  If there is something existing and the timestamp is greater than ours, we have a no-op.
      if (otherEntryTimestamp > entry.record.timestamp) {
        return {
          kind: "no_op",
          reason: "obsolete_from_same_subspace",
        };
      }

      const { payloadHash: otherPayloadHash } = decodeSummarisableStorageValue(
        otherEntry.value,
        this.protocolParams.payloadScheme,
      );

      const payloadDigestOrder = this.protocolParams.payloadScheme.order(
        entry.record.hash,
        otherPayloadHash,
      );

      // If the timestamps are the same, and our hash is less, we have a no-op.
      if (
        otherEntryTimestamp === entry.record.timestamp &&
        payloadDigestOrder === -1
      ) {
        return {
          kind: "no_op",
          reason: "obsolete_from_same_subspace",
        };
      }

      const otherPayloadLengthIsGreater =
        entry.record.length < otherEntryTimestamp;

      // If the timestamps and hashes are the same, and the other payload's length is greater, we have a no-op.
      if (
        otherEntryTimestamp === entry.record.timestamp &&
        payloadDigestOrder === 0 && otherPayloadLengthIsGreater
      ) {
        return {
          kind: "no_op",
          reason: "obsolete_from_same_subspace",
        };
      }

      const otherKeys = encodeEntryKeys(
        {
          path: otherDetails.path,
          timestamp: otherDetails.timestamp,
          subspace: otherDetails.subspace,
          pathEncoding: this.protocolParams.pathEncoding,
          subspaceEncoding: this.protocolParams.subspaceScheme,
        },
      );

      await Promise.all([
        this.ptsStorage.remove(otherKeys.pts),
        this.tspStorage.remove(otherKeys.tsp),
        this.sptStorage.remove(otherKeys.spt),
      ]);

      const toRemovePrefixKey = concat(
        this.protocolParams.subspaceScheme.encode(otherDetails.subspace),
        otherDetails.path,
      );

      await this.entryDriver.prefixIterator.remove(
        toRemovePrefixKey,
      );
    }

    await this.insertEntry({
      path: entry.identifier.path,
      subspace: entry.identifier.subspace,
      hash: entry.record.hash,
      timestamp: entry.record.timestamp,
      length: entry.record.length,
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
      path: Uint8Array;
      subspace: SubspacePublicKey;
      timestamp: bigint;
      hash: PayloadDigest;
      length: bigint;
      authToken: AuthorisationToken;
    },
  ) {
    const keys = encodeEntryKeys(
      {
        path,
        timestamp,
        subspace,
        subspaceEncoding: this.protocolParams.subspaceScheme,
        pathEncoding: this.protocolParams.pathEncoding,
      },
    );

    const encodedToken = this.protocolParams.authorisationScheme
      .tokenEncoding.encode(authToken);

    const stagingResult = await this.payloadDriver.stage(encodedToken);

    const toStore = encodeSummarisableStorageValue(
      {
        payloadHash: hash,
        payloadLength: length,
        authTokenHash: stagingResult.hash,
        payloadEncoding: this.protocolParams.payloadScheme,
      },
    );

    await this.entryDriver.writeAheadFlag.flagInsertion(keys.pts, toStore);

    const prefixKey = concat(
      this.protocolParams.subspaceScheme.encode(subspace),
      path,
    );

    await Promise.all([
      this.ptsStorage.insert(keys.pts, toStore),
      this.sptStorage.insert(keys.spt, toStore),
      this.tspStorage.insert(keys.tsp, toStore),
      stagingResult.commit(),
    ]);

    await this.entryDriver.prefixIterator.insert(
      prefixKey,
      keys.spt.subarray(keys.spt.byteLength - 8),
    );

    // And remove all prefixes with smaller timestamps.
    for await (
      const [prefixedByPath, prefixedByTimestamp] of this.entryDriver
        .prefixIterator
        .prefixedBy(
          prefixKey,
        )
    ) {
      const view = new DataView(prefixedByTimestamp.buffer);
      const prefixTimestamp = view.getBigUint64(prefixedByTimestamp.byteOffset);

      if (prefixTimestamp < timestamp) {
        const subspace = this.protocolParams.subspaceScheme.decode(
          prefixedByPath,
        );

        const encodedSubspaceLength = this.protocolParams.subspaceScheme
          .encodedLength(subspace);

        const prefixedPath = prefixedByPath.subarray(
          encodedSubspaceLength,
        );

        // Delete.
        // Flag a deletion.
        const toDeleteKeys = encodeEntryKeys(
          {
            path: prefixedPath,
            timestamp: prefixTimestamp,
            subspace: subspace,
            pathEncoding: this.protocolParams.pathEncoding,
            subspaceEncoding: this.protocolParams.subspaceScheme,
          },
        );

        const toDeleteValue = await this.ptsStorage.get(toDeleteKeys.pts);

        const storageStuff = toDeleteValue
          ? decodeSummarisableStorageValue(
            toDeleteValue,
            this.protocolParams.payloadScheme,
          )
          : undefined;

        await this.entryDriver.writeAheadFlag.flagRemoval(toDeleteKeys.pts);

        // Remove from all the summarisable storages...
        await Promise.all([
          this.ptsStorage.remove(toDeleteKeys.pts),
          this.tspStorage.remove(toDeleteKeys.tsp),
          this.sptStorage.remove(toDeleteKeys.spt),
          this.entryDriver.prefixIterator.remove(prefixedByPath),
          // Don't fail if we couldn't get the value of the hash due to DB being in a wonky state.
          storageStuff
            ? this.payloadDriver.erase(storageStuff.payloadHash)
            : () => {},
        ]);

        if (storageStuff) {
          this.dispatchEvent(
            new EntryRemoveEvent({
              identifier: {
                path: prefixedPath,
                subspace,
                namespace: this.namespace,
              },
              record: {
                hash: storageStuff.payloadHash,
                length: storageStuff.payloadLength,
                timestamp: prefixTimestamp,
              },
            }),
          );
        }

        await this.entryDriver.writeAheadFlag.unflagRemoval();
      }
    }

    await this.entryDriver.writeAheadFlag.unflagInsertion();
  }

  /** Attempt to store the corresponding payload for one of the replica's entries.
   *
   * A payload will not be ingested if the given entry is not stored in the replica; if the hash of the payload does not match the entry's; or if it is already held.
   */
  async ingestPayload(
    entryDetails: {
      path: Uint8Array;
      timestamp: bigint;
      subspace: SubspacePublicKey;
    },
    payload: Uint8Array | ReadableStream<Uint8Array>,
  ): Promise<IngestPayloadEvent> {
    // Check that there is an entry for this, and get the payload hash!
    const encodedSubspace = this.protocolParams.subspaceScheme.encode(
      entryDetails.subspace,
    );
    const encodedPath = this.protocolParams.pathEncoding.encode(
      entryDetails.path,
    );

    const keyLength = 8 + encodedSubspace.byteLength +
      encodedPath.byteLength;
    const ptsBytes = new Uint8Array(keyLength);

    ptsBytes.set(entryDetails.path, 0);
    const ptaDv = new DataView(ptsBytes.buffer);
    ptaDv.setBigUint64(
      encodedPath.byteLength,
      entryDetails.timestamp,
    );
    ptsBytes.set(encodedSubspace, encodedPath.byteLength + 8);

    for await (
      const kvEntry of this.ptsStorage.entries(
        ptsBytes,
        incrementLastByte(ptsBytes),
        {
          limit: 1,
        },
      )
    ) {
      if (!equalsBytes(kvEntry.key, ptsBytes)) {
        break;
      }

      // Check if we already have it.
      const {
        payloadHash,
        payloadLength,
      } = decodeSummarisableStorageValue(
        kvEntry.value,
        this.protocolParams.payloadScheme,
      );

      const existingPayload = await this.payloadDriver.get(payloadHash);

      if (existingPayload) {
        return {
          kind: "no_op",
          reason: "already_have_it",
        };
      }

      const stagedResult = await this.payloadDriver.stage(payload);

      if (
        this.protocolParams.payloadScheme.order(
          stagedResult.hash,
          payloadHash,
        ) !== 0
      ) {
        await stagedResult.reject();

        return {
          kind: "failure",
          reason: "mismatched_hash",
        };
      }

      const committedPayload = await stagedResult.commit();

      const { subspace, path, timestamp } = decodeEntryKey(
        kvEntry.key,
        "path",
        this.protocolParams.subspaceScheme,
        this.protocolParams.pathEncoding,
      );

      const entry: Entry<
        NamespacePublicKey,
        SubspacePublicKey,
        PayloadDigest
      > = {
        identifier: {
          subspace,
          namespace: this.namespace,
          path,
        },
        record: {
          hash: payloadHash,
          length: payloadLength,
          timestamp,
        },
      };

      this.dispatchEvent(
        new PayloadIngestEvent(entry, committedPayload),
      );

      return {
        kind: "success",
      };
    }

    return {
      kind: "failure",
      reason: "no_entry",
    };
  }

  /** Retrieve a list of entry-payload pairs from the replica for a given {@link Query}. */
  async *query(
    query: Query<SubspacePublicKey>,
  ): AsyncIterable<
    [
      Entry<NamespacePublicKey, SubspacePublicKey, PayloadDigest>,
      Payload | undefined,
      AuthorisationToken,
    ]
  > {
    let listToUse: SummarisableStorage<Uint8Array, Uint8Array>;
    let lowerBound: Uint8Array | undefined;
    let upperBound: Uint8Array | undefined;

    switch (query.order) {
      case "path":
        lowerBound = query.lowerBound;
        upperBound = query.upperBound;
        listToUse = this.ptsStorage;
        break;
      case "subspace":
        lowerBound = query.lowerBound
          ? this.protocolParams.subspaceScheme.encode(query.lowerBound)
          : undefined;
        upperBound = query.upperBound
          ? this.protocolParams.subspaceScheme.encode(query.upperBound)
          : undefined;
        listToUse = this.sptStorage;
        break;
      case "timestamp": {
        if (query.lowerBound) {
          lowerBound = bigintToBytes(query.lowerBound);
        }

        if (query.upperBound) {
          upperBound = bigintToBytes(query.upperBound);
        }

        listToUse = this.tspStorage;
        break;
      }
    }

    const iterator = listToUse.entries(lowerBound, upperBound, {
      reverse: query.reverse,
      limit: query.limit,
    });

    for await (const kvEntry of iterator) {
      const { subspace, path, timestamp } = decodeEntryKey(
        kvEntry.key,
        query.order,
        this.protocolParams.subspaceScheme,
        this.protocolParams.pathEncoding,
      );

      const {
        authTokenHash,
        payloadHash,
        payloadLength,
      } = decodeSummarisableStorageValue(
        kvEntry.value,
        this.protocolParams.payloadScheme,
      );

      const entry: Entry<NamespacePublicKey, SubspacePublicKey, PayloadDigest> =
        {
          identifier: {
            subspace,
            namespace: this.namespace,
            path,
          },
          record: {
            hash: payloadHash,
            length: payloadLength,
            timestamp,
          },
        };

      const payload = await this.payloadDriver.get(payloadHash);

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
