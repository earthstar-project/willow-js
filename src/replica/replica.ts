import { EntryDriverMemory } from "./storage/entry_drivers/memory.ts";
import { EntryDriver, PayloadDriver } from "./storage/types.ts";
import {
  EntryInput,
  IngestEvent,
  IngestPayloadEvent,
  Payload,
  ProtocolParameters,
  QueryOrder,
  ReplicaOpts,
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
  bigintToBytes,
  deferred,
  Entry,
  OPEN_END,
  orderPath,
  Path,
} from "../../deps.ts";
import { Storage3d } from "./storage/storage_3d/types.ts";

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
    opts: ReplicaOpts<
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

    const entryDriver = opts.entryDriver || new EntryDriverMemory({
      pathScheme: opts.protocolParameters.pathScheme,
      payloadScheme: opts.protocolParameters.payloadScheme,
      subspaceScheme: opts.protocolParameters.subspaceScheme,
      fingerprintScheme: opts.protocolParameters.fingerprintScheme,
    });
    const payloadDriver = opts.payloadDriver ||
      new PayloadDriverMemory(opts.protocolParameters.payloadScheme);

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

  /** Create a new {@link SignedEntry} for some data and store both in the replica. */
  async set(
    //
    input: EntryInput<SubspacePublicKey>,
    authorisation: AuthorisationOpts,
  ) {
    const timestamp = input.timestamp !== undefined
      ? input.timestamp
      : BigInt(Date.now() * 1000);

    // Stage it with the driver
    const stagedResult = await this.payloadDriver.stage(input.payload);

    const entry: Entry<NamespacePublicKey, SubspacePublicKey, PayloadDigest> = {
      namespaceId: this.namespace,
      subspaceId: input.subspace,
      path: input.path,
      timestamp: timestamp,
      payloadLength: stagedResult.length,
      payloadDigest: stagedResult.hash,
    };

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
        message: "Entry's namespace did not match replica's namespace.",
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
          area: {
            pathPrefix: entry.path,

            includedSubspaceId: entry.subspaceId,
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

    const stagingResult = await this.payloadDriver.stage(encodedToken);

    await this.entryDriver.writeAheadFlag.flagInsertion({
      namespaceId: this.namespace,
      subspaceId: subspace,
      path: path,
      payloadDigest: hash,
      payloadLength: length,
      timestamp,
    }, stagingResult.hash);

    const prefixKey = [
      this.protocolParams.subspaceScheme.encode(subspace),
      ...path,
    ];

    await Promise.all([
      this.storage.insert({
        payloadDigest: hash,
        authTokenDigest: stagingResult.hash,
        length,
        path,
        subspace,
        timestamp,
      }),
      stagingResult.commit(),
      this.entryDriver.prefixIterator.insert(
        prefixKey,
        bigintToBytes(timestamp),
      ),
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
            this.payloadDriver.erase(toDeleteResult.entry.payloadDigest),
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

  /** Attempt to store the corresponding payload for one of the replica's entries.
   *
   * A payload will not be ingested if the given entry is not stored in the replica; if the hash of the payload does not match the entry's; or if it is already held.
   */
  async ingestPayload(
    entryDetails: {
      path: Path;
      timestamp: bigint;
      subspace: SubspacePublicKey;
    },
    payload: Uint8Array | ReadableStream<Uint8Array>,
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

    const stagedResult = await this.payloadDriver.stage(payload);

    if (
      this.protocolParams.payloadScheme.order(
        stagedResult.hash,
        entry.payloadDigest,
      ) !== 0
    ) {
      await stagedResult.reject();

      return {
        kind: "failure",
        reason: "mismatched_hash",
      };
    }

    const committedPayload = await stagedResult.commit();

    this.dispatchEvent(
      new PayloadIngestEvent(entry, committedPayload),
    );

    return {
      kind: "success",
    };
  }

  /** Retrieve a list of entry-payload pairs from the replica for a given {@link Query}. */
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
        areaOfInterest,
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
}
