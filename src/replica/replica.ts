import { EntryDriverMemory } from "./storage/entry_drivers/memory.ts";
import { EntryDriver, PayloadDriver } from "./storage/types.ts";
import { bigintToBytes, compareBytes } from "../util/bytes.ts";
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
import { Entry } from "../entries/types.ts";
import {
  EntryIngestEvent,
  EntryPayloadSetEvent,
  EntryRemoveEvent,
  PayloadIngestEvent,
} from "./events.ts";
import { concat, deferred, Products } from "../../deps.ts";
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
      pathLengthScheme: opts.protocolParameters.pathLengthScheme,
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
          path: existingInsert.entry.identifier.path,
          subspace: existingInsert.entry.identifier.subspace,
          hash: existingInsert.entry.record.hash,
          length: existingInsert.entry.record.length,
          timestamp: existingInsert.entry.record.timestamp,
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

    // Check for collisions with stored entries

    for await (
      const { entry: otherEntry } of this.storage.entriesByQuery(
        {
          order: "subspace",
          subspace: {
            lowerBound: entry.identifier.subspace,
            upperBound: this.protocolParams.subspaceScheme.successor(
              entry.identifier.subspace,
            ),
          },
          path: {
            lowerBound: entry.identifier.path,
            upperBound: Products.makeSuccessorPath(
              this.protocolParams.pathLengthScheme.maxLength,
            )(entry.identifier.path),
          },
        },
      )
    ) {
      if (
        compareBytes(
          entry.identifier.path,
          otherEntry.identifier.path,
        ) !== 0
      ) {
        break;
      }

      //  If there is something existing and the timestamp is greater than ours, we have a no-op.
      if (otherEntry.record.timestamp > entry.record.timestamp) {
        return {
          kind: "no_op",
          reason: "obsolete_from_same_subspace",
        };
      }

      const payloadDigestOrder = this.protocolParams.payloadScheme.order(
        entry.record.hash,
        otherEntry.record.hash,
      );

      // If the timestamps are the same, and our hash is less, we have a no-op.
      if (
        otherEntry.record.timestamp === entry.record.timestamp &&
        payloadDigestOrder === -1
      ) {
        return {
          kind: "no_op",
          reason: "obsolete_from_same_subspace",
        };
      }

      const otherPayloadLengthIsGreater =
        entry.record.length < otherEntry.record.timestamp;

      // If the timestamps and hashes are the same, and the other payload's length is greater, we have a no-op.
      if (
        otherEntry.record.timestamp === entry.record.timestamp &&
        payloadDigestOrder === 0 && otherPayloadLengthIsGreater
      ) {
        return {
          kind: "no_op",
          reason: "obsolete_from_same_subspace",
        };
      }

      await this.storage.remove(otherEntry);

      const toRemovePrefixKey = concat(
        this.protocolParams.subspaceScheme.encode(
          otherEntry.identifier.subspace,
        ),
        otherEntry.identifier.path,
      );

      await this.entryDriver.prefixIterator.remove(
        toRemovePrefixKey,
      );

      this.dispatchEvent(
        new EntryRemoveEvent(otherEntry),
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
      path: Uint8Array;
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
      identifier: {
        namespace: this.namespace,
        subspace: subspace,
        path: path,
      },
      record: {
        hash,
        length,
        timestamp,
      },
    }, stagingResult.hash);

    const prefixKey = concat(
      this.protocolParams.subspaceScheme.encode(subspace),
      path,
    );

    await Promise.all([
      this.storage.insert({
        payloadHash: hash,
        authTokenHash: stagingResult.hash,
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
      const [prefixedByKey, prefixedByTimestamp] of this.entryDriver
        .prefixIterator
        .prefixedBy(
          prefixKey,
        )
    ) {
      const view = new DataView(prefixedByTimestamp.buffer);
      const prefixTimestamp = view.getBigUint64(prefixedByTimestamp.byteOffset);

      if (prefixTimestamp < timestamp) {
        const subspace = this.protocolParams.subspaceScheme.decode(
          prefixedByKey,
        );

        const encodedSubspaceLength = this.protocolParams.subspaceScheme
          .encodedLength(subspace);

        const prefixedPath = prefixedByKey.subarray(
          encodedSubspaceLength,
        );

        const toDeleteResult = await this.storage.get(subspace, prefixedPath);

        if (toDeleteResult) {
          await this.entryDriver.writeAheadFlag.flagRemoval(
            toDeleteResult.entry,
          );

          await Promise.all([
            this.storage.remove(toDeleteResult.entry),
            this.payloadDriver.erase(toDeleteResult.entry.record.hash),
            this.entryDriver.prefixIterator.remove(prefixedByKey),
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
      path: Uint8Array;
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

    const existingPayload = await this.payloadDriver.get(entry.record.hash);

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
        entry.record.hash,
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
    query: Query<SubspacePublicKey>,
  ): AsyncIterable<
    [
      Entry<NamespacePublicKey, SubspacePublicKey, PayloadDigest>,
      Payload | undefined,
      AuthorisationToken,
    ]
  > {
    for await (
      const { entry, authTokenHash } of this.storage.entriesByQuery(query)
    ) {
      const payload = await this.payloadDriver.get(entry.record.hash);

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
