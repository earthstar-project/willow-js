import { EntryDriverMemory } from "./storage/memory_driver.ts";
import { EntryDriver, PayloadDriver } from "./storage/types.ts";
import { Entry, SignedEntry } from "../types.ts";
import { bytesConcat, bytesEquals, deferred } from "../../deps.ts";
import { signEntry, verifyEntry } from "../sign_verify/sign_verify.ts";
import {
  bigintToBytes,
  compareBytes,
  concatSummarisableStorageValue,
  detailsFromBytes,
  entryAuthorPathBytes,
  entryKeyBytes,
  incrementLastByte,
  sliceSummarisableStorageValue,
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

export class Replica<KeypairType> {
  private namespace: Uint8Array;

  private protocolParams: ProtocolParameters<KeypairType>;

  private ptaStorage: SummarisableStorage<Uint8Array, Uint8Array>;
  private aptStorage: SummarisableStorage<Uint8Array, Uint8Array>;
  private tapStorage: SummarisableStorage<Uint8Array, Uint8Array>;

  private entryDriver: EntryDriver;
  private payloadDriver: PayloadDriver;

  private checkedWriteAheadFlag = deferred();

  constructor(opts: ReplicaOpts<KeypairType>) {
    // TODO: At least validate that the namespace length matches the protocol params.
    this.namespace = opts.namespace;
    this.protocolParams = opts.protocolParameters;

    const entryDriver = opts.entryDriver || new EntryDriverMemory();
    const payloadDriver = opts.payloadDriver ||
      new PayloadDriverMemory(opts.protocolParameters);

    this.entryDriver = entryDriver;
    this.payloadDriver = payloadDriver;

    this.ptaStorage = entryDriver.createSummarisableStorage("pta");
    this.aptStorage = entryDriver.createSummarisableStorage("apt");
    this.tapStorage = entryDriver.createSummarisableStorage("tap");

    this.checkWriteAheadFlag();
  }

  private async checkWriteAheadFlag() {
    const existingInsert = await this.entryDriver.writeAheadFlag.wasInserting();
    const existingRemove = await this.entryDriver.writeAheadFlag.wasRemoving();

    if (existingInsert) {
      const ptaKey = existingInsert[0];

      const details = detailsFromBytes(
        ptaKey,
        "path",
        this.protocolParams.pubkeyLength,
      );

      const keys = entryKeyBytes(
        details.path,
        details.timestamp,
        details.author,
      );

      // Remove key for each storage.
      await Promise.all([
        this.ptaStorage.remove(keys.pta),
        this.tapStorage.remove(keys.tap),
        this.aptStorage.remove(keys.apt),
      ]);

      const values = sliceSummarisableStorageValue(
        existingInsert[1],
        this.protocolParams,
      );

      await this.insertEntry({
        namespaceSignature: values.namespaceSignature,
        authorSignature: values.authorSignature,
        path: details.path,
        author: details.author,
        hash: values.payloadHash,
        length: values.payloadLength,
        timestamp: details.timestamp,
      });
    }

    if (existingRemove) {
      // Derive TAP, APT keys from PTA.
      const ptaKey = existingRemove;

      const details = detailsFromBytes(
        ptaKey,
        "path",
        this.protocolParams.pubkeyLength,
      );
      const keys = entryKeyBytes(
        details.path,
        details.timestamp,
        details.author,
      );

      // Remove key for each storage.
      await Promise.all([
        this.ptaStorage.remove(keys.pta),
        this.tapStorage.remove(keys.tap),
        this.aptStorage.remove(keys.apt),
      ]);

      // Unflag remove
      await this.entryDriver.writeAheadFlag.unflagRemoval();
    }

    this.checkedWriteAheadFlag.resolve();
  }

  private verify(signedEntry: SignedEntry) {
    return verifyEntry({
      signedEntry,
      verify: this.protocolParams.verify,
    });
  }

  private sign(
    entry: Entry,
    namespaceKeypair: KeypairType,
    authorKeypair: KeypairType,
  ) {
    return signEntry({
      entry,
      namespaceKeypair,
      authorKeypair,
      sign: this.protocolParams.sign,
    });
  }

  async set(
    namespaceKeypair: KeypairType,
    authorKeypair: KeypairType,
    input: EntryInput,
  ) {
    const identifier = {
      namespace: this.namespace,
      author: await this.protocolParams.pubkeyBytesFromPair(authorKeypair),
      path: input.path,
    };

    let timestamp = input.timestamp !== undefined
      ? input.timestamp
      : BigInt(Date.now() * 1000);

    if (!input.timestamp) {
      // Get the latest timestamp from the same path and plus one.

      for await (
        const [signed] of this.query({
          order: "path",
          lowerBound: input.path,
          upperBound: incrementLastByte(input.path),
        })
      ) {
        //
        if (
          compareBytes(
            new Uint8Array(signed.entry.identifier.path),
            new Uint8Array(input.path),
          ) !== 0
        ) {
          break;
        }

        timestamp = signed.entry.record.timestamp + BigInt(1);
      }
    }

    // Stage it with the driver
    const stagedResult = await this.payloadDriver.stage(input.payload);

    const record = {
      timestamp,
      length: BigInt(stagedResult.length),
      hash: stagedResult.hash,
    };

    const signed = await this.sign(
      { identifier, record },
      namespaceKeypair,
      authorKeypair,
    );

    const ingestResult = await this.ingestEntry(signed);

    if (ingestResult.kind !== "success") {
      return ingestResult;
    }

    await stagedResult.commit();

    return ingestResult;
  }

  async ingestEntry(
    signed: SignedEntry,
  ): Promise<IngestEvent> {
    await this.checkedWriteAheadFlag;

    // TODO: Add prefix lock.

    // Check if the entry belongs to this namespace.
    if (
      !bytesEquals(
        this.namespace,
        new Uint8Array(signed.entry.identifier.namespace),
      )
    ) {
      return {
        kind: "failure",
        reason: "invalid_entry",
        message: "Entry's namespace did not match replica's namespace.",
        err: null,
      };
    }

    if (await this.verify(signed) === false) {
      return {
        kind: "failure",
        reason: "invalid_entry",
        message: "One or more of the entry's signatures was invalid.",
        err: null,
      };
    }

    // Check for entries at the same path from the same author.
    const entryAuthorPathKey = entryAuthorPathBytes(signed.entry);
    const entryAuthorPathKeyUpper = incrementLastByte(entryAuthorPathKey);

    // Check if we have any newer entries with this prefix.
    for await (
      const [_path, timestampBytes] of this.entryDriver.prefixIterator
        .prefixesOf(entryAuthorPathKey)
    ) {
      const view = new DataView(timestampBytes.buffer);
      const prefixTimestamp = view.getBigUint64(0);

      if (prefixTimestamp >= signed.entry.record.timestamp) {
        return {
          kind: "no_op",
          reason: "newer_prefix_found",
        };
      }
    }

    for await (
      const otherEntry of this.aptStorage.entries(
        entryAuthorPathKey,
        entryAuthorPathKeyUpper,
      )
    ) {
      // TODO: break if encountering a path that is greater than ours within this range!

      const otherEntryTimestampBytesView = new DataView(
        otherEntry.key.buffer,
      );

      const otherEntryTimestamp = otherEntryTimestampBytesView.getBigUint64(
        otherEntry.key.byteLength - 8,
      );

      //  If there is something existing and the timestamp is greater than ours, we have a no-op.
      if (otherEntryTimestamp > signed.entry.record.timestamp) {
        return {
          kind: "no_op",
          reason: "obsolete_from_same_author",
        };
      }

      const hashOrder = compareBytes(
        new Uint8Array(signed.entry.record.hash),
        otherEntry.value,
      );

      // If the timestamps are the same, and our hash is less, we have a no-op.
      if (
        otherEntryTimestamp === signed.entry.record.timestamp &&
        hashOrder === -1
      ) {
        return {
          kind: "no_op",
          reason: "obsolete_from_same_author",
        };
      }

      const otherPayloadLengthIsGreater =
        signed.entry.record.length < otherEntryTimestamp;

      // If the timestamps and hashes are the same, and the other payload's length is greater, we have a no-op.
      if (
        otherEntryTimestamp === signed.entry.record.timestamp &&
        hashOrder === 0 && otherPayloadLengthIsGreater
      ) {
        return {
          kind: "no_op",
          reason: "obsolete_from_same_author",
        };
      }

      // The new entry will overwrite the one we just found.
      // Remove it.
      const otherDetails = detailsFromBytes(
        otherEntry.key,
        "author",
        this.protocolParams.pubkeyLength,
      );
      const keys = entryKeyBytes(
        otherDetails.path,
        otherDetails.timestamp,
        otherDetails.author,
      );

      // TODO: This does only recovers the removal, not the entry we mean to ingest...
      await this.entryDriver.writeAheadFlag.flagRemoval(keys.pta);

      Promise.all([
        this.ptaStorage.remove(keys.pta),
        this.tapStorage.remove(keys.tap),
        this.aptStorage.remove(keys.apt),
      ]);

      await this.entryDriver.prefixIterator.remove(
        new Uint8Array(signed.entry.identifier.path),
      );

      await this.entryDriver.writeAheadFlag.unflagRemoval();
    }

    await this.insertEntry({
      namespaceSignature: new Uint8Array(signed.namespaceSignature),
      authorSignature: new Uint8Array(signed.authorSignature),
      path: new Uint8Array(signed.entry.identifier.path),
      author: new Uint8Array(signed.entry.identifier.author),
      hash: new Uint8Array(signed.entry.record.hash),
      timestamp: signed.entry.record.timestamp,
      length: signed.entry.record.length,
    });

    return {
      kind: "success",
      entry: signed,
      sourceId: "TODO...",
    };
  }

  private async insertEntry(
    {
      path,
      timestamp,
      author,
      hash,
      namespaceSignature,
      authorSignature,
      length,
    }: {
      namespaceSignature: Uint8Array;
      authorSignature: Uint8Array;
      path: Uint8Array;
      author: Uint8Array;
      timestamp: bigint;
      hash: Uint8Array;
      length: bigint;
    },
  ) {
    const keys = entryKeyBytes(
      path,
      timestamp,
      author,
    );

    const toStore = concatSummarisableStorageValue(
      {
        payloadHash: hash,
        namespaceSignature: namespaceSignature,
        authorSignature: authorSignature,
        payloadLength: length,
      },
    );

    await this.entryDriver.writeAheadFlag.flagInsertion(keys.pta, toStore);

    const entryAuthorPathKey = bytesConcat(author, path);

    await Promise.all([
      this.ptaStorage.insert(keys.pta, toStore),
      this.aptStorage.insert(keys.apt, toStore),
      this.tapStorage.insert(keys.tap, toStore),
    ]);

    await this.entryDriver.prefixIterator.insert(
      entryAuthorPathKey,
      keys.apt.slice(keys.apt.byteLength - 8),
    );

    // And remove all prefixes with smaller timestamps.
    for await (
      const [prefixedByPath, prefixedByTimestamp] of this.entryDriver
        .prefixIterator
        .prefixedBy(
          entryAuthorPathKey,
        )
    ) {
      const view = new DataView(prefixedByTimestamp.buffer);
      const prefixTimestamp = view.getBigUint64(0);

      if (prefixTimestamp < timestamp) {
        const author = prefixedByPath.slice(
          0,
          this.protocolParams.pubkeyLength,
        );
        const path = prefixedByPath.slice(this.protocolParams.pubkeyLength);

        // Delete.
        // Flag a deletion.
        const toDeleteKeys = entryKeyBytes(
          path,
          prefixTimestamp,
          author,
        );

        const toDeleteValue = await this.ptaStorage.get(toDeleteKeys.pta);

        const storageStuff = toDeleteValue
          ? sliceSummarisableStorageValue(
            toDeleteValue,
            this.protocolParams,
          )
          : undefined;

        await this.entryDriver.writeAheadFlag.flagRemoval(toDeleteKeys.pta);

        // Remove from all the summarisable storages...
        await Promise.all([
          this.ptaStorage.remove(toDeleteKeys.pta),
          this.tapStorage.remove(toDeleteKeys.tap),
          this.aptStorage.remove(toDeleteKeys.apt),
          this.entryDriver.prefixIterator.remove(prefixedByPath),
          // Don't fail if we couldn't get the value of the hash due to DB being in a wonky state.
          storageStuff
            ? this.payloadDriver.erase(storageStuff.payloadHash)
            : () => {},
        ]);

        await this.entryDriver.writeAheadFlag.unflagRemoval();
      }
    }

    await this.entryDriver.writeAheadFlag.unflagInsertion();
  }

  async ingestPayload(
    entryDetails: {
      path: Uint8Array;
      timestamp: bigint;
      author: Uint8Array;
    },
    payload: Uint8Array | ReadableStream<Uint8Array>,
  ): Promise<IngestPayloadEvent> {
    // Check that there is an entry for this, and get the payload hash!
    const keyLength = 8 + entryDetails.author.byteLength +
      entryDetails.path.byteLength;
    const ptaBytes = new Uint8Array(keyLength);

    ptaBytes.set(entryDetails.path, 0);
    const ptaDv = new DataView(ptaBytes.buffer);
    ptaDv.setBigUint64(
      entryDetails.path.byteLength,
      entryDetails.timestamp,
    );
    ptaBytes.set(entryDetails.author, entryDetails.path.byteLength + 8);

    for await (
      const entry of this.ptaStorage.entries(
        ptaBytes,
        incrementLastByte(ptaBytes),
        {
          limit: 1,
        },
      )
    ) {
      if (!bytesEquals(entry.key, ptaBytes)) {
        break;
      }

      // Check if we already have it.
      const { payloadHash } = sliceSummarisableStorageValue(
        entry.value,
        this.protocolParams,
      );

      const existingPayload = await this.payloadDriver.get(payloadHash);

      if (existingPayload) {
        return {
          kind: "no_op",
          reason: "already_have_it",
        };
      }

      // Stage it with the driver
      const stagedResult = await this.payloadDriver.stage(payload);

      if (compareBytes(stagedResult.hash, payloadHash) !== 0) {
        await stagedResult.reject();

        return {
          kind: "failure",
          reason: "mismatched_hash",
        };
      }

      await stagedResult.commit();

      return {
        kind: "success",
      };
    }

    return {
      kind: "failure",
      reason: "no_entry",
    };
  }

  async *query(
    query: Query,
  ): AsyncIterable<[SignedEntry, Payload | undefined]> {
    let listToUse: SummarisableStorage<Uint8Array, Uint8Array>;
    let lowerBound: Uint8Array | undefined;
    let upperBound: Uint8Array | undefined;

    switch (query.order) {
      case "path":
        lowerBound = query.lowerBound;
        upperBound = query.upperBound;
        listToUse = this.ptaStorage;
        break;
      case "author":
        lowerBound = query.lowerBound;
        upperBound = query.upperBound;
        listToUse = this.aptStorage;
        break;
      case "timestamp": {
        if (query.lowerBound) {
          lowerBound = bigintToBytes(query.lowerBound);
        }

        if (query.upperBound) {
          upperBound = bigintToBytes(query.upperBound);
        }

        listToUse = this.tapStorage;
        break;
      }
    }

    const iterator = listToUse.entries(lowerBound, upperBound, {
      reverse: query.reverse,
      limit: query.limit,
    });

    for await (const entry of iterator) {
      const { author, path, timestamp } = detailsFromBytes(
        entry.key,
        query.order,
        this.protocolParams.pubkeyLength,
      );

      const {
        payloadHash,
        namespaceSignature,
        authorSignature,
        payloadLength,
      } = sliceSummarisableStorageValue(entry.value, this.protocolParams);

      const signedEntry: SignedEntry = {
        authorSignature: authorSignature,
        namespaceSignature: namespaceSignature,
        entry: {
          identifier: {
            author,
            namespace: this.namespace,
            path,
          },
          record: {
            hash: payloadHash,
            length: payloadLength,
            timestamp,
          },
        },
      };

      const payload = await this.payloadDriver.get(payloadHash);

      yield [signedEntry, payload];
    }
  }
}
