import { EntryDriverMemory } from "./storage/memory_driver.ts";
import {
  EntryDriver,
  PayloadDriver,
  SummarisableStorage,
} from "./storage/types.ts";
import { Entry, SignedEntry } from "../types.ts";
import { bytesEquals, deferred } from "../../deps.ts";
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
    this.namespace = opts.namespace;
    this.protocolParams = opts.format;

    const entryDriver = opts.entryDriver || new EntryDriverMemory();
    const payloadDriver = opts.payloadDriver ||
      new PayloadDriverMemory(opts.format);

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

      // Insert key for each storage.
      await Promise.all([
        this.ptaStorage.insert(keys.pta, existingInsert[1]),
        this.tapStorage.insert(keys.tap, existingInsert[1]),
        this.aptStorage.insert(keys.apt, existingInsert[1]),
      ]);

      // Unflag insert.
      await this.entryDriver.writeAheadFlag.unflagInsertion();
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

    let timestamp = BigInt(Date.now() * 1000);

    if (!input.timestamp) {
      // Get the latest timestamp from the same path and plus one.
      for await (
        const [entry] of this.query({
          order: "path",
          lowerBound: input.path,
          upperBound: incrementLastByte(input.path),
          // Only interested in the first
          limit: 1,
          // Only interested in the path with the highest timestamp.
          reverse: true,
        })
      ) {
        timestamp = entry.entry.record.timestamp + BigInt(1);
      }
    }

    // Stage it with the driver
    const stagedResult = await this.payloadDriver.stage(input.payload);

    const record = {
      timestamp,
      // TODO: Use real payload length.
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
    signedEntry: SignedEntry,
  ): Promise<IngestEvent> {
    await this.checkedWriteAheadFlag;

    if (
      !bytesEquals(
        this.namespace,
        new Uint8Array(signedEntry.entry.identifier.namespace),
      )
    ) {
      return {
        kind: "failure",
        reason: "invalid_entry",
        message: "Entry's namespace did not match replica's namespace.",
        err: null,
      };
    }

    if (await this.verify(signedEntry) === false) {
      return {
        kind: "failure",
        reason: "invalid_entry",
        message: "One or more of the entry's signatures was invalid.",
        err: null,
      };
    }

    // TODO: Check if path is prefix of another using exciting radix tree.

    const entryAuthorPathKey = entryAuthorPathBytes(signedEntry.entry);
    const entryAuthorPathKeyUpper = incrementLastByte(entryAuthorPathKey);

    for await (
      const otherEntry of this.aptStorage.entries(
        entryAuthorPathKey,
        entryAuthorPathKeyUpper,
      )
    ) {
      const otherEntryTimestampBytesView = new DataView(
        otherEntry.key.buffer,
      );

      const otherEntryTimestamp = otherEntryTimestampBytesView.getBigUint64(
        otherEntry.key.byteLength - 8,
      );

      //  If there is something existing and the timestamp is greater than ours, we have a no-op.
      if (otherEntryTimestamp > signedEntry.entry.record.timestamp) {
        return {
          kind: "no_op",
          reason: "obsolete_from_same_author",
        };
      }

      const hashOrder = compareBytes(
        new Uint8Array(signedEntry.entry.record.hash),
        otherEntry.value,
      );

      if (
        otherEntryTimestamp === signedEntry.entry.record.timestamp &&
        hashOrder === -1
      ) {
        return {
          kind: "no_op",
          reason: "obsolete_from_same_author",
        };
      }

      // TODO: Get actual payload length from driver.
      const otherPayloadLengthIsGreater =
        signedEntry.entry.record.length < otherEntryTimestamp;

      if (
        otherEntryTimestamp === signedEntry.entry.record.timestamp &&
        hashOrder === 0 && otherPayloadLengthIsGreater
      ) {
        return {
          kind: "no_op",
          reason: "obsolete_from_same_author",
        };
      }

      // Remove the old one...
      // Derive PTA and TAP keys.
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

      await this.entryDriver.writeAheadFlag.unflagRemoval();
    }

    // We passed all the criteria!
    // Store this entry.

    const keys = entryKeyBytes(
      new Uint8Array(signedEntry.entry.identifier.path),
      signedEntry.entry.record.timestamp,
      new Uint8Array(signedEntry.entry.identifier.author),
    );

    const toStore = concatSummarisableStorageValue(
      {
        payloadHash: signedEntry.entry.record.hash,
        namespaceSignature: signedEntry.namespaceSignature,
        authorSignature: signedEntry.authorSignature,
        payloadLength: signedEntry.entry.record.length,
      },
    );

    await this.entryDriver.writeAheadFlag.flagInsertion(keys.pta, toStore);

    await Promise.all([
      this.ptaStorage.insert(keys.pta, toStore),
      this.aptStorage.insert(keys.apt, toStore),
      this.tapStorage.insert(keys.tap, toStore),
    ]);

    await this.entryDriver.writeAheadFlag.unflagInsertion();

    // TODO: Store the path in a radix tree.

    return {
      kind: "success",
      entry: signedEntry,
      sourceId: "TODO...",
    };
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

      if (!compareBytes(stagedResult.hash, payloadHash)) {
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
