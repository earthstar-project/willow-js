import { ReplicaDriverMemory } from "./storage/memory_driver.ts";
import { ReplicaDriver, SummarisableStorage } from "./storage/types.ts";
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
  Payload,
  Query,
  ReplicaOpts,
  WillowFormat,
} from "./types.ts";

export class Replica<KeypairType> {
  private namespace: Uint8Array;

  private format: WillowFormat<KeypairType>;

  private ptaStorage: SummarisableStorage<Uint8Array, Uint8Array>;
  private aptStorage: SummarisableStorage<Uint8Array, Uint8Array>;
  private tapStorage: SummarisableStorage<Uint8Array, Uint8Array>;

  private driver: ReplicaDriver;

  private checkedWriteAheadFlag = deferred();

  constructor(opts: ReplicaOpts<KeypairType>) {
    this.namespace = opts.namespace;
    this.format = opts.format;

    const driver = opts.driver || new ReplicaDriverMemory();

    this.driver = driver;

    this.ptaStorage = driver.createSummarisableStorage("pta");
    this.aptStorage = driver.createSummarisableStorage("apt");
    this.tapStorage = driver.createSummarisableStorage("tap");

    this.checkWriteAheadFlag();
  }

  private async checkWriteAheadFlag() {
    const existingInsert = await this.driver.writeAheadFlag.wasInserting();
    const existingRemove = await this.driver.writeAheadFlag.wasRemoving();

    if (existingInsert) {
      const ptaKey = existingInsert[0];

      const details = detailsFromBytes(
        ptaKey,
        "path",
        this.format.pubkeyLength,
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
      await this.driver.writeAheadFlag.unflagInsertion();
    }

    if (existingRemove) {
      // Derive TAP, APT keys from PTA.
      const ptaKey = existingRemove;

      const details = detailsFromBytes(
        ptaKey,
        "path",
        this.format.pubkeyLength,
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
      await this.driver.writeAheadFlag.unflagRemoval();
    }

    this.checkedWriteAheadFlag.resolve();
  }

  private verify(signedEntry: SignedEntry) {
    return verifyEntry({
      signedEntry,
      verify: this.format.verify,
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
      sign: this.format.sign,
    });
  }

  async set(
    namespaceKeypair: KeypairType,
    authorKeypair: KeypairType,
    input: EntryInput,
  ) {
    const identifier = {
      namespace: this.namespace,
      author: await this.format.pubkeyBytesFromPair(authorKeypair),
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

    const record = {
      timestamp,
      // TODO: Use real payload length.
      length: BigInt(8),
      // TODO: Use real hash.
      hash: new Uint8Array(this.format.hashLength),
    };

    const signed = await this.sign(
      { identifier, record },
      namespaceKeypair,
      authorKeypair,
    );

    return this.ingest(signed);
  }

  async ingest(
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
        entryAuthorPathKey.byteLength,
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
      const payloadLengthOrder: number = 1;

      if (
        otherEntryTimestamp === signedEntry.entry.record.timestamp &&
        hashOrder === 0 && payloadLengthOrder === -1
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
        this.format.pubkeyLength,
      );
      const keys = entryKeyBytes(
        otherDetails.path,
        otherDetails.timestamp,
        otherDetails.author,
      );

      // TODO: This does only recovers the removal, not the entry we mean to ingest...
      await this.driver.writeAheadFlag.flagRemoval(keys.pta);

      Promise.all([
        this.ptaStorage.remove(keys.pta),
        this.tapStorage.remove(keys.tap),
        this.aptStorage.remove(keys.apt),
      ]);

      await this.driver.writeAheadFlag.unflagRemoval();
    }

    // We passed all the criteria!
    // Store this entry.

    const keys = entryKeyBytes(
      new Uint8Array(signedEntry.entry.identifier.path),
      signedEntry.entry.record.timestamp,
      new Uint8Array(signedEntry.entry.identifier.author),
    );

    //  TODO: Store signatures in here!
    const toStore = concatSummarisableStorageValue(
      signedEntry.entry.record.hash,
      signedEntry.namespaceSignature,
      signedEntry.authorSignature,
    );

    await this.driver.writeAheadFlag.flagInsertion(keys.pta, toStore);

    await Promise.all([
      this.ptaStorage.insert(keys.pta, toStore),
      this.aptStorage.insert(keys.apt, toStore),
      this.tapStorage.insert(keys.tap, toStore),
    ]);

    await this.driver.writeAheadFlag.unflagInsertion();

    // TODO: Store the path in a radix tree.

    return {
      kind: "success",
      entry: signedEntry,
      sourceId: "TODO...",
    };
  }

  async *query(query: Query): AsyncIterable<[SignedEntry, Payload]> {
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
        this.format.pubkeyLength,
      );

      const { hash, namespaceSignature, authorSignature } =
        sliceSummarisableStorageValue(entry.value, this.format);

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
            hash,
            // TODO: Return actual length.
            length: BigInt(8),
            timestamp,
          },
        },
      };

      const payload: Payload = {
        // TODO: Return actual bytes and stream.
        bytes: () => Promise.resolve(new Uint8Array()),
        stream: new ReadableStream(),
      };

      yield [signedEntry, payload];
    }
  }
}
