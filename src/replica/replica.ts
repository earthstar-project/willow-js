import { ReplicaDriverMemory } from "./storage/memory_driver.ts";
import { ReplicaDriver, SummarisableStorage } from "./storage/types.ts";
import { Entry, SignedEntry } from "../types.ts";
import { ui8Equals } from "../../deps.ts";
import { SignFn, VerifyFn } from "../sign_verify/types.ts";
import { signEntry, verifyEntry } from "../sign_verify/sign_verify.ts";
import {
  bigintToBytes,
  compareBytes,
  detailsFromBytes,
  entryAuthorPathBytes,
  entryKeyBytes,
  incrementLastByte,
} from "../util/bytes.ts";
import { deferred } from "https://deno.land/std@0.188.0/async/deferred.ts";

interface WillowFormat<KeypairType> {
  sign: SignFn<KeypairType>;
  verify: VerifyFn;
  pubkeyLength: number;
  hashLength: number;
  pubkeyBytesFromPair: (pair: KeypairType) => Promise<Uint8Array>;
}

type ReplicaOpts<KeypairType> = {
  namespace: Uint8Array;
  driver?: ReplicaDriver;
  format: WillowFormat<KeypairType>;
};

type QueryOrder =
  /** By path, then timestamp, then author */
  | "path"
  /** By timestamp, then author, then path */
  | "timestamp"
  /** By author, then path, then timestamp */
  | "author";

interface QueryBase {
  order: QueryOrder;
  limit?: number;
  reverse?: boolean;
}

interface PathQuery extends QueryBase {
  order: "path";
  lowerBound?: Uint8Array;
  upperBound?: Uint8Array;
}

interface AuthorQuery extends QueryBase {
  order: "author";
  lowerBound?: Uint8Array;
  upperBound?: Uint8Array;
}

interface TimestampQuery extends QueryBase {
  order: "timestamp";
  lowerBound?: bigint;
  upperBound?: bigint;
}

type Query = PathQuery | AuthorQuery | TimestampQuery;

type IngestEventFailure = {
  kind: "failure";
  reason: "write_failure" | "invalid_entry";
  message: string;
  err: Error | null;
};

type IngestEventNoOp = {
  kind: "no_op";
  reason: "obsolete_from_same_author";
};

type IngestEventSuccess = {
  kind: "success";
  entry: SignedEntry;
  /** An ID representing the source of this ingested entry. */
  sourceId: string;
};

type IngestEvent = IngestEventFailure | IngestEventNoOp | IngestEventSuccess;

type Payload = {
  bytes: () => Promise<Uint8Array>;
  stream: ReadableStream<Uint8Array>;
};

type EntryInput = {
  path: Uint8Array;
  payload: Uint8Array | ReadableStream<Uint8Array>;
  timestamp?: bigint;
};

export class Replica<KeypairType> {
  private namespace: Uint8Array;

  private format: WillowFormat<KeypairType>;

  private ptaList: SummarisableStorage<Uint8Array, Uint8Array>;
  private aptList: SummarisableStorage<Uint8Array, Uint8Array>;
  private tapList: SummarisableStorage<Uint8Array, Uint8Array>;

  private driver: ReplicaDriver;

  private checkedWriteAheadFlag = deferred();

  constructor(opts: ReplicaOpts<KeypairType>) {
    this.namespace = opts.namespace;
    this.format = opts.format;

    const driver = opts.driver || new ReplicaDriverMemory();

    this.driver = driver;

    this.ptaList = driver.createSummarisableStorage("pta");
    this.aptList = driver.createSummarisableStorage("apt");
    this.tapList = driver.createSummarisableStorage("tap");

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
        this.ptaList.remove(keys.pta),
        this.tapList.remove(keys.tap),
        this.aptList.remove(keys.apt),
      ]);

      // Insert key for each storage.
      await Promise.all([
        this.ptaList.insert(keys.pta, existingInsert[1]),
        this.tapList.insert(keys.tap, existingInsert[1]),
        this.aptList.insert(keys.apt, existingInsert[1]),
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
        this.ptaList.remove(keys.pta),
        this.tapList.remove(keys.tap),
        this.aptList.remove(keys.apt),
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
      !ui8Equals(
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
      const otherEntry of this.aptList.entries(
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
        this.ptaList.remove(keys.pta),
        this.tapList.remove(keys.tap),
        this.aptList.remove(keys.apt),
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
    const hashBytes = new Uint8Array(signedEntry.entry.record.hash);

    await this.driver.writeAheadFlag.flagInsertion(keys.pta, hashBytes);

    await Promise.all([
      this.ptaList.insert(keys.pta, hashBytes),
      this.aptList.insert(keys.apt, hashBytes),
      this.tapList.insert(keys.tap, hashBytes),
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
        listToUse = this.ptaList;
        break;
      case "author":
        lowerBound = query.lowerBound;
        upperBound = query.upperBound;
        listToUse = this.aptList;
        break;
      case "timestamp": {
        if (query.lowerBound) {
          lowerBound = bigintToBytes(query.lowerBound);
        }

        if (query.upperBound) {
          upperBound = bigintToBytes(query.upperBound);
        }

        listToUse = this.tapList;
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

      const signedEntry: SignedEntry = {
        // TODO: Retrieve real signatures!
        authorSignature: new Uint8Array(),
        namespaceSignature: new Uint8Array(),
        entry: {
          identifier: {
            author,
            namespace: this.namespace,
            path,
          },
          record: {
            hash: entry.value,
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
