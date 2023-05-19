import { ui8Equals } from "../deps.ts";
import { isErr, ValidationError } from "./errors.ts";
import { SignedEntry } from "./types.ts";

type Query = {
  onlyLatest: boolean;
  sortBy: "path ASC" | "path DESC" | "timestamp ASC" | "timestamp DESC";
  limit: number; // 0 is no limit
  filter?: {
    author?: Uint8Array;
    path?: Uint8Array;
    pathStartsWith?: Uint8Array;
    pathEndsWith?: Uint8Array;
    timestampGt?: bigint;
    timestampLt?: bigint;
    timestamp?: bigint;
    payloadGt?: bigint;
    payloadLt?: bigint;
  };
};

interface EntryDriver {
  upsert(entry: SignedEntry): Promise<void>;
  query(query: Query): Promise<SignedEntry[]>;
  forget(query: Query): Promise<SignedEntry[]>;
  destroy(): Promise<void>;
}

type Payload = {
  stream: ReadableStream<Uint8Array>;
  bytes: () => Promise<Uint8Array>;
};

// Given how foggy payload sync is to me at this point, this is probably way off.
// Just going off of what is in Earthstar for now.
interface PayloadDriver {
  get(hash: Uint8Array): Promise<Payload | undefined>;
  erase(hash: Uint8Array): Promise<boolean>;
  // This is how I dealt with not being able to know if something matched a hash until I received all the pieces, but not keeping the whole thing in memory.
  stage(
    payload: Uint8Array | ReadableStream<Uint8Array>,
  ): Promise<
    {
      hash: string;
      size: number;
      commit: () => Promise<void>;
      reject: () => Promise<void>;
    }
  >;
  destroy(): Promise<void>;
}

type ReplicaOpts = {
  namespace: Uint8Array;
  entryDriver: EntryDriver;
  payloadDriver: PayloadDriver;
  // Should we divide signature verification from other injectable rules, e.g. path format?
  validateEntry: (entry: SignedEntry) => Promise<true | ValidationError>;
};

type IngestEventFailure = {
  kind: "failure";
  reason: "write_error" | "invalid_entry";
  err: Error | null;
};
type IngestEventNothingHappened = {
  kind: "nothing_happened";
  reason: "obsolete_from_same_author" | "already_had_it";
  entry: SignedEntry;
};

type IngestEventSuccess = {
  kind: "success";

  entry: SignedEntry; // the just-written doc, frozen, with updated extra properties like _localIndex

  /** An ID representing the source of this ingested entry. */
  sourceId: string;
};

type IngestEvent =
  | IngestEventSuccess
  | IngestEventNothingHappened
  | IngestEventFailure;

class Replica extends EventTarget {
  namespace: Uint8Array;

  private entryDriver: EntryDriver;
  private payloadDriver: PayloadDriver;
  private validateEntry: (
    entry: SignedEntry,
  ) => Promise<true | ValidationError>;

  constructor(opts: ReplicaOpts) {
    super();

    this.namespace = opts.namespace;
    this.entryDriver = opts.entryDriver;
    this.payloadDriver = opts.payloadDriver;
    this.validateEntry = opts.validateEntry;
  }

  async query(query: Query): Promise<[SignedEntry, Payload | undefined][]> {
    const entries = await this.entryDriver.query(query);

    const results: [SignedEntry, Payload | undefined][] = [];

    for (const entry of entries) {
      const payloadResult = await this.payloadDriver.get(
        new Uint8Array(entry.entry.record.hash),
      );

      const tuple = [entry, payloadResult] as [
        SignedEntry,
        Payload | undefined,
      ];

      results.push(tuple);
    }

    return results;
  }

  async ingestEntry(
    entry: SignedEntry,
    payload?: Uint8Array | ReadableStream<Uint8Array>,
  ): Promise<IngestEvent> {
    // Does it belong to this namespace?
    if (
      !ui8Equals(
        new Uint8Array(entry.entry.identifier.namespace),
        this.namespace,
      )
    ) {
      return {
        kind: "failure",
        reason: "invalid_entry",
        err: new ValidationError("Entry belongs to another namespace"),
      };
    }

    // Is it valid?
    const validateResult = this.validateEntry(entry);

    if (isErr(validateResult)) {
      return {
        kind: "failure",
        reason: "invalid_entry",
        err: validateResult,
      };
    }

    // Same author, path / path that is prefix of incoming - remove the one with the smaller timestamp
    // How do we efficiently find other docs that are prefixes of this one..?

    // Same path, author, and timestamp - remove the one with the smaller hash.

    // Same path, author, timestamp, hash - remove the one with the smaller length.

    await this.entryDriver.upsert(entry);

    //

    return {
      kind: "success",
      entry: entry,
      sourceId: "blabla",
    };
  }

  async ingestPayload(
    path: Uint8Array,
    author: Uint8Array,
    payload: Uint8Array | ReadableStream<Uint8Array>,
  ): Promise<IngestEvent>;

  forget(query: Query): Promise<SignedEntry[]>;
}
