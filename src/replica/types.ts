import { SignedEntry, SignFn, VerifyFn } from "../entries/types.ts";
import { EntryDriver, PayloadDriver } from "./storage/types.ts";

/** Concrete parameters peculiar to a specific usage of Willow. */
export interface ProtocolParameters<KeypairType> {
  /** The function used to sign entries. */
  sign: SignFn<KeypairType>;
  /** The function use to verify entries. */
  verify: VerifyFn;
  /** The function used to create hashes for blobs or streams of data. */
  hash: (bytes: Uint8Array | ReadableStream<Uint8Array>) => Promise<Uint8Array>;
  /** The byte-length of keypair public keys. */
  pubkeyLength: number;
  /** The byte-length of hashes. */
  hashLength: number;
  /** The byte-length of signatures. */
  signatureLength: number;
  /** A function to extract a public key from a keypair as bytes. */
  pubkeyBytesFromPair: (pair: KeypairType) => Promise<Uint8Array>;
}

export type ReplicaOpts<KeypairType> = {
  /** The public key of the namespace this replica is a snapshot of. */
  namespace: Uint8Array;
  /** The protocol parameters this replica should use. */
  protocolParameters: ProtocolParameters<KeypairType>;
  /** An optional driver used to store and retrieve a replica's entries. */
  entryDriver?: EntryDriver;
  /** An option driver used to store and retrieve a replica's payloads.  */
  payloadDriver?: PayloadDriver;
};

export type QueryOrder =
  /** By path, then timestamp, then author */
  | "path"
  /** By timestamp, then author, then path */
  | "timestamp"
  /** By author, then path, then timestamp */
  | "author";

export interface QueryBase {
  /** The order to return results in. */
  order: QueryOrder;
  /** The maximum number of results to return. */
  limit?: number;
  /** Whether the results should be returned in reverse order. */
  reverse?: boolean;
}

export interface PathQuery extends QueryBase {
  order: "path";
  /** The path to start returning results from, inclusive. Starts from the first entry in the replica if left undefined. */
  lowerBound?: Uint8Array;
  /** The path to stop returning results at, exclusive. Stops after the last entry in the replica if  undefined. */
  upperBound?: Uint8Array;
}

export interface AuthorQuery extends QueryBase {
  order: "author";
  /** The author public key to start returning results from, inclusive. Starts from the first entry in the replica if left undefined. */
  lowerBound?: Uint8Array;
  /** The author public key to stop returning results at, exclusive. Stops after the last entry in the replica if  undefined. */
  upperBound?: Uint8Array;
}

export interface TimestampQuery extends QueryBase {
  order: "timestamp";
  /** The timestamp to start returning results from, inclusive. Starts from the first entry in the replica if left undefined. */
  lowerBound?: bigint;
  /** The timestamp to stop returning results at, exclusive. Stops after the last entry in the replica if  undefined. */
  upperBound?: bigint;
}

export type Query = PathQuery | AuthorQuery | TimestampQuery;

export type IngestEventFailure = {
  kind: "failure";
  reason: "write_failure" | "invalid_entry";
  message: string;
  err: Error | null;
};

export type IngestEventNoOp = {
  kind: "no_op";
  reason: "obsolete_from_same_author" | "newer_prefix_found";
};

export type IngestEventSuccess = {
  kind: "success";
  /** The successfully ingested signed entry. */
  signed: SignedEntry;
  /** An ID representing the source of this ingested entry. */
  externalSourceId?: string;
};

export type IngestEvent =
  | IngestEventFailure
  | IngestEventNoOp
  | IngestEventSuccess;

/** The data associated with a {@link SignedEntry}. */
export type Payload = {
  /** Retrieves the payload's data all at once in a single {@link Uint8Array}. */
  bytes: () => Promise<Uint8Array>;
  /** A {@link ReadableStream} of the payload's data which can be read chunk by chunk. */
  stream: ReadableStream<Uint8Array>;
};

export type EntryInput = {
  path: Uint8Array;
  payload: Uint8Array | ReadableStream<Uint8Array>;
  /** The desired timestamp for the new entry. If left undefined, uses the current time, OR if another entry exists at the same path will be that entry's timestamp + 1. */
  timestamp?: bigint;
};

export type IngestPayloadEventFailure = {
  kind: "failure";
  reason: "no_entry" | "mismatched_hash";
};

export type IngestPayloadEventNoOp = {
  kind: "no_op";
  reason: "already_have_it";
};

export type IngestPayloadEventSuccess = {
  kind: "success";
};

export type IngestPayloadEvent =
  | IngestPayloadEventFailure
  | IngestPayloadEventNoOp
  | IngestPayloadEventSuccess;
