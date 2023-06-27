import { SignedEntry, SignFn, VerifyFn } from "../entries/types.ts";
import { EntryDriver, PayloadDriver } from "./storage/types.ts";

export interface PayloadHashSizeTransformer
  extends TransformStream<Uint8Array, Uint8Array> {
  size: Promise<number>;
  hash: Promise<Uint8Array>;
}

export interface ProtocolParameters<KeypairType> {
  sign: SignFn<KeypairType>;
  verify: VerifyFn;
  hash: (bytes: Uint8Array | ReadableStream<Uint8Array>) => Promise<Uint8Array>;
  pubkeyLength: number;
  hashLength: number;
  signatureLength: number;
  pubkeyBytesFromPair: (pair: KeypairType) => Promise<Uint8Array>;
}

export type ReplicaOpts<KeypairType> = {
  namespace: Uint8Array;
  protocolParameters: ProtocolParameters<KeypairType>;
  entryDriver?: EntryDriver;
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
  order: QueryOrder;
  limit?: number;
  reverse?: boolean;
}

export interface PathQuery extends QueryBase {
  order: "path";
  lowerBound?: Uint8Array;
  upperBound?: Uint8Array;
}

export interface AuthorQuery extends QueryBase {
  order: "author";
  lowerBound?: Uint8Array;
  upperBound?: Uint8Array;
}

export interface TimestampQuery extends QueryBase {
  order: "timestamp";
  lowerBound?: bigint;
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

  signed: SignedEntry;
  /** An ID representing the source of this ingested entry. */
  sourceId: string;
};

export type IngestEvent =
  | IngestEventFailure
  | IngestEventNoOp
  | IngestEventSuccess;

export type Payload = {
  bytes: () => Promise<Uint8Array>;
  stream: ReadableStream<Uint8Array>;
};

export type EntryInput = {
  path: Uint8Array;
  payload: Uint8Array | ReadableStream<Uint8Array>;
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
