import { ValidationError } from "../../errors.ts";
import { Payload } from "../types.ts";
import { SummarisableStorage } from "./summarisable_storage/types.ts";

export interface WriteAheadFlag<ValueType, KeyType> {
  wasInserting: () => Promise<[KeyType, ValueType] | undefined>;
  wasRemoving: () => Promise<KeyType | undefined>;
  flagInsertion: (key: KeyType, value: ValueType) => Promise<void>;
  unflagInsertion: () => Promise<void>;
  flagRemoval: (key: KeyType) => Promise<void>;
  unflagRemoval: () => Promise<void>;
}

export interface EntryDriver {
  createSummarisableStorage: (
    id: string,
  ) => SummarisableStorage<Uint8Array, Uint8Array>;
  writeAheadFlag: WriteAheadFlag<Uint8Array, Uint8Array>;
}
export interface PayloadDriver {
  /** Returns an attachment for a given format and hash.*/
  get(
    payloadHash: Uint8Array,
    opts?: {
      startOffset?: number;
    },
  ): Promise<Payload | undefined>;

  /** Upserts the attachment to a staging area, and returns an object used to assess whether it is what we're expecting. */
  stage(
    payload: Uint8Array | ReadableStream<Uint8Array>,
  ): Promise<
    {
      hash: Uint8Array;
      length: number;
      /** Commit the staged attachment to storage. */
      commit: () => Promise<void>;
      /** Reject the staged attachment, erasing it. */
      reject: () => Promise<void>;
    }
  >;

  /** Erases an attachment for a given format and hash.*/
  erase(
    payloadHash: Uint8Array,
  ): Promise<true | ValidationError>;
}
