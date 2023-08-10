import { ValidationError } from "../../errors.ts";
import { Payload } from "../types.ts";
import { PrefixIterator } from "./prefix_iterators/types.ts";
import { SummarisableStorage } from "./summarisable_storage/types.ts";

export interface WriteAheadFlag<ValueType, KeyType> {
  wasInserting: () => Promise<[KeyType, ValueType] | undefined>;
  wasRemoving: () => Promise<KeyType | undefined>;
  flagInsertion: (key: KeyType, value: ValueType) => Promise<void>;
  unflagInsertion: () => Promise<void>;
  flagRemoval: (key: KeyType) => Promise<void>;
  unflagRemoval: () => Promise<void>;
}

/** Provides methods for storing and retrieving entries for a {@link Replica}. */
export interface EntryDriver {
  /** Creates a {@link SummarisableStorage} with a given ID, used for storing entries and their data. */
  createSummarisableStorage: (
    id: string,
  ) => SummarisableStorage<Uint8Array, Uint8Array>;
  /** Helps a Replica recover from unexpected shutdowns mid-write. */
  writeAheadFlag: WriteAheadFlag<Uint8Array, Uint8Array>;
  /** Used to find paths that are prefixes of, or prefixed by, another path. */
  prefixIterator: PrefixIterator<Uint8Array>;
}

/**  */
export interface PayloadDriver {
  /** Returns an payload for a given format and hash.*/
  get(
    payloadHash: Uint8Array,
    opts?: {
      startOffset?: number;
    },
  ): Promise<Payload | undefined>;

  /** Stores the payload in a staging area, and returns an object used to assess whether it is what we're expecting, and if so, commit it to canonical storage. */
  stage(
    payload: Uint8Array | ReadableStream<Uint8Array>,
  ): Promise<
    {
      hash: Uint8Array;
      length: number;
      /** Commit the staged attachment to storage. */
      commit: () => Promise<Payload>;
      /** Reject the staged attachment, erasing it. */
      reject: () => Promise<void>;
    }
  >;

  /** Erases an payload for a given format and hash.*/
  erase(
    payloadHash: Uint8Array,
  ): Promise<true | ValidationError>;
}
