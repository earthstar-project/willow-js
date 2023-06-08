export interface SummarisableStorage<ValueType, LiftedType> {
  get(key: ValueType): Promise<Uint8Array | undefined>;
  insert(key: ValueType, value: Uint8Array): Promise<void>;
  remove(key: ValueType): Promise<boolean>;

  summarise(
    start: ValueType,
    end: ValueType,
  ): Promise<{ fingerprint: LiftedType; size: number }>;

  entries(
    start: ValueType | undefined,
    end: ValueType | undefined,
    opts?: {
      reverse?: boolean;
      limit?: number;
    },
  ): AsyncIterable<{ key: ValueType; value: Uint8Array }>;
  allEntries(): AsyncIterable<{ key: ValueType; value: Uint8Array }>;
}

export interface WriteAheadFlag<ValueType, KeyType> {
  wasInserting: () => Promise<[KeyType, ValueType] | undefined>;
  wasRemoving: () => Promise<KeyType | undefined>;
  flagInsertion: (key: KeyType, value: ValueType) => Promise<void>;
  unflagInsertion: () => Promise<void>;
  flagRemoval: (key: KeyType) => Promise<void>;
  unflagRemoval: () => Promise<void>;
}

export interface ReplicaDriver {
  createSummarisableStorage: (
    id: string,
  ) => SummarisableStorage<Uint8Array, Uint8Array>;
  writeAheadFlag: WriteAheadFlag<Uint8Array, Uint8Array>;
}
