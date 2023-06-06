export interface SummarisableStorage<ValueType, LiftedType> {
  get(key: ValueType): Promise<Uint8Array | undefined>;
  insert(key: ValueType, value: Uint8Array): Promise<void>;
  remove(key: ValueType): Promise<boolean>;

  summarise(
    start: ValueType,
    end: ValueType,
  ): Promise<{ fingerprint: LiftedType; size: number }>;

  entries(
    start: ValueType,
    end: ValueType,
  ): AsyncIterable<{ key: ValueType; value: Uint8Array }>;
  allEntries(): AsyncIterable<{ key: ValueType; value: Uint8Array }>;
}
