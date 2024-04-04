export interface SummarisableStorage<LogicalKey, LogicalValue, SummaryData> {
  get(key: LogicalKey): Promise<LogicalValue | undefined>;
  insert(key: LogicalKey, value: LogicalValue): Promise<void>;
  remove(key: LogicalKey): Promise<boolean>;

  summarise(
    start: LogicalKey,
    end: LogicalKey,
  ): Promise<{ fingerprint: SummaryData; size: number }>;

  entries(
    start: LogicalKey | undefined,
    end: LogicalKey | undefined,
    opts?: {
      reverse?: boolean;
      limit?: number;
    },
  ): AsyncIterable<{ key: LogicalKey; value: LogicalValue }>;
  allEntries(): AsyncIterable<{ key: LogicalKey; value: LogicalValue }>;
}
