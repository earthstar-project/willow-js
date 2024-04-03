export interface SummarisableStorage<LogicaKey, SummaryData> {
  get(key: LogicaKey): Promise<Uint8Array | undefined>;
  insert(key: LogicaKey, value: Uint8Array): Promise<void>;
  remove(key: LogicaKey): Promise<boolean>;

  summarise(
    start: LogicaKey,
    end: LogicaKey,
  ): Promise<{ fingerprint: SummaryData; size: number }>;

  entries(
    start: LogicaKey | undefined,
    end: LogicaKey | undefined,
    opts?: {
      reverse?: boolean;
      limit?: number;
    },
  ): AsyncIterable<{ key: LogicaKey; value: Uint8Array }>;
  allEntries(): AsyncIterable<{ key: LogicaKey; value: Uint8Array }>;
}
