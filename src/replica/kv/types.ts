export type KeyPart =
  | Uint8Array
  | string
  | number
  | bigint
  | boolean;
export type Key = KeyPart[];

export interface KvDriver {
  prefixLevel: number;
  get<ValueType>(key: Key): Promise<ValueType | undefined>;
  set(key: Key, value: unknown): Promise<void>;
  delete(key: Key): Promise<void>;
  list<ValueType>(
    range: { start: Key; end: Key },
    opts?: {
      prefix?: Key;
      reverse?: boolean;
      limit?: number;
      batchSize?: number;
    },
  ): AsyncIterable<{ key: Key; value: ValueType }>;
  clear(opts?: { prefix: Key; start: Key; end: Key }): Promise<void>;
}
