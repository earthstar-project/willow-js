export type KeyPart =
  | Uint8Array
  | string
  | number
  | bigint
  | boolean
  | symbol;

export type Key = KeyPart[];

export type KvBatch = {
  set: (key: Key, value: unknown) => void;
  delete: (key: Key) => void;
  commit: () => Promise<void>;
};

export interface KvDriver {
  get<ValueType>(key: Key): Promise<ValueType | undefined>;
  set(key: Key, value: unknown): Promise<void>;
  delete(key: Key): Promise<void>;
  list<ValueType>(
    selector: { start: Key; end: Key } | { prefix: Key },
    opts?: {
      reverse?: boolean;
      limit?: number;
      batchSize?: number;
    },
  ): AsyncIterable<{ key: Key; value: ValueType }>;
  clear(opts?: { prefix: Key; start: Key; end: Key }): Promise<void>;
  batch(): KvBatch;
}
