export type KeyPart =
  | Uint8Array
  | string
  | number
  | bigint
  | boolean
  | symbol;

export type KvBatch<Key extends KeyPart[], Value> = {
  set: (key: Key, value: Value) => void;
  delete: (key: Key) => void;
  commit: () => Promise<void>;
};

export interface KvDriver<Key extends KeyPart[], Value> {
  get(key: Key): Promise<Value | undefined>;
  set(key: Key, value: Value): Promise<void>;
  delete(key: Key): Promise<void>;
  list(
    selector: { start: Key; end: Key } | { prefix: Key },
    opts?: {
      reverse?: boolean;
      limit?: number;
      batchSize?: number;
    },
  ): AsyncIterable<{ key: Key; value: Value }>;
  clear(opts?: { prefix: Key; start: Key; end: Key }): Promise<void>;
  batch(): KvBatch<Key, Value>;
}
