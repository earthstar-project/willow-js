export interface PrefixIterator<ValueType> {
  insert(key: Uint8Array, value: ValueType): Promise<void>;
  remove(key: Uint8Array): Promise<boolean>;
  prefixesOf(key: Uint8Array): AsyncIterable<[Uint8Array, ValueType]>;
  prefixedBy(key: Uint8Array): AsyncIterable<[Uint8Array, ValueType]>;
}
