import { Path } from "../../../../deps.ts";

export interface PrefixIterator<ValueType> {
  insert(path: Path, value: ValueType): Promise<void>;
  remove(path: Path): Promise<boolean>;
  /** Return all paths that are prefixes of the given path */
  prefixesOf(path: Path): AsyncIterable<[Path, ValueType]>;
  /** Returns all paths that are prefixed by the given path */
  prefixedBy(path: Path): AsyncIterable<[Path, ValueType]>;
}
