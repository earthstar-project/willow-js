import { Path } from "../../../../deps.ts";

/** A data structure which stores Path-value pairs, and can efficiently track which paths are prefixes of others. */
export interface PrefixIterator<ValueType> {
  insert(path: Path, value: ValueType): Promise<void>;
  remove(path: Path): Promise<boolean>;
  /** Return all paths that are prefixes of the given path */
  prefixesOf(path: Path): AsyncIterable<[Path, ValueType]>;
  /** Returns all paths that are prefixed by the given path */
  prefixedBy(path: Path): AsyncIterable<[Path, ValueType]>;
}
