import { isPathPrefixed, orderPath, Path } from "../../../../deps.ts";
import { KvDriver } from "../kv/types.ts";
import { PrefixIterator } from "./types.ts";

export class SimpleKeyIterator<ValueType> implements PrefixIterator<ValueType> {
  private kv: KvDriver;

  constructor(kv: KvDriver) {
    this.kv = kv;
  }

  insert(path: Path, value: ValueType) {
    return this.kv.set(path, value);
  }

  async remove(path: Path) {
    await this.kv.delete(path);

    return true;
  }

  async *prefixesOf(
    path: Path,
    atLeast: Path = [],
  ): AsyncIterable<[Path, ValueType]> {
    for await (
      const entry of this.kv.list<ValueType>({
        start: atLeast,
        end: path,
      }, {
        batchSize: path.length === 0 ? 1 : undefined,
        limit: path.length === 0 ? 1 : undefined,
      })
    ) {
      const candidate = entry.key as Path;

      // If the candidate is greater than or equal to the current path, we've reached the end of the line.
      if (orderPath(candidate, path) >= 0) {
        break;
      }

      if (isPathPrefixed(candidate, path)) {
        yield [candidate, entry.value];

        for await (
          const result of this.prefixesOf(
            path,
            path.slice(0, candidate.length + 1),
          )
        ) {
          yield result;
        }

        break;
      }
    }
  }

  async *prefixedBy(path: Path): AsyncIterable<[Path, ValueType]> {
    for await (
      const entry of this.kv.list<ValueType>({
        prefix: path,
      })
    ) {
      yield [entry.key as Path, entry.value];
    }
  }
}
