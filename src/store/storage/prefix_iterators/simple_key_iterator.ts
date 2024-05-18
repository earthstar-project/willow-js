import { orderPath, type Path, prefixesOf } from "@earthstar/willow-utils";
import type { KvDriver } from "../kv/types.ts";
import type { PrefixIterator } from "./types.ts";

export class SimpleKeyIterator<ValueType> implements PrefixIterator<ValueType> {
  private kv: KvDriver;

  constructor(kv: KvDriver) {
    this.kv = kv;
  }

  insert(path: Path, value: ValueType): Promise<void> {
    return this.kv.set(path, value);
  }

  async remove(path: Path): Promise<true> {
    await this.kv.delete(path);

    return true;
  }

  async *prefixesOf(
    path: Path,
  ): AsyncIterable<[Path, ValueType]> {
    const prefixes = prefixesOf(path);

    for (const prefix of prefixes) {
      if (orderPath(prefix, path) >= 0) {
        break;
      }

      const value = await this.kv.get<ValueType>(prefix);

      if (!value) {
        continue;
      }

      yield [prefix, value];
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
