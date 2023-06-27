import { compareBytes } from "../../../util/bytes.ts";
import { KvDriverDeno } from "../kv/kv_driver_deno.ts";
import { PrefixedDriver } from "../kv/prefixed_driver.ts";
import { KvDriver } from "../kv/types.ts";
import { KeyHopTree } from "../prefix_iterators/key_hop_tree.ts";
import { PrefixIterator } from "../prefix_iterators/types.ts";
import { sha256XorMonoid } from "../summarisable_storage/lifting_monoid.ts";
import { Skiplist } from "../summarisable_storage/monoid_skiplist/monoid_skiplist.ts";
import { SummarisableStorage } from "../summarisable_storage/types.ts";
import { EntryDriver } from "../types.ts";

export class EntryDriverKvStore implements EntryDriver {
  private kvDriver: KvDriver;
  prefixIterator: PrefixIterator<Uint8Array>;

  constructor(kv: Deno.Kv) {
    this.kvDriver = new KvDriverDeno(kv);
    this.prefixIterator = new KeyHopTree<Uint8Array>(this.kvDriver);
  }

  createSummarisableStorage(
    address: string,
  ): SummarisableStorage<Uint8Array, Uint8Array> {
    const prefixedDriver = new PrefixedDriver([address], this.kvDriver);

    return new Skiplist({
      monoid: sha256XorMonoid,
      compare: compareBytes,
      kv: prefixedDriver,
    });
  }

  writeAheadFlag = {
    wasInserting: () => {
      return this.kvDriver.get<[Uint8Array, Uint8Array]>([
        "waf",
        "insert",
      ]);
    },
    wasRemoving: () => {
      return this.kvDriver.get<Uint8Array>([
        "waf",
        "remove",
      ]);
    },
    flagInsertion: (key: Uint8Array, value: Uint8Array) => {
      return this.kvDriver.set(["waf", "insert"], [key, value]);
    },
    flagRemoval: (key: Uint8Array) => {
      return this.kvDriver.set(["waf", "remove"], key);
    },
    unflagInsertion: () => {
      return this.kvDriver.delete(["waf", "insert"]);
    },
    unflagRemoval: () => {
      {
        return this.kvDriver.delete(["waf", "remove"]);
      }
    },
  };
}
