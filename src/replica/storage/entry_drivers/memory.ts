import { compareBytes } from "../../../util/bytes.ts";
import { RadixishTree } from "../prefix_iterators/radixish_tree.ts";
import { sha256XorMonoid } from "../summarisable_storage/lifting_monoid.ts";
import { MonoidRbTree } from "../summarisable_storage/monoid_rbtree.ts";
import { SummarisableStorage } from "../summarisable_storage/types.ts";
import { EntryDriver } from "../types.ts";

/** Store and retrieve entries in memory. */
export class EntryDriverMemory implements EntryDriver {
  createSummarisableStorage(): SummarisableStorage<Uint8Array, Uint8Array> {
    return new MonoidRbTree({
      monoid: sha256XorMonoid,
      compare: compareBytes,
    });
  }
  writeAheadFlag = {
    wasInserting: () => Promise.resolve(undefined),
    wasRemoving: () => Promise.resolve(undefined),
    flagInsertion: () => Promise.resolve(undefined),
    flagRemoval: () => Promise.resolve(undefined),
    unflagInsertion: () => Promise.resolve(undefined),
    unflagRemoval: () => Promise.resolve(undefined),
  };
  prefixIterator = new RadixishTree<Uint8Array>();
}
