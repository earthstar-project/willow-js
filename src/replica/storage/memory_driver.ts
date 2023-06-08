import { compareBytes } from "../../util/bytes.ts";
import { sha256XorMonoid } from "./lifting_monoid.ts";
import { MonoidRbTree } from "./monoid_rbtree/monoid_rbtree.ts";
import { ReplicaDriver, SummarisableStorage } from "./types.ts";

export class ReplicaDriverMemory implements ReplicaDriver {
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
}
