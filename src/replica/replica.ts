import { Skiplist } from "./monoid_skiplist/monoid_skiplist.ts";
import { KVDriver } from "./kv/types.ts";

type ReplicaOpts = {
  namespace: Uint8Array;
  recordsDriver?: KVDriver;
};

class Replica {
  private ptaList: Skiplist;
  private aptList: Skiplist;
  private tpaList: Skiplist;

  constructor(opts: ReplicaOpts) {
  }
}
