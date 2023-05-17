import { SignedEntry } from "./types.ts";

type ReplicaOpts = {
  storagePath: string;
};

type Query = {
  onlyLatest: boolean;
  sortBy: "path ASC" | "path DESC" | "timestamp ASC" | "timestamp DESC";
  limit: number; // 0 is no limit
};

interface ReplicaDriver {
  upsert(entry: SignedEntry): Promise<void>;
  query(query: Query): SignedEntry[];
  forget(query: Query): SignedEntry[];
}

// Here we go.
class Replica {
  constructor(opts: ReplicaOpts) {
  }
}
