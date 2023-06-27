import { join } from "https://deno.land/std@0.188.0/path/mod.ts";
import { EntryDriverKvStore } from "./storage/entry_drivers/kv_store.ts";
import { PayloadDriverFilesystem } from "./storage/payload_drivers/filesystem.ts";
import { ProtocolParameters } from "./types.ts";
import { ensureDir } from "https://deno.land/std@0.188.0/fs/ensure_dir.ts";

/** Create a pair of entry and payload drivers for use with a {@link Replica} which will store their data at a given filesystem path. */
export async function getPersistedDrivers<KeypairType>(
  /** The filesystem path to store entry and payload data within. */
  path: string,
  protocolParameters: ProtocolParameters<KeypairType>,
) {
  const kvPath = join(path, "entries");
  const payloadPath = join(path, "payloads");

  await ensureDir(path);

  const kv = await Deno.openKv(kvPath);

  return {
    entryDriver: new EntryDriverKvStore(kv),
    payloadDriver: new PayloadDriverFilesystem(payloadPath, protocolParameters),
  };
}
