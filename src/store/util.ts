import { join } from "https://deno.land/std@0.188.0/path/mod.ts";
import { EntryDriverKvStore } from "./storage/entry_drivers/kv_store.ts";
import { PayloadDriverFilesystem } from "./storage/payload_drivers/filesystem.ts";
import { PayloadScheme, ProtocolParameters } from "./types.ts";
import { ensureDir } from "https://deno.land/std@0.188.0/fs/ensure_dir.ts";
import { bigintToBytes, concat, EncodingScheme, Path } from "../../deps.ts";
import { KvDriverDeno } from "./storage/kv/kv_driver_deno.ts";

/** Create a pair of entry and payload drivers for use with a {@link Store} which will store their data at a given filesystem path. */
export async function getPersistedDrivers<
  NamespacePublicKey,
  SubspacePublicKey,
  PayloadDigest,
  AuthorisationOpts,
  AuthorisationToken,
  Fingerprint,
>(
  /** The filesystem path to store entry and payload data within. */
  path: string,
  protocolParameters: ProtocolParameters<
    NamespacePublicKey,
    SubspacePublicKey,
    PayloadDigest,
    AuthorisationOpts,
    AuthorisationToken,
    Fingerprint
  >,
) {
  const kvPath = join(path, "entries");
  const payloadPath = join(path, "payloads");

  await ensureDir(path);

  // TODO: Use the platform appropriate KV driver.
  const kv = await Deno.openKv(kvPath);

  return {
    entryDriver: new EntryDriverKvStore({
      ...protocolParameters,
      kvDriver: new KvDriverDeno(kv),
    }),
    payloadDriver: new PayloadDriverFilesystem(
      payloadPath,
      protocolParameters.payloadScheme,
    ),
  };
}
