import { join } from "https://deno.land/std@0.188.0/path/mod.ts";
import { EntryDriverKvStore } from "./storage/entry_drivers/kv_store.ts";
import { PayloadDriverFilesystem } from "./storage/payload_drivers/filesystem.ts";
import { EncodingScheme, ProtocolParameters } from "./types.ts";
import { ensureDir } from "https://deno.land/std@0.188.0/fs/ensure_dir.ts";
import { bigintToBytes } from "../util/bytes.ts";
import { concat } from "$std/bytes/concat.ts";

/** Create a pair of entry and payload drivers for use with a {@link Replica} which will store their data at a given filesystem path. */
export async function getPersistedDrivers<
  NamespacePublicKey,
  SubspacePublicKey,
  PayloadDigest,
  AuthorisationOpts,
  AuthorisationToken,
>(
  /** The filesystem path to store entry and payload data within. */
  path: string,
  protocolParameters: ProtocolParameters<
    NamespacePublicKey,
    SubspacePublicKey,
    PayloadDigest,
    AuthorisationOpts,
    AuthorisationToken
  >,
) {
  const kvPath = join(path, "entries");
  const payloadPath = join(path, "payloads");

  await ensureDir(path);

  const kv = await Deno.openKv(kvPath);

  return {
    entryDriver: new EntryDriverKvStore(kv),
    payloadDriver: new PayloadDriverFilesystem(
      payloadPath,
      protocolParameters.payloadScheme,
    ),
  };
}

// Keys

export function encodeEntryKeys<SubspacePublicKey>(
  opts: {
    path: Uint8Array;
    timestamp: bigint;
    subspace: SubspacePublicKey;
    subspaceEncoding: EncodingScheme<SubspacePublicKey>;
    pathEncoding: EncodingScheme<Uint8Array>;
  },
): { spt: Uint8Array; pts: Uint8Array; tsp: Uint8Array } {
  const encodedSubspace = opts.subspaceEncoding.encode(opts.subspace);
  const encodedPath = opts.pathEncoding.encode(opts.path);

  const keyLength = 8 + encodedPath.byteLength +
    encodedSubspace.byteLength;

  const sptBytes = new Uint8Array(keyLength);
  const ptsBytes = new Uint8Array(keyLength);
  const tspBytes = new Uint8Array(keyLength);

  // Subspace, path, timestamp
  sptBytes.set(encodedSubspace, 0);
  sptBytes.set(
    encodedPath,
    encodedSubspace.byteLength,
  );
  const sptDv = new DataView(sptBytes.buffer);
  sptDv.setBigUint64(
    encodedSubspace.byteLength + encodedPath.byteLength,
    opts.timestamp,
  );

  // Path, timestamp, subspace
  ptsBytes.set(encodedPath, 0);
  const ptsDv = new DataView(ptsBytes.buffer);
  ptsDv.setBigUint64(
    encodedPath.byteLength,
    opts.timestamp,
  );
  ptsBytes.set(encodedSubspace, encodedPath.byteLength + 8);

  // Timestamp, subspace, path
  const tapDv = new DataView(tspBytes.buffer);
  tapDv.setBigUint64(
    0,
    opts.timestamp,
  );
  tspBytes.set(encodedSubspace, 8);
  tspBytes.set(encodedPath, 8 + encodedSubspace.byteLength);

  return { spt: sptBytes, pts: ptsBytes, tsp: tspBytes };
}

export function decodeEntryKey<SubspacePublicKey>(
  encoded: Uint8Array,
  order: "subspace" | "path" | "timestamp",
  subspaceEncoding: EncodingScheme<SubspacePublicKey>,
  pathEncoding: EncodingScheme<Uint8Array>,
): {
  subspace: SubspacePublicKey;
  path: Uint8Array;
  timestamp: bigint;
} {
  let subspace;
  let timestamp;
  let path;

  switch (order) {
    case "subspace": {
      subspace = subspaceEncoding.decode(encoded);

      const encodedSubspaceLength = subspaceEncoding.encodedLength(subspace);

      path = pathEncoding.decode(encoded.subarray(encodedSubspaceLength));

      const dataView = new DataView(encoded.buffer);
      timestamp = dataView.getBigUint64(encoded.byteLength - 8);

      break;
    }
    case "path": {
      path = pathEncoding.decode(encoded);

      const encodedPathLength = pathEncoding.encodedLength(path);

      const dataView = new DataView(encoded.buffer);

      timestamp = dataView.getBigUint64(
        encodedPathLength,
      );

      subspace = subspaceEncoding.decode(encoded.subarray(
        encodedPathLength + 8,
      ));

      break;
    }
    case "timestamp": {
      const dataView = new DataView(encoded.buffer);
      timestamp = dataView.getBigUint64(
        0,
      );

      path = pathEncoding.decode(encoded.subarray(8));

      const encodedPathLength = pathEncoding.encodedLength(path);

      subspace = subspaceEncoding.decode(
        encoded.subarray(8 + encodedPathLength),
      );
    }
  }

  return {
    subspace,
    path,
    timestamp,
  };
}

export function encodeSummarisableStorageValue<PayloadDigest>(
  {
    authTokenHash,
    payloadHash,
    payloadLength,
    payloadEncoding,
  }: {
    authTokenHash: PayloadDigest;
    payloadHash: PayloadDigest;
    payloadLength: bigint;
    payloadEncoding: EncodingScheme<PayloadDigest>;
  },
): Uint8Array {
  return concat(
    bigintToBytes(payloadLength),
    payloadEncoding.encode(payloadHash),
    payloadEncoding.encode(authTokenHash),
  );
}

export function decodeSummarisableStorageValue<PayloadDigest>(
  encoded: Uint8Array,
  payloadEncoding: EncodingScheme<PayloadDigest>,
): {
  payloadLength: bigint;
  payloadHash: PayloadDigest;
  authTokenHash: PayloadDigest;
} {
  const dataView = new DataView(encoded.buffer);

  const payloadLength = dataView.getBigUint64(0);

  const payloadHash = payloadEncoding.decode(encoded.subarray(8));

  const payloadHashLength = payloadEncoding.encodedLength(payloadHash);

  const authTokenHash = payloadEncoding.decode(
    encoded.subarray(8 + payloadHashLength),
  );

  return {
    payloadLength,
    payloadHash,
    authTokenHash,
  };
}

export function isPrefixOf(testing: Uint8Array, bytes: Uint8Array) {
  if (testing.byteLength >= bytes.byteLength) {
    return false;
  }

  for (let i = 0; i < testing.length; i++) {
    if (testing[i] !== bytes[i]) {
      return false;
    }
  }

  return true;
}
