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
  },
): { spt: Uint8Array; pts: Uint8Array; tsp: Uint8Array } {
  const encodedSubspace = opts.subspaceEncoding.encode(opts.subspace);

  const keyLength = 8 + opts.path.byteLength +
    encodedSubspace.byteLength;

  const sptBytes = new Uint8Array(keyLength);
  const ptsBytes = new Uint8Array(keyLength);
  const tspBytes = new Uint8Array(keyLength);

  // Subspace, path, timestamp
  sptBytes.set(encodedSubspace, 0);
  sptBytes.set(
    opts.path,
    encodedSubspace.byteLength,
  );
  const sptDv = new DataView(sptBytes.buffer);
  sptDv.setBigUint64(
    encodedSubspace.byteLength + opts.path.byteLength,
    opts.timestamp,
  );

  // Path, timestamp, subspace
  ptsBytes.set(opts.path, 0);
  const ptsDv = new DataView(ptsBytes.buffer);
  ptsDv.setBigUint64(
    opts.path.byteLength,
    opts.timestamp,
  );
  ptsBytes.set(encodedSubspace, opts.path.byteLength + 8);

  // Timestamp, subspace, path
  const tapDv = new DataView(tspBytes.buffer);
  tapDv.setBigUint64(
    0,
    opts.timestamp,
  );
  tspBytes.set(encodedSubspace, 8);
  tspBytes.set(opts.path, 8 + encodedSubspace.byteLength);

  return { spt: sptBytes, pts: ptsBytes, tsp: tspBytes };
}

export function decodeEntryKey<SubspacePublicKey>(
  encoded: Uint8Array,
  order: "subspace" | "path" | "timestamp",
  subspaceEncoding: EncodingScheme<SubspacePublicKey>,
  pathLength: number,
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

      path = encoded.subarray(
        encodedSubspaceLength,
        encodedSubspaceLength + pathLength,
      );

      const dataView = new DataView(encoded.buffer);
      timestamp = dataView.getBigUint64(encoded.byteLength - 8);

      break;
    }
    case "path": {
      path = encoded.subarray(0, pathLength);

      const dataView = new DataView(encoded.buffer);

      timestamp = dataView.getBigUint64(
        pathLength,
      );

      subspace = subspaceEncoding.decode(encoded.subarray(
        pathLength + 8,
      ));

      break;
    }
    case "timestamp": {
      const dataView = new DataView(encoded.buffer);
      timestamp = dataView.getBigUint64(
        0,
      );

      path = encoded.subarray(8, 8 + pathLength);

      subspace = subspaceEncoding.decode(
        encoded.subarray(8 + pathLength),
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
    pathLength,
    pathLengthEncoding,
  }: {
    authTokenHash: PayloadDigest;
    payloadHash: PayloadDigest;
    payloadLength: bigint;
    payloadEncoding: EncodingScheme<PayloadDigest>;
    pathLength: number;
    pathLengthEncoding: EncodingScheme<number>;
  },
): Uint8Array {
  return concat(
    pathLengthEncoding.encode(pathLength),
    bigintToBytes(payloadLength),
    payloadEncoding.encode(payloadHash),
    payloadEncoding.encode(authTokenHash),
  );
}

export function decodeSummarisableStorageValue<PayloadDigest>(
  encoded: Uint8Array,
  payloadEncoding: EncodingScheme<PayloadDigest>,
  pathLengthEncoding: EncodingScheme<number>,
): {
  pathLength: number;
  payloadLength: bigint;
  payloadHash: PayloadDigest;
  authTokenHash: PayloadDigest;
} {
  const pathLength = pathLengthEncoding.decode(encoded);

  const pathLengthWidth = pathLengthEncoding.encodedLength(pathLength);

  const dataView = new DataView(encoded.buffer);

  const payloadLength = dataView.getBigUint64(pathLengthWidth);

  const payloadHash = payloadEncoding.decode(
    encoded.subarray(pathLengthWidth + 8),
  );

  const payloadHashLength = payloadEncoding.encodedLength(payloadHash);

  const authTokenHash = payloadEncoding.decode(
    encoded.subarray(pathLengthWidth + 8 + payloadHashLength),
  );

  return {
    pathLength,
    payloadLength,
    payloadHash,
    authTokenHash,
  };
}