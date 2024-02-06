import { join } from "https://deno.land/std@0.188.0/path/mod.ts";
import { EntryDriverKvStore } from "./storage/entry_drivers/kv_store.ts";
import { PayloadDriverFilesystem } from "./storage/payload_drivers/filesystem.ts";
import { PayloadScheme, ProtocolParameters } from "./types.ts";
import { ensureDir } from "https://deno.land/std@0.188.0/fs/ensure_dir.ts";
import { bigintToBytes, concat, EncodingScheme, Path } from "../../deps.ts";
import { KvDriverDeno } from "./storage/kv/kv_driver_deno.ts";

/** Create a pair of entry and payload drivers for use with a {@link Replica} which will store their data at a given filesystem path. */
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

// Keys

export function encodePathWithSeparators(path: Path): Uint8Array {
  const encodedComponents: Uint8Array[] = [];

  for (const component of path) {
    const bytes: number[] = [];

    for (const byte of component) {
      if (byte !== 0) {
        bytes.push(byte);
        continue;
      }

      bytes.push(0, 1);
    }

    bytes.push(0, 0);
    const encodedComponent = new Uint8Array(bytes);
    encodedComponents.push(encodedComponent);
  }

  return concat(...encodedComponents);
}

export function decodePathWithSeparators(
  encoded: Uint8Array,
): Path {
  const path: Path = [];

  let currentComponentBytes = [];
  let previousWasZero = false;

  for (const byte of encoded) {
    if (previousWasZero && byte === 0) {
      // Separator
      previousWasZero = false;

      const component = new Uint8Array(currentComponentBytes);

      path.push(component);

      currentComponentBytes = [];

      continue;
    }

    if (previousWasZero && byte === 1) {
      // Encoded zero.
      currentComponentBytes.push(0);
      previousWasZero = false;
      continue;
    }

    if (byte === 0) {
      previousWasZero = true;
      continue;
    }

    currentComponentBytes.push(byte);
    previousWasZero = false;
  }

  return path;
}

export function encodeEntryKeys<SubspacePublicKey>(
  opts: {
    path: Path;
    timestamp: bigint;
    subspace: SubspacePublicKey;
    subspaceEncoding: EncodingScheme<SubspacePublicKey>;
  },
): {
  spt: Uint8Array;
  pts: Uint8Array;
  tsp: Uint8Array;
  encodedPathLength: number;
} {
  const encodedSubspace = opts.subspaceEncoding.encode(opts.subspace);

  const encodedPath = encodePathWithSeparators(opts.path);

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

  return {
    spt: sptBytes,
    pts: ptsBytes,
    tsp: tspBytes,
    encodedPathLength: encodedPath.byteLength,
  };
}

export function decodeEntryKey<SubspacePublicKey>(
  encoded: Uint8Array,
  order: "subspace" | "path" | "timestamp",
  subspaceEncoding: EncodingScheme<SubspacePublicKey>,
  encodedPathLength: number,
): {
  subspace: SubspacePublicKey;
  path: Path;
  timestamp: bigint;
} {
  let subspace: SubspacePublicKey;
  let timestamp: bigint;
  let path: Path;

  switch (order) {
    case "subspace": {
      subspace = subspaceEncoding.decode(encoded);

      const encodedSubspaceLength = subspaceEncoding.encodedLength(subspace);

      const pathComponentPos = encodedSubspaceLength;

      path = decodePathWithSeparators(
        encoded.subarray(
          pathComponentPos,
          pathComponentPos + encodedPathLength,
        ),
      );

      const dataView = new DataView(encoded.buffer);
      timestamp = dataView.getBigUint64(encoded.byteLength - 8);

      break;
    }
    case "path": {
      path = decodePathWithSeparators(
        encoded.subarray(
          0,
          encodedPathLength,
        ),
      );

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

      subspace = subspaceEncoding.decode(
        encoded.subarray(8),
      );

      const encodedSubspaceLength = subspaceEncoding.encodedLength(subspace);

      path = decodePathWithSeparators(
        encoded.subarray(
          encodedSubspaceLength,
          encodedSubspaceLength + encodedPathLength,
        ),
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
    authTokenDigest,
    payloadDigest,
    payloadLength,
    payloadScheme,
    encodedPathLength,
  }: {
    authTokenDigest: PayloadDigest;
    payloadDigest: PayloadDigest;
    payloadLength: bigint;
    payloadScheme: PayloadScheme<PayloadDigest>;
    encodedPathLength: number;
  },
): Uint8Array {
  const pathLengthBytes = new Uint8Array(4);
  const view = new DataView(pathLengthBytes.buffer);
  view.setUint32(0, encodedPathLength);

  return concat(
    pathLengthBytes,
    bigintToBytes(payloadLength),
    payloadScheme.encode(payloadDigest),
    payloadScheme.encode(authTokenDigest),
  );
}

export function decodeSummarisableStorageValue<PayloadDigest>(
  encoded: Uint8Array,
  payloadEncoding: EncodingScheme<PayloadDigest>,
): {
  encodedPathLength: number;
  payloadLength: bigint;
  payloadHash: PayloadDigest;
  authTokenHash: PayloadDigest;
} {
  const dataView = new DataView(encoded.buffer);

  const encodedPathLength = dataView.getUint32(0);

  const payloadLength = dataView.getBigUint64(4);

  const payloadHash = payloadEncoding.decode(
    encoded.subarray(4 + 8),
  );

  const payloadHashLength = payloadEncoding.encodedLength(payloadHash);

  const authTokenHash = payloadEncoding.decode(
    encoded.subarray(4 + 8 + payloadHashLength),
  );

  return {
    encodedPathLength,
    payloadLength,
    payloadHash,
    authTokenHash,
  };
}

// The successor of a path depends on the maximum length a path can have.
// Once a path reaches the maximum length, the bytestring is incremented to the left,
// e.g. [0, 0, 0, 255] -> [0, 0, 1, 255].
export function makeSuccessorPath(
  maxLength: number,
): (bytes: Uint8Array) => Uint8Array {
  return (bytes: Uint8Array) => {
    if (bytes.byteLength < maxLength) {
      const newBytes = new Uint8Array(bytes.byteLength + 1);

      newBytes.set(bytes, 0);
      newBytes.set([0], bytes.byteLength);

      return newBytes;
    } else {
      return incrementBytesLeft(bytes);
    }
  };
}

function incrementBytesLeft(bytes: Uint8Array): Uint8Array {
  const newBytes = new Uint8Array(bytes.byteLength);

  const last = bytes[bytes.byteLength - 1];

  if (last === 255 && bytes.byteLength > 1) {
    newBytes.set([last + 1], bytes.byteLength - 1);

    const left = incrementBytesLeft(bytes.slice(0, bytes.byteLength - 1));

    if (last === 255 && left[left.byteLength - 1] === 255) {
      return bytes;
    }

    newBytes.set(left, 0);

    return newBytes;
  } else if (last === 255) {
    return bytes;
  } else {
    newBytes.set([last + 1], bytes.byteLength - 1);
    newBytes.set(bytes.slice(0, bytes.byteLength - 1), 0);

    return newBytes;
  }
}
