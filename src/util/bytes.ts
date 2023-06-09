import { bytesConcat } from "../../deps.ts";
import { WillowFormat } from "../replica/types.ts";
import { Entry } from "../types.ts";

export function compareBytes(a: Uint8Array, b: Uint8Array): number {
  // They have the same length.
  for (let i = 0; i < a.byteLength; i++) {
    const aByte = a[i];
    const bByte = b[i];

    if (aByte === bByte) {
      continue;
    }

    if (aByte < bByte) {
      return -1;
    }

    if (aByte > bByte) {
      return 1;
    }
  }

  return 0;
}

export function bigintToBytes(bigint: bigint): Uint8Array {
  const bytes = new Uint8Array(8);
  const view = new DataView(bytes);

  view.setBigUint64(0, bigint);

  return bytes;
}

export function incrementLastByte(bytes: Uint8Array) {
  const last = bytes[bytes.byteLength - 1];

  if (last === 255) {
    const newBytes = new Uint8Array(bytes.byteLength + 1);

    newBytes.set(bytes, 0);
    newBytes.set([0], bytes.byteLength);

    return newBytes;
  } else {
    const newBytes = new Uint8Array(bytes);

    newBytes.set([last + 1], bytes.byteLength - 1);

    return newBytes;
  }
}

export function entryAuthorPathBytes(entry: Entry): Uint8Array {
  const bytes = new Uint8Array(
    entry.identifier.author.byteLength + entry.identifier.path.byteLength,
  );

  bytes.set(new Uint8Array(entry.identifier.author), 0);
  bytes.set(
    new Uint8Array(entry.identifier.path),
    entry.identifier.author.byteLength,
  );

  return bytes;
}

export function entryKeyBytes(
  path: Uint8Array,
  timestamp: bigint,
  author: Uint8Array,
): { apt: Uint8Array; pta: Uint8Array; tap: Uint8Array } {
  const keyLength = 8 + author.byteLength +
    path.byteLength;

  const aptBytes = new Uint8Array(keyLength);
  const ptaBytes = new Uint8Array(keyLength);
  const tapBytes = new Uint8Array(keyLength);

  const pathBytes = new Uint8Array(path);
  const authorBytes = new Uint8Array(author);

  // Author, path, timestamp
  aptBytes.set(authorBytes, 0);
  aptBytes.set(
    pathBytes,
    authorBytes.length,
  );
  const aptDv = new DataView(aptBytes.buffer);
  aptDv.setBigUint64(
    pathBytes.byteLength + authorBytes.byteLength,
    timestamp,
  );

  // Path, timestamp, author
  ptaBytes.set(pathBytes, 0);
  const ptaDv = new DataView(ptaBytes.buffer);
  ptaDv.setBigUint64(
    pathBytes.byteLength,
    timestamp,
  );
  ptaBytes.set(authorBytes, pathBytes.byteLength + 8);

  // Timestamp, author, path
  const tapDv = new DataView(tapBytes.buffer);
  tapDv.setBigUint64(
    0,
    timestamp,
  );
  tapBytes.set(authorBytes, 8);
  tapBytes.set(pathBytes, 8 + authorBytes.byteLength);

  return { apt: aptBytes, pta: ptaBytes, tap: tapBytes };
}

export function entryAptBytes(entry: Entry): Uint8Array {
  const bytes = new Uint8Array(
    entry.identifier.author.byteLength + entry.identifier.path.byteLength + 8,
  );

  bytes.set(new Uint8Array(entry.identifier.author), 0);
  bytes.set(
    new Uint8Array(entry.identifier.path),
    entry.identifier.author.byteLength,
  );

  const timestampDv = new DataView(bytes);
  timestampDv.setBigUint64(
    entry.identifier.author.byteLength + entry.identifier.path.byteLength,
    entry.record.timestamp,
  );

  return bytes;
}

export function entryPtaBytes(entry: Entry): Uint8Array {
  const bytes = new Uint8Array(
    entry.identifier.path.byteLength + 8 + entry.identifier.author.byteLength,
  );

  bytes.set(
    new Uint8Array(entry.identifier.path),
    0,
  );

  const timestampDv = new DataView(bytes);
  timestampDv.setBigUint64(
    entry.identifier.path.byteLength,
    entry.record.timestamp,
  );

  bytes.set(
    new Uint8Array(entry.identifier.author),
    entry.identifier.path.byteLength + 8,
  );

  return bytes;
}

export function entryTapBytes(entry: Entry): Uint8Array {
  const bytes = new Uint8Array(
    8 + entry.identifier.author.byteLength + entry.identifier.path.byteLength,
  );

  const timestampDv = new DataView(bytes);
  timestampDv.setBigUint64(
    0,
    entry.record.timestamp,
  );

  bytes.set(
    new Uint8Array(entry.identifier.author),
    8,
  );

  bytes.set(
    new Uint8Array(entry.identifier.path),
    8 + entry.identifier.author.byteLength,
  );

  return bytes;
}

export function detailsFromBytes(
  bytes: Uint8Array,
  order: "author" | "path" | "timestamp",
  pubkeyLength: number,
): {
  author: Uint8Array;
  path: Uint8Array;
  timestamp: bigint;
} {
  let author;
  let timestamp;
  let path;

  switch (order) {
    case "author": {
      author = bytes.slice(0, pubkeyLength);

      path = bytes.slice(pubkeyLength, bytes.byteLength - 8);

      const dataView = new DataView(bytes.buffer);
      timestamp = dataView.getBigUint64(bytes.byteLength - 8);

      break;
    }
    case "path": {
      path = bytes.slice(0, bytes.byteLength - pubkeyLength - 8);

      const dataView = new DataView(bytes.buffer);
      timestamp = dataView.getBigUint64(
        bytes.byteLength - pubkeyLength - 8,
      );

      author = bytes.slice(
        bytes.byteLength - pubkeyLength,
      );

      break;
    }
    case "timestamp": {
      const dataView = new DataView(bytes);
      timestamp = dataView.getBigUint64(
        0,
      );

      path = bytes.slice(8, bytes.byteLength - pubkeyLength);

      author = bytes.slice(bytes.byteLength - pubkeyLength);
    }
  }

  return {
    author,
    path,
    timestamp,
  };
}

export function concatSummarisableStorageValue(
  hash: ArrayBuffer,
  namespaceSig: ArrayBuffer,
  authorSig: ArrayBuffer,
): Uint8Array {
  return bytesConcat(
    new Uint8Array(hash),
    new Uint8Array(namespaceSig),
    new Uint8Array(authorSig),
  );
}

export function sliceSummarisableStorageValue<KeypairType>(
  bytes: Uint8Array,
  format: WillowFormat<KeypairType>,
) {
  return {
    hash: bytes.slice(0, format.hashLength),
    namespaceSignature: bytes.slice(
      format.hashLength,
      format.hashLength + format.pubkeyLength,
    ),
    authorSignature: bytes.slice(format.hashLength + format.pubkeyLength),
  };
}
