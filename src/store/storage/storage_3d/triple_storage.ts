import {
  ANY_SUBSPACE,
  Area,
  AreaOfInterest,
  bigintToBytes,
  concat,
  EncodingScheme,
  Entry,
  isIncludedRange,
  isPathPrefixed,
  OPEN_END,
  orderTimestamp,
  Path,
  PathScheme,
  successorPrefix,
} from "../../../../deps.ts";
import {
  FingerprintScheme,
  PayloadScheme,
  QueryOrder,
  SubspaceScheme,
} from "../../types.ts";
import { LiftingMonoid } from "../summarisable_storage/lifting_monoid.ts";
import { SummarisableStorage } from "../summarisable_storage/types.ts";
import { Storage3d } from "./types.ts";

export type TripleStorageOpts<
  NamespaceId,
  SubspaceId,
  PayloadDigest,
  Fingerprint,
> = {
  namespace: NamespaceId;
  /** Creates a {@link SummarisableStorage} with a given ID, used for storing entries and their data. */
  createSummarisableStorage: (
    monoid: LiftingMonoid<Uint8Array, Fingerprint>,
    id: string,
  ) => SummarisableStorage<Uint8Array, Fingerprint>;
  subspaceScheme: SubspaceScheme<SubspaceId>;
  payloadScheme: PayloadScheme<PayloadDigest>;
  pathScheme: PathScheme;
  fingerprintScheme: FingerprintScheme<
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    Fingerprint
  >;
};

export class TripleStorage<
  NamespaceId,
  SubspaceId,
  PayloadDigest,
  Fingerprint,
> implements
  Storage3d<
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    Fingerprint
  > {
  private namespace: NamespaceId;

  private ptsStorage: SummarisableStorage<Uint8Array, Fingerprint>;
  private sptStorage: SummarisableStorage<Uint8Array, Fingerprint>;
  private tspStorage: SummarisableStorage<Uint8Array, Fingerprint>;
  private subspaceScheme: SubspaceScheme<SubspaceId>;
  private payloadScheme: PayloadScheme<PayloadDigest>;
  private fingerprintScheme: FingerprintScheme<
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    Fingerprint
  >;

  constructor(
    opts: TripleStorageOpts<
      NamespaceId,
      SubspaceId,
      PayloadDigest,
      Fingerprint
    >,
  ) {
    this.namespace = opts.namespace;

    const lift = (
      key: Uint8Array,
      value: Uint8Array,
      order: "path" | "subspace" | "timestamp",
    ) => {
      const values = decodeKvValue(
        value,
        this.payloadScheme,
      );

      // Decode the key.
      const { subspace, timestamp, path } = decodeEntryKey(
        key,
        order,
        this.subspaceScheme,
      );

      const entry: Entry<NamespaceId, SubspaceId, PayloadDigest> = {
        namespaceId: this.namespace,
        subspaceId: subspace,
        path,
        timestamp,
        payloadDigest: values.payloadHash,
        payloadLength: values.payloadLength,
      };

      return opts.fingerprintScheme.fingerprintSingleton(entry);
    };

    this.ptsStorage = opts.createSummarisableStorage({
      lift: (key, value) => lift(key, value, "path"),
      combine: opts.fingerprintScheme.fingerprintCombine,
      neutral: opts.fingerprintScheme.neutral,
    }, "pts");
    this.sptStorage = opts.createSummarisableStorage({
      lift: (key, value) => lift(key, value, "subspace"),
      combine: opts.fingerprintScheme.fingerprintCombine,
      neutral: opts.fingerprintScheme.neutral,
    }, "spt");
    this.tspStorage = opts.createSummarisableStorage({
      lift: (key, value) => lift(key, value, "timestamp"),
      combine: opts.fingerprintScheme.fingerprintCombine,
      neutral: opts.fingerprintScheme.neutral,
    }, "tsp");

    this.subspaceScheme = opts.subspaceScheme;
    this.payloadScheme = opts.payloadScheme;

    this.fingerprintScheme = opts.fingerprintScheme;
  }

  async get(
    subspace: SubspaceId,
    path: Path,
  ): Promise<
    {
      entry: Entry<NamespaceId, SubspaceId, PayloadDigest>;
      authTokenHash: PayloadDigest;
    } | undefined
  > {
    const firstResult = this.query({
      area: {
        includedSubspaceId: subspace,
        pathPrefix: path,
        timeRange: {
          start: BigInt(0),
          end: OPEN_END,
        },
      },
      maxCount: 1,
      maxSize: BigInt(0),
    }, "subspace");

    for await (const result of firstResult) {
      return result;
    }
  }

  async insert(
    { path, subspace, payloadDigest, timestamp, length, authTokenDigest }: {
      path: Path;
      subspace: SubspaceId;
      payloadDigest: PayloadDigest;
      timestamp: bigint;
      length: bigint;
      authTokenDigest: PayloadDigest;
    },
  ): Promise<void> {
    const keys = encodeEntryKeys(
      {
        path,
        timestamp,
        subspace,
        subspaceEncoding: this.subspaceScheme,
      },
    );

    const toStore = encodeKvValue(
      {
        payloadDigest,
        payloadLength: length,
        authTokenDigest: authTokenDigest,
        payloadScheme: this.payloadScheme,
      },
    );

    await Promise.all([
      this.ptsStorage.insert(keys.pts, toStore),
      this.sptStorage.insert(keys.spt, toStore),
      this.tspStorage.insert(keys.tsp, toStore),
    ]);
  }

  async remove(
    entry: Entry<NamespaceId, SubspaceId, PayloadDigest>,
  ): Promise<boolean> {
    const keys = encodeEntryKeys(
      {
        path: entry.path,
        timestamp: entry.timestamp,
        subspace: entry.subspaceId,
        subspaceEncoding: this.subspaceScheme,
      },
    );

    const results = await Promise.all([
      this.ptsStorage.remove(keys.pts),
      this.tspStorage.remove(keys.tsp),
      this.sptStorage.remove(keys.spt),
    ]);

    return results[0];
  }

  async summarise(
    areaOfInterest: AreaOfInterest<SubspaceId>,
  ): Promise<{ fingerprint: Fingerprint; size: number }> {
    let fingerprint = this.fingerprintScheme.neutral;
    /** The size of the fingerprint. */
    let size = 0;

    let countUsed = 0;
    let sizeUsed = BigInt(0);

    // Iterate through all the entries of each range.
    const subspaceEntriesLowerBound =
      areaOfInterest.area.includedSubspaceId === ANY_SUBSPACE
        ? undefined
        : areaOfInterest.area.includedSubspaceId;
    const subspaceEntriesUpperBound =
      areaOfInterest.area.includedSubspaceId === ANY_SUBSPACE
        ? undefined
        : this.subspaceScheme.successor(areaOfInterest.area.includedSubspaceId);

    const subspaceEntries = this.sptStorage.entries(
      subspaceEntriesLowerBound
        ? this.subspaceScheme.encode(subspaceEntriesLowerBound)
        : undefined,
      subspaceEntriesUpperBound
        ? this.subspaceScheme.encode(subspaceEntriesUpperBound)
        : undefined,
      {
        reverse: true,
      },
    );

    /** The least excluded item we've run into.
     * This is going to be the upper bound of a summarise op we run when we detect a contiguous range of included entries.
     */
    let leastExcluded = this.subspaceScheme.encode(
      subspaceEntriesUpperBound
        ? subspaceEntriesUpperBound
        : this.subspaceScheme.minimalSubspaceId,
    );

    /** The least included item we've run into.
     * This is going to be the lower bound of a summarise op we run when we detect a contiguous range of included entries.
     */
    let leastIncluded: Uint8Array | undefined;

    /** Run this when we detect a contiguous range of included entries. */
    const updateFingerprint = async (start: Uint8Array) => {
      const { fingerprint: includedFp, size: includedSize } = await this
        .sptStorage.summarise(
          start,
          leastExcluded,
        );

      fingerprint = this.fingerprintScheme.fingerprintCombine(
        fingerprint,
        includedFp,
      );

      size += includedSize;

      // Prevent this from running again until we run into another included entry.
      leastIncluded = undefined;
    };

    for await (const subspaceEntry of subspaceEntries) {
      // Decode the key.
      const values = decodeKvValue(
        subspaceEntry.value,
        this.payloadScheme,
      );

      // Decode the key.
      const { timestamp, path } = decodeEntryKey(
        subspaceEntry.key,
        "subspace",
        this.subspaceScheme,
      );

      // Check that decoded time and subspace are included by both other dimensions
      let pathIncluded = false;

      if (isPathPrefixed(areaOfInterest.area.pathPrefix, path)) {
        pathIncluded = true;
      }

      // If it's not included, and we ran into an included item earlier,
      // that indicates the end of a contiguous range.
      // Recalculate the fingerprint!
      if (!pathIncluded) {
        if (leastIncluded) {
          await updateFingerprint(leastIncluded);
        }

        // This entry is now the least excluded entry we've run into.
        leastExcluded = subspaceEntry.key;
        continue;
      }

      let timeIncluded = false;

      if (
        isIncludedRange(
          orderTimestamp,
          areaOfInterest.area.timeRange,
          timestamp,
        )
      ) {
        timeIncluded = true;
      }

      // If it's not included, and we ran into an included item earlier,
      // that indicates the end of a contiguous range.
      // Recalculate the fingerprint!
      if (!timeIncluded) {
        if (leastIncluded) {
          await updateFingerprint(leastIncluded);
        }

        // This entry is now the least excluded entry we've run into.
        leastExcluded = subspaceEntry.key;
        continue;
      }

      // Now we know this entry is included.

      // Check all dimension count and size limits.
      // If any limits have been exceeded, we have to stop here.

      // Boring.

      const nextCountUsed = countUsed + 1;
      const nextSizeUsed = sizeUsed + values.payloadLength;

      if (
        (areaOfInterest.maxCount !== 0 &&
          nextCountUsed > areaOfInterest.maxCount) ||
        (areaOfInterest.maxSize !== BigInt(0) &&
          nextSizeUsed > areaOfInterest.maxSize)
      ) {
        break;
      }

      countUsed = nextCountUsed;
      sizeUsed = nextSizeUsed;

      // This entry is part of a contiguous range of included entries,
      // and it's the least included key we've encountered so far.
      leastIncluded = subspaceEntry.key;
    }

    // Calculate a range that was left over, if any.
    if (leastIncluded) {
      await updateFingerprint(leastIncluded);
    }

    return {
      fingerprint,
      size,
    };
  }

  async *query(
    areaOfInterest: AreaOfInterest<SubspaceId>,
    order: QueryOrder,
    reverse = false,
  ): AsyncIterable<{
    entry: Entry<NamespaceId, SubspaceId, PayloadDigest>;
    authTokenHash: PayloadDigest;
  }> {
    const storage = order === "subspace"
      ? this.sptStorage
      : order === "path"
      ? this.ptsStorage
      : this.tspStorage;

    const { lowerBound, upperBound } = order === "subspace"
      ? this.createSptBounds(areaOfInterest.area)
      : order === "path"
      ? this.createPtsBounds(areaOfInterest.area)
      : this.createTspBounds(areaOfInterest.area);

    let entriesYielded = 0;
    let payloadBytesYielded = BigInt(0);

    const iterator = storage.entries(lowerBound, upperBound, {
      reverse,
    });

    for await (const { key, value } of iterator) {
      const values = decodeKvValue(
        value,
        this.payloadScheme,
      );

      // Decode the key.
      const { subspace, timestamp, path } = decodeEntryKey(
        key,
        order,
        this.subspaceScheme,
      );

      if (
        areaOfInterest.area.includedSubspaceId !== ANY_SUBSPACE
      ) {
        const isSubspace = this.subspaceScheme.order(
          subspace,
          areaOfInterest.area.includedSubspaceId,
        ) === 0;

        if (!isSubspace) {
          continue;
        }
      }

      const isOpenTime = areaOfInterest.area.timeRange.start === BigInt(0) &&
        areaOfInterest.area.timeRange.end === OPEN_END;

      if (
        !isOpenTime
      ) {
        const isTimeIncluded = isIncludedRange(
          orderTimestamp,
          areaOfInterest.area.timeRange,
          timestamp,
        );

        if (!isTimeIncluded) {
          continue;
        }
      }

      if (
        areaOfInterest.area.pathPrefix.length !== 0
      ) {
        const isPathIncluded = isPathPrefixed(
          areaOfInterest.area.pathPrefix,
          path,
        );

        if (!isPathIncluded) {
          continue;
        }
      }

      entriesYielded += 1;
      payloadBytesYielded += values.payloadLength;

      if (
        areaOfInterest.maxSize !== BigInt(0) &&
        payloadBytesYielded >= areaOfInterest.maxSize
      ) {
        break;
      }

      yield {
        entry: {
          namespaceId: this.namespace,
          subspaceId: subspace,
          path,
          payloadDigest: values.payloadHash,
          payloadLength: values.payloadLength,
          timestamp,
        },
        authTokenHash: values.authTokenHash,
      };

      if (
        areaOfInterest.maxCount !== 0 &&
        entriesYielded >= areaOfInterest.maxCount
      ) {
        break;
      }
    }
  }

  private encodeKey(
    doNotEscapeIdx: number,
    ...parts: Uint8Array[]
  ): Uint8Array {
    const toConcat = [];

    for (let i = 0; i < parts.length; i++) {
      if (i === doNotEscapeIdx) {
        toConcat.push(parts[i], new Uint8Array([0, 0]));
        continue;
      }

      const escapedBytes = escapeBytes(parts[i]);

      toConcat.push(escapedBytes, new Uint8Array([0, 0]));
    }

    return concat(...toConcat);
  }

  private createSptBounds(
    area: Area<SubspaceId>,
  ): {
    lowerBound: Uint8Array | undefined;
    upperBound: Uint8Array | undefined;
  } {
    if (area.includedSubspaceId === ANY_SUBSPACE) {
      return { lowerBound: undefined, upperBound: undefined };
    }

    const encodedSubspace = this.subspaceScheme.encode(area.includedSubspaceId);

    const successorSubspace = this.subspaceScheme.successor(
      area.includedSubspaceId,
    );

    if (!successorSubspace) {
      return {
        lowerBound: this.encodeKey(1, encodedSubspace),
        upperBound: undefined,
      };
    }

    const encodeddSubspaceSuccessor = this.subspaceScheme.encode(
      successorSubspace,
    );

    if (area.pathPrefix.length === 0) {
      return {
        lowerBound: this.encodeKey(1, encodedSubspace),
        upperBound: this.encodeKey(1, encodeddSubspaceSuccessor),
      };
    }

    const encodedAreaPrefix = encodePathWithSeparators(area.pathPrefix);

    const areaPrefixSuccessor = successorPrefix(area.pathPrefix);

    if (!areaPrefixSuccessor) {
      return {
        lowerBound: this.encodeKey(1, encodedSubspace, encodedAreaPrefix),
        upperBound: this.encodeKey(1, encodeddSubspaceSuccessor),
      };
    }

    const encodedTime = bigintToBytes(area.timeRange.start);

    const encodedAreaPrefixSuccessor = encodePathWithSeparators(
      areaPrefixSuccessor,
    );

    if (area.timeRange.end === OPEN_END) {
      return {
        lowerBound: this.encodeKey(
          1,
          encodedSubspace,
          encodedAreaPrefix,
          encodedTime,
        ),
        upperBound: this.encodeKey(
          1,
          encodedSubspace,
          encodedAreaPrefixSuccessor,
        ),
      };
    }

    const encodedTimeEnd = bigintToBytes(area.timeRange.end);

    return {
      lowerBound: this.encodeKey(
        1,
        encodedSubspace,
        encodedAreaPrefix,
        encodedTime,
      ),
      upperBound: this.encodeKey(
        1,
        encodeddSubspaceSuccessor,
        encodedAreaPrefixSuccessor,
        encodedTimeEnd,
      ),
    };
  }

  private createPtsBounds(
    area: Area<SubspaceId>,
  ): {
    lowerBound: Uint8Array | undefined;
    upperBound: Uint8Array | undefined;
  } {
    if (area.pathPrefix.length === 0) {
      return {
        lowerBound: undefined,
        upperBound: undefined,
      };
    }

    const encodedPrefix = encodePathWithSeparators(area.pathPrefix);

    const areaPrefixSuccessor = successorPrefix(area.pathPrefix);
    const encodedTimeStart = bigintToBytes(area.timeRange.start);

    if (!areaPrefixSuccessor) {
      return {
        lowerBound: this.encodeKey(0, encodedPrefix, encodedTimeStart),
        upperBound: undefined,
      };
    }

    const encodedPrefixSuccessor = encodePathWithSeparators(
      areaPrefixSuccessor,
    );

    if (area.timeRange.end === OPEN_END) {
      return {
        lowerBound: this.encodeKey(0, encodedPrefix, encodedTimeStart),
        upperBound: encodedPrefixSuccessor,
      };
    }

    if (area.includedSubspaceId === ANY_SUBSPACE) {
      return {
        lowerBound: this.encodeKey(0, encodedPrefix, encodedTimeStart),
        upperBound: encodedPrefixSuccessor,
      };
    }

    const encodedSubspace = this.subspaceScheme.encode(area.includedSubspaceId);

    return {
      lowerBound: this.encodeKey(
        0,
        encodedPrefix,
        encodedTimeStart,
        encodedSubspace,
      ),
      upperBound: this.encodeKey(0, encodedPrefixSuccessor),
    };
  }

  private createTspBounds(
    area: Area<SubspaceId>,
  ): {
    lowerBound: Uint8Array | undefined;
    upperBound: Uint8Array | undefined;
  } {
    if (area.timeRange.start === BigInt(0)) {
      return {
        lowerBound: undefined,
        upperBound: undefined,
      };
    }

    const encodedTimeStart = bigintToBytes(area.timeRange.start);

    if (area.timeRange.end === OPEN_END) {
      return {
        lowerBound: this.encodeKey(2, encodedTimeStart),
        upperBound: undefined,
      };
    }

    const encodedTimeEnd = bigintToBytes(area.timeRange.end);

    if (area.includedSubspaceId === ANY_SUBSPACE) {
      return {
        lowerBound: this.encodeKey(2, encodedTimeStart),
        upperBound: this.encodeKey(2, encodedTimeEnd),
      };
    }

    const encodedSubspace = this.subspaceScheme.encode(area.includedSubspaceId);

    const encodedPathPrefix = encodePathWithSeparators(area.pathPrefix);

    return {
      lowerBound: this.encodeKey(
        2,
        encodedTimeStart,
        encodedSubspace,
        encodedPathPrefix,
      ),
      upperBound: this.encodeKey(2, encodedTimeEnd),
    };
  }
}

/** Escape all 0 bytes to 0x02, and encode separators as 0x01.
 *
 * This is all so the keys triple storage uses (which are concatenations of subspace / path / time [in differing orders] are ordered in KV the same way as we would outside the KV.
 */
export function encodePathWithSeparators(path: Path): Uint8Array {
  const encodedComponents: Uint8Array[] = [];

  for (const component of path) {
    const bytes: number[] = [];

    for (const byte of component) {
      if (byte !== 0) {
        bytes.push(byte);
        continue;
      }

      bytes.push(0, 2);
    }

    bytes.push(0, 1);
    const encodedComponent = new Uint8Array(bytes);
    encodedComponents.push(encodedComponent);
  }

  return concat(...encodedComponents);
}

/** Decodes an escaped encoded path. */
export function decodePathWithSeparators(
  encoded: Uint8Array,
): Path {
  const path: Path = [];

  let currentComponentBytes = [];
  let previousWasZero = false;

  for (const byte of encoded) {
    if (previousWasZero && byte === 1) {
      // Separator
      previousWasZero = false;

      const component = new Uint8Array(currentComponentBytes);

      path.push(component);

      currentComponentBytes = [];

      continue;
    }

    if (previousWasZero && byte === 2) {
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

/** Escape all 0 bytes as 0x01. */
export function escapeBytes(bytes: Uint8Array): Uint8Array {
  const newBytes: number[] = [];

  for (const byte of bytes) {
    if (byte !== 0) {
      newBytes.push(byte);
      continue;
    }

    newBytes.push(0, 1);
  }

  return new Uint8Array(newBytes);
}

/** Unescape all 0x01 back to 0x0. */
export function unescapeBytes(escaped: Uint8Array): Uint8Array {
  let previousWasZero = false;

  const escapedBytes = [];

  for (const byte of escaped) {
    if (previousWasZero && byte === 1) {
      escapedBytes.push(0);
      previousWasZero = false;
      continue;
    }

    if (byte === 0) {
      previousWasZero = true;
      continue;
    }

    previousWasZero = false;
    escapedBytes.push(byte);
  }

  return new Uint8Array(escapedBytes);
}

/** Join all the parts of a key (which are presumed to be escaped and contain no 0x0 bytes) with a 0x00 separator between them. */
export function joinKey(
  ...parts: Uint8Array[]
): Uint8Array {
  const newParts = [];

  for (const part of parts) {
    newParts.push(part, new Uint8Array([0, 0]));
  }

  return concat(...newParts);
}

/** Split all the semantic parts of a key (subspace, path, time) out of a key. Returns them in the same order as they are in the key. */
export function splitKey(
  key: Uint8Array,
): [Uint8Array, Uint8Array, Uint8Array] {
  const parts = [];

  let previousWasZero = false;

  let currentPartBytes: number[] = [];

  for (const byte of key) {
    if (previousWasZero && byte === 0) {
      parts.push(new Uint8Array(currentPartBytes));
      currentPartBytes = [];
      previousWasZero = false;
      continue;
    }

    if (!previousWasZero && byte === 0) {
      previousWasZero = true;
      continue;
    }

    if (previousWasZero && byte !== 0) {
      currentPartBytes.push(0);
    }

    previousWasZero = false;

    currentPartBytes.push(byte);
  }

  return [parts[0], parts[1], parts[2]];
}

/** Encodes the subspace, path, and time of an entry into three keys for three respective orderings.
 */
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
} {
  const escapedSubspace = escapeBytes(
    opts.subspaceEncoding.encode(opts.subspace),
  );
  const escapedPath = encodePathWithSeparators(opts.path);
  const escapedTime = escapeBytes(bigintToBytes(opts.timestamp));

  const sptBytes = joinKey(escapedSubspace, escapedPath, escapedTime);
  const ptsBytes = joinKey(escapedPath, escapedTime, escapedSubspace);
  const tspBytes = joinKey(escapedTime, escapedSubspace, escapedPath);

  return {
    spt: sptBytes,
    pts: ptsBytes,
    tsp: tspBytes,
  };
}

/** Decodes a key back into subspace, path, and timestamp. */
export function decodeEntryKey<SubspacePublicKey>(
  encoded: Uint8Array,
  order: "subspace" | "path" | "timestamp",
  subspaceEncoding: EncodingScheme<SubspacePublicKey>,
): {
  subspace: SubspacePublicKey;
  path: Path;
  timestamp: bigint;
} {
  let subspace: SubspacePublicKey;
  let timestamp: bigint;
  let path: Path;

  const [fst, snd, thd] = splitKey(encoded);

  switch (order) {
    case "subspace": {
      subspace = subspaceEncoding.decode(unescapeBytes(fst));

      path = decodePathWithSeparators(snd);

      const dataView = new DataView(unescapeBytes(thd).buffer);
      timestamp = dataView.getBigUint64(0);

      break;
    }
    case "path": {
      path = decodePathWithSeparators(fst);

      const dataView = new DataView(unescapeBytes(snd).buffer);
      timestamp = dataView.getBigUint64(0);

      subspace = subspaceEncoding.decode(unescapeBytes(thd));

      break;
    }
    case "timestamp": {
      const dataView = new DataView(unescapeBytes(fst).buffer);
      timestamp = dataView.getBigUint64(0);

      subspace = subspaceEncoding.decode(unescapeBytes(snd));

      path = decodePathWithSeparators(thd);
    }
  }

  return {
    subspace,
    path,
    timestamp,
  };
}

/** Encodes some values associated with an entry to a single value to be stored in KV. */
export function encodeKvValue<PayloadDigest>(
  {
    authTokenDigest,
    payloadDigest,
    payloadLength,
    payloadScheme,
  }: {
    authTokenDigest: PayloadDigest;
    payloadDigest: PayloadDigest;
    payloadLength: bigint;
    payloadScheme: PayloadScheme<PayloadDigest>;
  },
): Uint8Array {
  return concat(
    bigintToBytes(payloadLength),
    payloadScheme.encode(payloadDigest),
    payloadScheme.encode(authTokenDigest),
  );
}

export function decodeKvValue<PayloadDigest>(
  encoded: Uint8Array,
  payloadEncoding: EncodingScheme<PayloadDigest>,
): {
  payloadLength: bigint;
  payloadHash: PayloadDigest;
  authTokenHash: PayloadDigest;
} {
  const dataView = new DataView(encoded.buffer);

  const payloadLength = dataView.getBigUint64(0);

  const payloadHash = payloadEncoding.decode(
    encoded.subarray(8),
  );

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
