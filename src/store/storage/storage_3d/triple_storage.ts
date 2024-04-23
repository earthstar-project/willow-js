import {
  AreaOfInterest,
  areaTo3dRange,
  bigintToBytes,
  concat,
  EncodingScheme,
  Entry,
  isIncluded3d,
  isIncludedRange,
  OPEN_END,
  orderPath,
  orderTimestamp,
  Path,
  PathScheme,
  Range3d,
  successorPath,
} from "../../../../deps.ts";
import {
  FingerprintScheme,
  PayloadScheme,
  QueryOrder,
  SubspaceScheme,
} from "../../types.ts";
import { LiftingMonoid } from "../summarisable_storage/lifting_monoid.ts";
import { SummarisableStorage } from "../summarisable_storage/types.ts";
import { RangeOfInterest, Storage3d } from "./types.ts";
import { WillowError } from "../../../errors.ts";

export type TripleStorageOpts<
  NamespaceId,
  SubspaceId,
  PayloadDigest,
  Fingerprint,
> = {
  namespace: NamespaceId;
  /** Creates a {@link SummarisableStorage} with a given ID, used for storing entries and their data. */
  createSummarisableStorage: (
    monoid: LiftingMonoid<[Uint8Array, Uint8Array], Fingerprint>,
    id: string,
  ) => SummarisableStorage<Uint8Array, Uint8Array, Fingerprint>;
  subspaceScheme: SubspaceScheme<SubspaceId>;
  payloadScheme: PayloadScheme<PayloadDigest>;
  pathScheme: PathScheme;
  fingerprintScheme: FingerprintScheme<
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    Fingerprint
  >;
  getPayloadLength: (digest: PayloadDigest) => Promise<bigint>;
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

  private ptsStorage: SummarisableStorage<Uint8Array, Uint8Array, Fingerprint>;
  private sptStorage: SummarisableStorage<Uint8Array, Uint8Array, Fingerprint>;
  private tspStorage: SummarisableStorage<Uint8Array, Uint8Array, Fingerprint>;
  private subspaceScheme: SubspaceScheme<SubspaceId>;
  private payloadScheme: PayloadScheme<PayloadDigest>;
  private fingerprintScheme: FingerprintScheme<
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    Fingerprint
  >;
  private pathScheme: PathScheme;

  constructor(
    opts: TripleStorageOpts<
      NamespaceId,
      SubspaceId,
      PayloadDigest,
      Fingerprint
    >,
  ) {
    this.namespace = opts.namespace;

    const lift = async (
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

      const available = await opts.getPayloadLength(entry.payloadDigest) ||
        BigInt(0);

      return opts.fingerprintScheme.fingerprintSingleton({ entry, available });
    };

    this.ptsStorage = opts.createSummarisableStorage({
      lift: ([key, value]) => lift(key, value, "path"),
      combine: opts.fingerprintScheme.fingerprintCombine,
      neutral: opts.fingerprintScheme.neutral,
    }, "pts");
    this.sptStorage = opts.createSummarisableStorage({
      lift: ([key, value]) => lift(key, value, "subspace"),
      combine: opts.fingerprintScheme.fingerprintCombine,
      neutral: opts.fingerprintScheme.neutral,
    }, "spt");
    this.tspStorage = opts.createSummarisableStorage({
      lift: ([key, value]) => lift(key, value, "timestamp"),
      combine: opts.fingerprintScheme.fingerprintCombine,
      neutral: opts.fingerprintScheme.neutral,
    }, "tsp");

    this.subspaceScheme = opts.subspaceScheme;
    this.payloadScheme = opts.payloadScheme;
    this.fingerprintScheme = opts.fingerprintScheme;
    this.pathScheme = opts.pathScheme;
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
      range: {
        subspaceRange: {
          start: subspace,
          end: this.subspaceScheme.successor(subspace) || OPEN_END,
        },
        pathRange: {
          start: path,
          end: successorPath(path, this.pathScheme) || OPEN_END,
        },
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
    range: Range3d<SubspaceId>,
  ): Promise<{ fingerprint: Fingerprint; size: number }> {
    let fingerprint = this.fingerprintScheme.neutral;
    /** The size of the fingerprint. */
    let size = 0;

    const { lowerBound, upperBound } = this.createTspBounds(range);

    const timeEntries = this.tspStorage.entries(
      lowerBound,
      upperBound,
      {
        reverse: true,
      },
    );

    /** The least excluded item we've run into.
     * This is going to be the upper bound of a summarise op we run when we detect a contiguous range of included entries.
     */
    let leastExcluded = upperBound;

    /** The least included item we've run into.
     * This is going to be the lower bound of a summarise op we run when we detect a contiguous range of included entries.
     */
    let leastIncluded: Uint8Array | undefined;

    /** Run this when we detect a contiguous range of included entries. */
    const updateFingerprint = async (start: Uint8Array) => {
      const { fingerprint: includedFp, size: includedSize } = await this
        .tspStorage.summarise(
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

    for await (const entry of timeEntries) {
      // Decode the key.
      const { timestamp, path, subspace } = decodeEntryKey(
        entry.key,
        "timestamp",
        this.subspaceScheme,
      );

      const isIncluded = isIncluded3d(
        this.subspaceScheme.order,
        range,
        {
          path,
          time: timestamp,
          subspace,
        },
      );

      // If it's not included, and we ran into an included item earlier,
      // that indicates the end of a contiguous range.
      // Recalculate the fingerprint!
      if (!isIncluded) {
        if (leastIncluded) {
          await updateFingerprint(leastIncluded);
        }

        // This entry is now the least excluded entry we've run into.
        leastExcluded = entry.key;
        continue;
      }

      // Now we know this entry is included.

      // This entry is part of a contiguous range of included entries,
      // and it's the least included key we've encountered so far.
      leastIncluded = entry.key;
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

  async removeInterest(
    areaOfInterest: AreaOfInterest<SubspaceId>,
  ): Promise<Range3d<SubspaceId>> {
    const range = areaTo3dRange({
      maxComponentCount: this.pathScheme.maxComponentCount,
      maxPathComponentLength: this.pathScheme.maxComponentLength,
      maxPathLength: this.pathScheme.maxPathLength,
      minimalSubspace: this.subspaceScheme.minimalSubspaceId,
      successorSubspace: this.subspaceScheme.successor,
    }, areaOfInterest.area);

    if (areaOfInterest.maxCount === 0 && areaOfInterest.maxSize === BigInt(0)) {
      return range;
    }

    let countUsed = 0;
    let sizeUsed = BigInt(0);

    const { lowerBound, upperBound } = this.createTspBounds(range);

    let lowerBoundTime;
    let lowerBoundSubspace;
    let lowerBoundPath;

    let upperboundTime;
    let upperboundSubspace;
    let upperboundPath;

    const timeEntries = this.tspStorage.entries(
      lowerBound,
      upperBound,
      {
        reverse: true,
      },
    );

    for await (const entry of timeEntries) {
      // Decode the key.
      const values = decodeKvValue(
        entry.value,
        this.payloadScheme,
      );

      // Decode the key.
      const { timestamp, path, subspace } = decodeEntryKey(
        entry.key,
        "timestamp",
        this.subspaceScheme,
      );

      const isIncluded = isIncluded3d(
        this.subspaceScheme.order,
        range,
        {
          path,
          time: timestamp,
          subspace,
        },
      );

      if (!isIncluded) {
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

      if (!upperboundTime || timestamp > upperboundTime) {
        if (timestamp === BigInt(2 ** 64 - 1)) {
          // TODO: account for the largest possible bigint.
          //upperboundTime = OPEN_END;
        } else {
          upperboundTime = timestamp + BigInt(1);
        }
      }

      if (
        !upperboundSubspace ||
        this.subspaceScheme.order(subspace, upperboundSubspace) === 1
      ) {
        upperboundSubspace = this.subspaceScheme.successor(subspace);
      }

      if (!upperboundPath || orderPath(path, upperboundPath) === 1) {
        upperboundPath = successorPath(path, this.pathScheme);
      }

      if (!lowerBoundTime || timestamp < lowerBoundTime) {
        lowerBoundTime = timestamp;
      }

      if (
        !lowerBoundSubspace ||
        this.subspaceScheme.order(subspace, lowerBoundSubspace) === -1
      ) {
        lowerBoundSubspace = subspace;
      }

      if (
        !lowerBoundPath || orderPath(path, lowerBoundPath) === -1
      ) {
        lowerBoundPath = path;
      }
    }

    return {
      subspaceRange: {
        start: lowerBoundSubspace || range.subspaceRange.start,
        end: upperboundSubspace || range.subspaceRange.end,
      },
      pathRange: {
        start: lowerBoundPath || range.pathRange.start,
        end: upperboundPath || range.pathRange.end,
      },
      timeRange: {
        start: lowerBoundTime || range.timeRange.start,
        end: upperboundTime || range.timeRange.end,
      },
    };
  }

  async *query(
    rangeOfInterest: RangeOfInterest<SubspaceId>,
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
      ? this.createSptBounds(rangeOfInterest.range)
      : order === "path"
      ? this.createPtsBounds(rangeOfInterest.range)
      : this.createTspBounds(rangeOfInterest.range);

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

      const allowAnySubspace = this.subspaceScheme.order(
            rangeOfInterest.range.subspaceRange.start,
            this.subspaceScheme.minimalSubspaceId,
          ) === 0 && rangeOfInterest.range.subspaceRange.end === OPEN_END;

      if (!allowAnySubspace) {
        if (
          !isIncludedRange(
            this.subspaceScheme.order,
            rangeOfInterest.range.subspaceRange,
            subspace,
          )
        ) {
          continue;
        }
      }

      const allowAnyTime =
        rangeOfInterest.range.timeRange.start === BigInt(0) &&
        rangeOfInterest.range.timeRange.end === OPEN_END;

      if (
        !allowAnyTime
      ) {
        const isTimeIncluded = isIncludedRange(
          orderTimestamp,
          rangeOfInterest.range.timeRange,
          timestamp,
        );

        if (!isTimeIncluded) {
          continue;
        }
      }

      const allowAnyPath = rangeOfInterest.range.pathRange.start.length === 0 &&
        rangeOfInterest.range.pathRange.end === OPEN_END;

      if (
        !allowAnyPath
      ) {
        const isPathIncluded = isIncludedRange(
          orderPath,
          rangeOfInterest.range.pathRange,
          path,
        );

        if (!isPathIncluded) {
          continue;
        }
      }

      entriesYielded += 1;
      payloadBytesYielded += values.payloadLength;

      if (
        rangeOfInterest.maxSize !== BigInt(0) &&
        payloadBytesYielded >= rangeOfInterest.maxSize
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
        rangeOfInterest.maxCount !== 0 &&
        entriesYielded >= rangeOfInterest.maxCount
      ) {
        break;
      }
    }
  }

  async splitRange(
    range: Range3d<SubspaceId>,
    knownSize: number,
  ): Promise<[Range3d<SubspaceId>, Range3d<SubspaceId>]> {
    if (knownSize < 2) {
      throw new WillowError(
        "Tried to split a range which doesn't need splitting",
      );
    }

    let low = range.timeRange.start;
    let high = BigInt(2 ** 64 - 1);
    let mid = BigInt(0);

    if (range.timeRange.end === OPEN_END) {
      for await (
        const greatest of this.query(
          {
            range,
            maxCount: 0,
            maxSize: BigInt(0),
          },
          "timestamp",
          true,
        )
      ) {
        high = greatest.entry.timestamp;
        break;
      }
    } else {
      high = range.timeRange.end;
    }

    let amIHappy = false;

    while (low < high) {
      if (low === high) {
        // TODO: Screw you and your unrealistic data set.
        // OR: Split along the other dimensions
        break;
      }

      mid = (high + low) / BigInt(2);

      const { size: sizeLowToMid } = await this.summarise(
        {
          ...range,
          timeRange: {
            start: range.timeRange.start,
            end: mid,
          },
        },
      );

      const quality = sizeLowToMid / knownSize;

      if (quality > 0.3333 && quality <= 0.6666) {
        amIHappy = true;
        break;
      }

      if (quality <= 0.5) {
        low = mid;
      } else {
        high = mid;
      }

      // await delay(1000);
    }

    if (amIHappy) {
      return [
        {
          ...range,
          timeRange: {
            start: range.timeRange.start,
            end: mid,
          },
        },
        {
          ...range,
          timeRange: {
            start: mid,
            end: range.timeRange.end,
          },
        },
      ];
    }

    return [
      {
        ...range,
        timeRange: {
          start: range.timeRange.start,
          end: mid,
        },
      },
      {
        ...range,
        timeRange: {
          start: mid,
          end: range.timeRange.end,
        },
      },
    ];

    // Search along path

    // Am I happy? No?

    // Search along subspace.

    // do not split range of interest, just a range
    // when you get a (non trivial) range of interest -> range and split it.
    // to do this, we query that range of interest and then use its results to create the new range.
    // this way we won't exceed their limits.
  }

  async updateAvailablePayload(
    subspace: SubspaceId,
    path: Path,
  ): Promise<boolean> {
    const result = await this.get(subspace, path);

    if (!result) {
      return false;
    }

    const { entry, authTokenHash } = result;

    await this.remove(result.entry);
    await this.insert({
      subspace: entry.subspaceId,
      path: entry.path,
      timestamp: entry.timestamp,
      payloadDigest: entry.payloadDigest,
      length: entry.payloadLength,
      authTokenDigest: authTokenHash,
    });

    return true;
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
    range: Range3d<SubspaceId>,
  ): {
    lowerBound: Uint8Array | undefined;
    upperBound: Uint8Array | undefined;
  } {
    const encSubspaceStart = this.subspaceScheme.encode(
      range.subspaceRange.start,
    );

    if (range.subspaceRange.end === OPEN_END) {
      return {
        lowerBound: this.encodeKey(1, encSubspaceStart),
        upperBound: undefined,
      };
    }

    const encSubspaceEnd = this.subspaceScheme.encode(range.subspaceRange.end);

    const encPathStart = encodePathWithSeparators(range.pathRange.start);

    if (range.pathRange.end === OPEN_END) {
      return {
        lowerBound: this.encodeKey(1, encSubspaceStart, encPathStart),
        upperBound: this.encodeKey(1, encSubspaceEnd),
      };
    }

    const encPathEnd = encodePathWithSeparators(range.pathRange.end);

    const encTimeStart = bigintToBytes(range.timeRange.start);

    if (range.timeRange.end === OPEN_END) {
      return {
        lowerBound: this.encodeKey(
          1,
          encSubspaceStart,
          encPathStart,
          encTimeStart,
        ),
        upperBound: this.encodeKey(1, encSubspaceEnd, encPathEnd),
      };
    }

    const encTimeEnd = bigintToBytes(range.timeRange.end);

    return {
      lowerBound: this.encodeKey(
        1,
        encSubspaceStart,
        encPathStart,
        encTimeStart,
      ),
      upperBound: this.encodeKey(1, encSubspaceEnd, encPathEnd, encTimeEnd),
    };
  }

  private createPtsBounds(
    range: Range3d<SubspaceId>,
  ): {
    lowerBound: Uint8Array | undefined;
    upperBound: Uint8Array | undefined;
  } {
    const encPathStart = encodePathWithSeparators(range.pathRange.start);

    if (range.pathRange.end === OPEN_END) {
      return {
        lowerBound: this.encodeKey(0, encPathStart),
        upperBound: undefined,
      };
    }

    const encPathEnd = encodePathWithSeparators(range.pathRange.end);

    const encTimeStart = bigintToBytes(range.timeRange.start);

    if (range.timeRange.end === OPEN_END) {
      return {
        lowerBound: this.encodeKey(
          0,
          encPathStart,
          encTimeStart,
        ),
        upperBound: this.encodeKey(0, encPathEnd),
      };
    }

    const encTimeEnd = bigintToBytes(range.timeRange.end);

    const encSubspaceStart = this.subspaceScheme.encode(
      range.subspaceRange.start,
    );

    if (range.subspaceRange.end === OPEN_END) {
      return {
        lowerBound: this.encodeKey(
          0,
          encPathStart,
          encTimeStart,
          encSubspaceStart,
        ),
        upperBound: this.encodeKey(0, encPathEnd, encTimeEnd),
      };
    }

    const encSubspaceEnd = this.subspaceScheme.encode(range.subspaceRange.end);

    return {
      lowerBound: this.encodeKey(
        0,
        encPathStart,
        encTimeStart,
        encSubspaceStart,
      ),
      upperBound: this.encodeKey(0, encPathEnd, encTimeEnd, encSubspaceEnd),
    };
  }

  private createTspBounds(
    range: Range3d<SubspaceId>,
  ): {
    lowerBound: Uint8Array | undefined;
    upperBound: Uint8Array | undefined;
  } {
    const encTimeStart = bigintToBytes(range.timeRange.start);

    if (range.timeRange.end === OPEN_END) {
      return {
        lowerBound: this.encodeKey(2, encTimeStart),
        upperBound: undefined,
      };
    }

    const encTimeEnd = bigintToBytes(range.timeRange.end);

    const encSubspaceStart = this.subspaceScheme.encode(
      range.subspaceRange.start,
    );

    if (range.subspaceRange.end === OPEN_END) {
      return {
        lowerBound: this.encodeKey(
          2,
          encTimeStart,
          encSubspaceStart,
        ),
        upperBound: this.encodeKey(2, encTimeEnd),
      };
    }

    const encSubspaceEnd = this.subspaceScheme.encode(range.subspaceRange.end);

    const encPathStart = encodePathWithSeparators(range.pathRange.start);

    if (range.pathRange.end === OPEN_END) {
      return {
        lowerBound: this.encodeKey(
          2,
          encTimeStart,
          encSubspaceStart,
          encPathStart,
        ),
        upperBound: this.encodeKey(2, encTimeEnd, encSubspaceEnd),
      };
    }

    const encPathEnd = encodePathWithSeparators(range.pathRange.end);

    return {
      lowerBound: this.encodeKey(
        2,
        encTimeStart,
        encSubspaceStart,
        encPathStart,
      ),
      upperBound: this.encodeKey(2, encTimeEnd, encSubspaceEnd, encPathEnd),
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
