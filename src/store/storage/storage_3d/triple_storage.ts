import {
  AreaOfInterest,
  areaTo3dRange,
  bigintToBytes,
  concat,
  EncodingScheme,
  Entry,
  isIncluded3d,
  isIncludedRange,
  isPathPrefixed,
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
import { KvKey } from "../kv/types.ts";

export type TripleStorageOpts<
  NamespaceId,
  SubspaceId,
  PayloadDigest,
  Prefingerprint,
  Fingerprint,
> = {
  namespace: NamespaceId;
  /** Creates a {@link SummarisableStorage} with a given ID, used for storing entries and their data. */
  createSummarisableStorage: (
    monoid: LiftingMonoid<[KvKey, Uint8Array], Prefingerprint>,
    id: string,
  ) => SummarisableStorage<KvKey, Uint8Array, Prefingerprint>;
  subspaceScheme: SubspaceScheme<SubspaceId>;
  payloadScheme: PayloadScheme<PayloadDigest>;
  pathScheme: PathScheme;
  fingerprintScheme: FingerprintScheme<
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    Prefingerprint,
    Fingerprint
  >;
  getPayloadLength: (digest: PayloadDigest) => Promise<bigint>;
};

/** A `Storage3d` made up of three `SummarisableStorage` holding the same data in three different orders: subspace, path, and timestamp. */
export class TripleStorage<
  NamespaceId,
  SubspaceId,
  PayloadDigest,
  Prefingerprint,
  Fingerprint,
> implements
  Storage3d<
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    Prefingerprint
  > {
  private namespace: NamespaceId;

  private ptsStorage: SummarisableStorage<KvKey, Uint8Array, Prefingerprint>;
  private sptStorage: SummarisableStorage<KvKey, Uint8Array, Prefingerprint>;
  private tspStorage: SummarisableStorage<KvKey, Uint8Array, Prefingerprint>;
  private subspaceScheme: SubspaceScheme<SubspaceId>;
  private payloadScheme: PayloadScheme<PayloadDigest>;
  private fingerprintScheme: FingerprintScheme<
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    Prefingerprint,
    Fingerprint
  >;
  private pathScheme: PathScheme;

  constructor(
    opts: TripleStorageOpts<
      NamespaceId,
      SubspaceId,
      PayloadDigest,
      Prefingerprint,
      Fingerprint
    >,
  ) {
    this.namespace = opts.namespace;

    const lift = async (
      key: KvKey,
      value: Uint8Array,
      order: "path" | "subspace" | "timestamp",
    ) => {
      const values = decodeKvValue(
        value,
        this.payloadScheme,
      );

      // Decode the key.
      const { subspace, timestamp, path } = this.decodeEntryKey(
        key,
        order,
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
    const keys = this.encodeEntryKeys(
      {
        path,
        timestamp,
        subspace,
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
      this.ptsStorage.insert(keys.pts!, toStore),
      this.sptStorage.insert(keys.spt!, toStore),
      this.tspStorage.insert(keys.tsp!, toStore),
    ]);
  }

  async remove(
    entry: Entry<NamespaceId, SubspaceId, PayloadDigest>,
  ): Promise<boolean> {
    const keys = this.encodeEntryKeys(
      {
        path: entry.path,
        timestamp: entry.timestamp,
        subspace: entry.subspaceId,
      },
    );

    const results = await Promise.all([
      this.ptsStorage.remove(keys.pts!),
      this.tspStorage.remove(keys.tsp!),
      this.sptStorage.remove(keys.spt!),
    ]);

    return results[0];
  }

  async summarise(
    range: Range3d<SubspaceId>,
  ): Promise<{ fingerprint: Prefingerprint; size: number }> {
    let prefingerprint = this.fingerprintScheme.neutral;

    /** The size of the fingerprint. */
    let size = 0;

    const { lowerBound, upperBound } = this.createBounds(range, "timestamp");

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
    let leastIncluded: KvKey | undefined;

    /** Run this when we detect a contiguous range of included entries. */
    const updateFingerprint = async (start: KvKey) => {
      const { fingerprint: includedFp, size: includedSize } = await this
        .tspStorage.summarise(
          start,
          leastExcluded,
        );

      prefingerprint = this.fingerprintScheme.fingerprintCombine(
        prefingerprint,
        includedFp,
      );

      size += includedSize;

      // Prevent this from running again until we run into another included entry.
      leastIncluded = undefined;
    };

    for await (const entry of timeEntries) {
      // Decode the key.
      const { timestamp, path, subspace } = this.decodeEntryKey(
        entry.key,
        "timestamp",
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
      fingerprint: prefingerprint,
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

    const { lowerBound, upperBound } = this.createBounds(range, "timestamp");

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
      const { timestamp, path, subspace } = this.decodeEntryKey(
        entry.key,
        "timestamp",
      );

      if (!isPathPrefixed(areaOfInterest.area.pathPrefix, path)) {
        continue;
      }

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

      if (upperboundTime === undefined || timestamp > upperboundTime) {
        if (timestamp === BigInt(2 ** 64 - 1)) {
          // TODO: account for the largest possible bigint.
          //upperboundTime = OPEN_END;
        } else {
          upperboundTime = timestamp + BigInt(1);
        }
      }

      if (
        upperboundSubspace === undefined || upperboundSubspace &&
          this.subspaceScheme.order(subspace, upperboundSubspace) === 1
      ) {
        upperboundSubspace = this.subspaceScheme.successor(subspace);
      }

      if (
        !upperboundPath === undefined ||
        upperboundPath && orderPath(path, upperboundPath) === 1
      ) {
        upperboundPath = successorPath(path, this.pathScheme);
      }

      if (lowerBoundTime === undefined || timestamp < lowerBoundTime) {
        lowerBoundTime = timestamp;
      }

      if (
        lowerBoundSubspace === undefined ||
        this.subspaceScheme.order(subspace, lowerBoundSubspace) === -1
      ) {
        lowerBoundSubspace = subspace;
      }

      if (
        lowerBoundPath === undefined || orderPath(path, lowerBoundPath) === -1
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

    const { lowerBound, upperBound } = this.createBounds(
      rangeOfInterest.range,
      order,
    );

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
      const { subspace, timestamp, path } = this.decodeEntryKey(
        key,
        order,
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

  private createBounds(
    range: Range3d<SubspaceId>,
    order: "subspace" | "path" | "timestamp",
  ): {
    lowerBound: KvKey | undefined;
    upperBound: KvKey | undefined;
  } {
    const lowerBounds = this.encodeEntryKeys({
      subspace: range.subspaceRange.start,
      path: range.pathRange.start,
      timestamp: range.timeRange.start,
    });

    const upperBound = this.encodeEntryKeys({
      subspace: range.subspaceRange.end !== OPEN_END
        ? range.subspaceRange.end
        : undefined,
      path: range.pathRange.end !== OPEN_END ? range.pathRange.end : undefined,
      timestamp: range.timeRange.end !== OPEN_END
        ? range.timeRange.end
        : undefined,
    });

    switch (order) {
      case "subspace":
        return {
          lowerBound: lowerBounds.spt,
          upperBound: upperBound.spt,
        };

      case "path":
        return {
          lowerBound: lowerBounds.pts,
          upperBound: upperBound.pts,
        };
      case "timestamp":
        return {
          lowerBound: lowerBounds.tsp,
          upperBound: upperBound.tsp,
        };
    }
  }

  /** Encodes the subspace, path, and time of an entry into three keys for three respective orderings.
   */
  encodeEntryKeys(
    opts: {
      subspace?: SubspaceId;
      path?: Path;
      timestamp?: bigint;
    },
  ): {
    spt: KvKey | undefined;
    pts: KvKey | undefined;
    tsp: KvKey | undefined;
  } {
    const encodedSubspace = opts.subspace !== undefined
      ? this.subspaceScheme.encode(opts.subspace)
      : undefined;
    const encodedPath = opts.path !== undefined
      ? encodePathWithSeparators(opts.path)
      : undefined;
    const encodedTime = opts.timestamp !== undefined
      ? bigintToBytes(opts.timestamp)
      : undefined;

    return {
      spt: denseArr(encodedSubspace, encodedPath, encodedTime),
      pts: denseArr(encodedPath, encodedTime, encodedSubspace),
      tsp: denseArr(encodedTime, encodedSubspace, encodedPath),
    };
  }

  /** Decodes a key back into subspace, path, and timestamp. */
  decodeEntryKey(
    encoded: KvKey,
    order: "subspace" | "path" | "timestamp",
  ): {
    subspace: SubspaceId;
    path: Path;
    timestamp: bigint;
  } {
    let subspace: SubspaceId;
    let timestamp: bigint;
    let path: Path;

    const fst = encoded[0] as Uint8Array;
    const snd = encoded[1] as Uint8Array;
    const thd = encoded[2] as Uint8Array;

    switch (order) {
      case "subspace": {
        subspace = this.subspaceScheme.decode(fst);

        path = decodePathWithSeparators(snd);

        const dataView = new DataView(thd.buffer);
        timestamp = dataView.getBigUint64(0);

        break;
      }
      case "path": {
        path = decodePathWithSeparators(fst);

        const dataView = new DataView(snd.buffer);
        timestamp = dataView.getBigUint64(0);

        subspace = this.subspaceScheme.decode(thd);

        break;
      }
      case "timestamp": {
        const dataView = new DataView(fst.buffer);
        timestamp = dataView.getBigUint64(0);

        subspace = this.subspaceScheme.decode(snd);

        path = decodePathWithSeparators(thd);
      }
    }

    return {
      subspace,
      path,
      timestamp,
    };
  }
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

function denseArr<T>(...items: (T | undefined)[]): T[] | undefined {
  const denseItems: T[] = [];

  for (const item of items) {
    if (item === undefined) {
      break;
    }

    denseItems.push(item);
  }

  if (denseItems.length === 0) {
    return undefined;
  }

  return denseItems;
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
