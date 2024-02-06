import {
  ANY_SUBSPACE,
  AreaOfInterest,
  bigintToBytes,
  concat,
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
import {
  decodeEntryKey,
  decodeSummarisableStorageValue,
  encodeEntryKeys,
  encodeSummarisableStorageValue,
} from "../../util.ts";
import { LiftingMonoid } from "../summarisable_storage/lifting_monoid.ts";
import { SummarisableStorage } from "../summarisable_storage/types.ts";
import { Storage3d } from "./types.ts";

export type TripleStorageOpts<
  NamespaceKey,
  SubspaceKey,
  PayloadDigest,
  Fingerprint,
> = {
  namespace: NamespaceKey;
  /** Creates a {@link SummarisableStorage} with a given ID, used for storing entries and their data. */
  createSummarisableStorage: (
    monoid: LiftingMonoid<Uint8Array, Fingerprint>,
    id: string,
  ) => SummarisableStorage<Uint8Array, Fingerprint>;
  subspaceScheme: SubspaceScheme<SubspaceKey>;
  payloadScheme: PayloadScheme<PayloadDigest>;
  pathScheme: PathScheme;
  fingerprintScheme: FingerprintScheme<
    NamespaceKey,
    SubspaceKey,
    PayloadDigest,
    Fingerprint
  >;
};

export class TripleStorage<
  NamespaceKey,
  SubspaceKey,
  PayloadDigest,
  Fingerprint,
> implements
  Storage3d<
    NamespaceKey,
    SubspaceKey,
    PayloadDigest,
    Fingerprint
  > {
  private namespace: NamespaceKey;

  private ptsStorage: SummarisableStorage<Uint8Array, Fingerprint>;
  private sptStorage: SummarisableStorage<Uint8Array, Fingerprint>;
  private tspStorage: SummarisableStorage<Uint8Array, Fingerprint>;
  private subspaceScheme: SubspaceScheme<SubspaceKey>;
  private payloadScheme: PayloadScheme<PayloadDigest>;
  private pathScheme: PathScheme;
  private fingerprintScheme: FingerprintScheme<
    NamespaceKey,
    SubspaceKey,
    PayloadDigest,
    Fingerprint
  >;

  constructor(
    opts: TripleStorageOpts<
      NamespaceKey,
      SubspaceKey,
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
      const values = decodeSummarisableStorageValue(
        value,
        this.payloadScheme,
      );

      // Decode the key.
      const { subspace, timestamp, path } = decodeEntryKey(
        key,
        order,
        this.subspaceScheme,
        values.encodedPathLength,
      );

      const entry: Entry<NamespaceKey, SubspaceKey, PayloadDigest> = {
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
    this.pathScheme = opts.pathScheme;

    this.fingerprintScheme = opts.fingerprintScheme;
  }

  async get(
    subspace: SubspaceKey,
    path: Path,
  ): Promise<
    {
      entry: Entry<NamespaceKey, SubspaceKey, PayloadDigest>;
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
      subspace: SubspaceKey;
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

    // console.log(keys.spt);

    const toStore = encodeSummarisableStorageValue(
      {
        payloadDigest,
        payloadLength: length,
        authTokenDigest: authTokenDigest,
        payloadScheme: this.payloadScheme,
        encodedPathLength: keys.encodedPathLength,
      },
    );

    await Promise.all([
      this.ptsStorage.insert(keys.pts, toStore),
      this.sptStorage.insert(keys.spt, toStore),
      this.tspStorage.insert(keys.tsp, toStore),
    ]);
  }

  async remove(
    entry: Entry<NamespaceKey, SubspaceKey, PayloadDigest>,
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
    areaOfInterest: AreaOfInterest<SubspaceKey>,
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
        : this.subspaceScheme.minimalSubspaceKey,
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
      const values = decodeSummarisableStorageValue(
        subspaceEntry.value,
        this.payloadScheme,
      );

      // Decode the key.
      const { timestamp, path } = decodeEntryKey(
        subspaceEntry.key,
        "subspace",
        this.subspaceScheme,
        values.encodedPathLength,
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
    areaOfInterest: AreaOfInterest<SubspaceKey>,
    order: QueryOrder,
    reverse = false,
  ): AsyncIterable<{
    entry: Entry<NamespaceKey, SubspaceKey, PayloadDigest>;
    authTokenHash: PayloadDigest;
  }> {
    const storage = order === "subspace"
      ? this.sptStorage
      : order === "path"
      ? this.ptsStorage
      : this.tspStorage;

    const includesAllTime = areaOfInterest.area.timeRange.start === BigInt(0) &&
      areaOfInterest.area.timeRange.end === OPEN_END;
    const includesAllPaths = areaOfInterest.area.pathPrefix.length === 0;
    const includesAllSubspaces =
      areaOfInterest.area.includedSubspaceId === ANY_SUBSPACE;

    // Do the simplest thing if the area starts from the lowest value and is open ended in all dimensions.
    if (
      includesAllTime &&
      includesAllPaths &&
      includesAllSubspaces &&
      areaOfInterest.maxSize === BigInt(0) &&
      areaOfInterest.maxCount === 0
    ) {
      const allEntriesOnOrder = storage.entries(undefined, undefined, {
        limit: areaOfInterest.maxCount,
        reverse: reverse,
      });

      for await (const { key, value } of allEntriesOnOrder) {
        const values = decodeSummarisableStorageValue(
          value,
          this.payloadScheme,
        );

        // Decode the key.
        const { subspace, timestamp, path } = decodeEntryKey(
          key,
          order,
          this.subspaceScheme,
          values.encodedPathLength,
        );

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
      }

      return;
    }

    let lowerBound: Uint8Array | undefined;
    let upperBound: Uint8Array | undefined;

    if (order === "path") {
      lowerBound = concat(...areaOfInterest.area.pathPrefix);

      const maybeSuccessorPrefix = successorPrefix(
        areaOfInterest.area.pathPrefix,
      );

      if (maybeSuccessorPrefix) {
        upperBound = concat(...maybeSuccessorPrefix);
      }
    } else if (
      order === "subspace" &&
      areaOfInterest.area.includedSubspaceId !== ANY_SUBSPACE
    ) {
      lowerBound = this.subspaceScheme.encode(
        areaOfInterest.area.includedSubspaceId,
      );

      const maybeSuccessorSubspace = this.subspaceScheme.successor(
        areaOfInterest.area.includedSubspaceId,
      );

      if (maybeSuccessorSubspace) {
        upperBound = this.subspaceScheme.encode(maybeSuccessorSubspace);
      }
    } else if (order === "timestamp") {
      if (areaOfInterest.area.timeRange.start > BigInt(0)) {
        lowerBound = bigintToBytes(areaOfInterest.area.timeRange.start);
      }

      if (areaOfInterest.area.timeRange.end !== OPEN_END) {
        upperBound = bigintToBytes(areaOfInterest.area.timeRange.start);
      }
    }

    let entriesYielded = 0;
    let payloadBytesYielded = BigInt(0);

    const iterator = storage.entries(lowerBound, upperBound, {
      reverse,
    });

    for await (const { key, value } of iterator) {
      const values = decodeSummarisableStorageValue(
        value,
        this.payloadScheme,
      );

      // Decode the key.
      const { subspace, timestamp, path } = decodeEntryKey(
        key,
        order,
        this.subspaceScheme,
        values.encodedPathLength,
      );

      if (
        (order === "path" || order === "timestamp") &&
        areaOfInterest.area.includedSubspaceId !== ANY_SUBSPACE
      ) {
        const isSubspace = this.subspaceScheme.order(
          subspace,
          areaOfInterest.area.includedSubspaceId,
        );

        if (!isSubspace) {
          continue;
        }
      }

      if ((order === "path" || order === "subspace") && !includesAllTime) {
        const isIncluded = isIncludedRange(
          orderTimestamp,
          areaOfInterest.area.timeRange,
          timestamp,
        );

        if (!isIncluded) {
          continue;
        }
      }

      if (
        (order === "subspace" || order === "timestamp") && !includesAllPaths
      ) {
        const isIncluded = isPathPrefixed(areaOfInterest.area.pathPrefix, path);

        if (!isIncluded) {
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
}
