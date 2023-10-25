import { Products } from "../../../../deps.ts";
import { Entry } from "../../../entries/types.ts";
import { bigintToBytes } from "../../../util/bytes.ts";
import {
  FingerprintScheme,
  OptionalBounds,
  PathLengthScheme,
  PayloadScheme,
  Query,
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
  pathLengthScheme: PathLengthScheme;
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
  private pathLengthScheme: PathLengthScheme;
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
        this.pathLengthScheme,
      );

      // Decode the key.
      const { subspace, timestamp, path } = decodeEntryKey(
        key,
        order,
        this.subspaceScheme,
        values.pathLength,
      );

      const entry: Entry<NamespaceKey, SubspaceKey, PayloadDigest> = {
        identifier: {
          namespace: this.namespace,
          path,
          subspace,
        },
        record: {
          timestamp,
          hash: values.payloadHash,
          length: values.payloadLength,
        },
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
    this.pathLengthScheme = opts.pathLengthScheme;

    this.fingerprintScheme = opts.fingerprintScheme;
  }

  async get(
    subspace: SubspaceKey,
    path: Uint8Array,
  ): Promise<
    {
      entry: Entry<NamespaceKey, SubspaceKey, PayloadDigest>;
      authTokenHash: PayloadDigest;
    } | undefined
  > {
    const firstResult = this.entriesByQuery({
      subspace: {
        lowerBound: subspace,
        upperBound: this.subspaceScheme.successor(subspace),
      },
      path: {
        lowerBound: path,
        upperBound: Products.makeSuccessorPath(this.pathLengthScheme.maxLength)(
          path,
        ),
      },
      limit: 1,
      order: "subspace",
    });

    for await (const result of firstResult) {
      return result;
    }
  }

  async insert(
    { path, subspace, payloadHash, timestamp, length, authTokenHash }: {
      path: Uint8Array;
      subspace: SubspaceKey;
      payloadHash: PayloadDigest;
      timestamp: bigint;
      length: bigint;
      authTokenHash: PayloadDigest;
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

    const toStore = encodeSummarisableStorageValue(
      {
        payloadHash,
        payloadLength: length,
        authTokenHash: authTokenHash,
        payloadScheme: this.payloadScheme,
        pathLength: path.byteLength,
        pathLengthEncoding: this.pathLengthScheme,
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
        path: entry.identifier.path,
        timestamp: entry.record.timestamp,
        subspace: entry.identifier.subspace,
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
    product: Products.CanonicProduct<SubspaceKey>,
    countLimits?: { subspace?: number; path?: number; time?: number },
    sizeLimits?: { subspace?: bigint; path?: bigint; time?: bigint },
  ): Promise<{ fingerprint: Fingerprint; size: number }> {
    const [subspaceDisjoint, pathDisjoint, timeDisjoint] = product;

    // Get the empty product out the way.
    if (
      subspaceDisjoint.length === 0 && pathDisjoint.length === 0 &&
      timeDisjoint.length === 0
    ) {
      return {
        fingerprint: this.fingerprintScheme.neutral,
        size: 0,
      };
    }

    let fingerprint = this.fingerprintScheme.neutral;
    let size = 0;

    // These keep track of how much of the count and size limits we've used.
    let sclUsed = 0;
    let sslUsed = BigInt(0);
    let pclUsed = 0;
    let pslUsed = BigInt(0);
    let tclUsed = 0;
    let tslUsed = BigInt(0);

    let limitsExceeded = false;

    // Go backwards through each range of the subspace disjoint,
    // As we need to return greatest items first.
    for (
      let subspaceDjIdx = subspaceDisjoint.length - 1;
      subspaceDjIdx >= 0;
      subspaceDjIdx--
    ) {
      const subspaceRange = subspaceDisjoint[subspaceDjIdx];

      // Iterate through all the entries of each range.
      const subspaceEntriesLowerBound = subspaceRange.start;
      const subspaceEntriesUpperBound = subspaceRange.kind === "open"
        ? undefined
        : subspaceRange.kind === "closed_exclusive"
        ? subspaceRange.end
        : this.subspaceScheme.successor(subspaceRange.end);

      const subspaceEntries = this.sptStorage.entries(
        this.subspaceScheme.encode(subspaceEntriesLowerBound),
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
        subspaceEntriesUpperBound !== undefined
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
          this.pathLengthScheme,
        );

        // Decode the key.
        const { timestamp, path } = decodeEntryKey(
          subspaceEntry.key,
          "subspace",
          this.subspaceScheme,
          values.pathLength,
        );

        // Check that decoded time and subspace are included by both other dimensions
        let pathIncluded = false;

        for (
          let pathDisjointIdx = pathDisjoint.length - 1;
          pathDisjointIdx >= 0;
          pathDisjointIdx--
        ) {
          if (
            Products.rangeIncludesValue(
              { order: Products.orderPaths },
              pathDisjoint[pathDisjointIdx],
              path,
            )
          ) {
            pathIncluded = true;
            // If we're included in one, we don't need to check the others.
            break;
          }
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

        for (
          let timeDisjointIdx = timeDisjoint.length - 1;
          timeDisjointIdx >= 0;
          timeDisjointIdx--
        ) {
          if (
            Products.rangeIncludesValue(
              { order: Products.orderTimestamps },
              timeDisjoint[timeDisjointIdx],
              timestamp,
            )
          ) {
            timeIncluded = true;
            // If we're included in one, we don't need to check the others.
            break;
          }
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

        const nextSclUsed = sclUsed + 1;
        const nextPclUsed = pclUsed + 1;
        const nextTclUsed = tclUsed + 1;

        const nextSslUsed = sslUsed + values.payloadLength;
        const nextPslUsed = pslUsed + values.payloadLength;
        const nextTslUsed = tslUsed + values.payloadLength;

        const sclExceeded = countLimits?.subspace &&
          nextSclUsed > countLimits.subspace;
        const pclExceeded = countLimits?.path && nextPclUsed > countLimits.path;
        const tclExceeded = countLimits?.time && nextTclUsed > countLimits.time;

        const sslExceeded = sizeLimits?.subspace &&
          nextSslUsed > sizeLimits.subspace;
        const pslExceeded = sizeLimits?.path && nextPslUsed > sizeLimits.path;
        const tslExceeded = sizeLimits?.time && nextTslUsed > sizeLimits.time;

        if (
          sclExceeded || pclExceeded || tclExceeded || sslExceeded ||
          pslExceeded || tslExceeded
        ) {
          limitsExceeded = true;
          break;
        }

        sclUsed = nextSclUsed;
        pclUsed = nextPclUsed;
        tclUsed = nextTclUsed;

        sslUsed = nextSslUsed;
        pslUsed = nextPslUsed;
        tslUsed = nextTslUsed;

        // This entry is part of a contiguous range of included entries,
        // and it's the least included key we've encountered so far.
        leastIncluded = subspaceEntry.key;
      }

      // Calculate a range that was left over, if any.
      if (leastIncluded) {
        await updateFingerprint(leastIncluded);
      }

      // If the limits have been exceeded, we don't need to go through all the other ranges.
      if (limitsExceeded) {
        break;
      }
    }

    return {
      fingerprint,
      size,
    };
  }

  async *entriesByProduct(
    product: Products.CanonicProduct<SubspaceKey>,
    countLimits?: { subspace?: number; path?: number; time?: number },
    sizeLimits?: { subspace?: bigint; path?: bigint; time?: bigint },
  ): AsyncIterable<{
    entry: Entry<NamespaceKey, SubspaceKey, PayloadDigest>;
    authTokenHash: PayloadDigest;
  }> {
    const [subspaceDisjoint, pathDisjoint, timeDisjoint] = product;

    // Get the empty product out the way.
    if (
      subspaceDisjoint.length === 0 && pathDisjoint.length === 0 &&
      timeDisjoint.length === 0
    ) {
      return;
    }

    // These keep track of how much of the count and size limits we've used.
    let sclUsed = 0;
    let sslUsed = BigInt(0);
    let pclUsed = 0;
    let pslUsed = BigInt(0);
    let tclUsed = 0;
    let tslUsed = BigInt(0);

    let limitsExceeded = false;

    // Go backwards through each range of the subspace disjoint,
    // As we need to return greatest items first.
    for (
      let subspaceDjIdx = subspaceDisjoint.length - 1;
      subspaceDjIdx >= 0;
      subspaceDjIdx--
    ) {
      const subspaceRange = subspaceDisjoint[subspaceDjIdx];

      // Iterate through all the entries of each range.
      const subspaceEntriesLowerBound = subspaceRange.start;
      const subspaceEntriesUpperBound = subspaceRange.kind === "open"
        ? undefined
        : subspaceRange.kind === "closed_exclusive"
        ? subspaceRange.end
        : this.subspaceScheme.successor(subspaceRange.end);

      const subspaceEntries = this.sptStorage.entries(
        this.subspaceScheme.encode(subspaceEntriesLowerBound),
        subspaceEntriesUpperBound
          ? this.subspaceScheme.encode(subspaceEntriesUpperBound)
          : undefined,
        {
          reverse: true,
        },
      );

      for await (const subspaceEntry of subspaceEntries) {
        // Decode the key.
        const values = decodeSummarisableStorageValue(
          subspaceEntry.value,
          this.payloadScheme,
          this.pathLengthScheme,
        );

        // Decode the key.
        const { timestamp, path, subspace } = decodeEntryKey(
          subspaceEntry.key,
          "subspace",
          this.subspaceScheme,
          values.pathLength,
        );

        // Check that decoded time and subspace are included by both other dimensions
        let pathIncluded = false;

        for (
          let pathDisjointIdx = pathDisjoint.length - 1;
          pathDisjointIdx >= 0;
          pathDisjointIdx--
        ) {
          if (
            Products.rangeIncludesValue(
              { order: Products.orderPaths },
              pathDisjoint[pathDisjointIdx],
              path,
            )
          ) {
            pathIncluded = true;
            // If we're included in one, we don't need to check the others.
            break;
          }
        }

        // Not included, continue to the next entry.
        if (!pathIncluded) {
          continue;
        }

        let timeIncluded = false;

        for (
          let timeDisjointIdx = timeDisjoint.length - 1;
          timeDisjointIdx >= 0;
          timeDisjointIdx--
        ) {
          if (
            Products.rangeIncludesValue(
              { order: Products.orderTimestamps },
              timeDisjoint[timeDisjointIdx],
              timestamp,
            )
          ) {
            timeIncluded = true;
            // If we're included in one, we don't need to check the others.
            break;
          }
        }

        // Not included, continue to the next entry.
        if (!timeIncluded) {
          continue;
        }

        // Now we know this entry is included.

        // Check all dimension count and size limits.
        // If any limits have been exceeded, we have to stop here.

        // Boring.

        const nextSclUsed = sclUsed + 1;
        const nextPclUsed = pclUsed + 1;
        const nextTclUsed = tclUsed + 1;

        const nextSslUsed = sslUsed + values.payloadLength;
        const nextPslUsed = pslUsed + values.payloadLength;
        const nextTslUsed = tslUsed + values.payloadLength;

        const sclExceeded = countLimits?.subspace &&
          nextSclUsed > countLimits.subspace;
        const pclExceeded = countLimits?.path && nextPclUsed > countLimits.path;
        const tclExceeded = countLimits?.time && nextTclUsed > countLimits.time;

        const sslExceeded = sizeLimits?.subspace &&
          nextSslUsed > sizeLimits.subspace;
        const pslExceeded = sizeLimits?.path && nextPslUsed > sizeLimits.path;
        const tslExceeded = sizeLimits?.time && nextTslUsed > sizeLimits.time;

        if (
          sclExceeded || pclExceeded || tclExceeded || sslExceeded ||
          pslExceeded || tslExceeded
        ) {
          limitsExceeded = true;
          break;
        }

        sclUsed = nextSclUsed;
        pclUsed = nextPclUsed;
        tclUsed = nextTclUsed;

        sslUsed = nextSslUsed;
        pslUsed = nextPslUsed;
        tslUsed = nextTslUsed;

        yield {
          entry: {
            identifier: {
              namespace: this.namespace,
              subspace,
              path,
            },
            record: {
              hash: values.payloadHash,
              length: values.payloadLength,
              timestamp: timestamp,
            },
          },
          authTokenHash: values.authTokenHash,
        };
      }

      // If the limits have been exceeded, we don't need to go through all the other ranges.
      if (limitsExceeded) {
        break;
      }
    }
  }

  async *entriesByQuery(
    query: Query<SubspaceKey>,
  ): AsyncIterable<{
    entry: Entry<NamespaceKey, SubspaceKey, PayloadDigest>;
    authTokenHash: PayloadDigest;
  }> {
    const storage = query.order === "subspace"
      ? this.sptStorage
      : query.order === "path"
      ? this.ptsStorage
      : this.tspStorage;

    if (!query.subspace && !query.path && !query.time) {
      const allEntriesOnOrder = storage.entries(undefined, undefined, {
        limit: query.limit,
        reverse: query.reverse,
      });

      for await (const { key, value } of allEntriesOnOrder) {
        const values = decodeSummarisableStorageValue(
          value,
          this.payloadScheme,
          this.pathLengthScheme,
        );

        // Decode the key.
        const { subspace, timestamp, path } = decodeEntryKey(
          key,
          query.order,
          this.subspaceScheme,
          values.pathLength,
        );

        yield {
          entry: {
            identifier: {
              namespace: this.namespace,
              subspace,
              path,
            },
            record: {
              hash: values.payloadHash,
              length: values.payloadLength,
              timestamp,
            },
          },
          authTokenHash: values.authTokenHash,
        };
      }

      return;
    }

    let lowerBound: Uint8Array | undefined;
    let upperBound: Uint8Array | undefined;

    const leastPath = new Uint8Array();

    if (query.order === "path" && query.path) {
      lowerBound = query.path.lowerBound;
      upperBound = query.path.upperBound;
    } else if (query.order === "subspace" && query.subspace) {
      lowerBound = query.subspace.lowerBound
        ? this.subspaceScheme.encode(query.subspace.lowerBound)
        : undefined;
      upperBound = query.subspace.upperBound
        ? this.subspaceScheme.encode(query.subspace.upperBound)
        : undefined;
    } else if (query.order === "timestamp" && query.time) {
      lowerBound = query.time.lowerBound
        ? bigintToBytes(query.time.lowerBound)
        : undefined;
      upperBound = query.time.upperBound
        ? bigintToBytes(query.time.upperBound)
        : undefined;
    }

    let entriesYielded = 0;

    const iterator = storage.entries(lowerBound, upperBound, {
      reverse: query.reverse,
    });

    for await (const { key, value } of iterator) {
      const values = decodeSummarisableStorageValue(
        value,
        this.payloadScheme,
        this.pathLengthScheme,
      );

      // Decode the key.
      const { subspace, timestamp, path } = decodeEntryKey(
        key,
        query.order,
        this.subspaceScheme,
        values.pathLength,
      );

      if (
        (query.order === "path" || query.order === "timestamp") &&
        query.subspace
      ) {
        const isIncludedInSubspaceRange = Products.rangeIncludesValue(
          {
            order: this.subspaceScheme.order,
          },
          rangeFromOptionalBounds(
            query.subspace,
            this.subspaceScheme.minimalSubspaceKey,
          ),
          subspace,
        );

        if (!isIncludedInSubspaceRange) {
          continue;
        }
      }

      if (
        (query.order === "path" || query.order === "subspace") && query.time
      ) {
        const isIncluded = Products.rangeIncludesValue(
          { order: Products.orderTimestamps },
          rangeFromOptionalBounds(query.time, BigInt(0)),
          timestamp,
        );

        if (!isIncluded) {
          continue;
        }
      }

      if (
        (query.order === "subspace" || query.order === "timestamp") &&
        query.path
      ) {
        const isIncludedInPathRange = Products.rangeIncludesValue(
          { order: Products.orderPaths },
          rangeFromOptionalBounds(query.path, leastPath),
          path,
        );

        if (!isIncludedInPathRange) {
          continue;
        }
      }

      entriesYielded += 1;

      yield {
        entry: {
          identifier: {
            namespace: this.namespace,
            subspace,
            path,
          },
          record: {
            hash: values.payloadHash,
            length: values.payloadLength,
            timestamp,
          },
        },
        authTokenHash: values.authTokenHash,
      };

      if (query.limit && entriesYielded === query.limit) {
        break;
      }
    }
  }
}

function rangeFromOptionalBounds<ValueType>(
  bounds: OptionalBounds<ValueType>,
  leastValue: ValueType,
): Products.Range<ValueType> {
  if (bounds.lowerBound && !bounds.upperBound) {
    return {
      kind: "open",
      start: bounds.lowerBound,
    };
  }

  if (bounds.upperBound && !bounds.lowerBound) {
    return {
      kind: "closed_exclusive",
      start: leastValue,
      end: bounds.upperBound,
    };
  }

  return {
    kind: "closed_exclusive",
    start: bounds.lowerBound!,
    end: bounds.upperBound!,
  };
}
