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
    countLimit?: number | undefined,
    sizeLimit?: bigint | undefined,
  ): Promise<{ fingerprint: Fingerprint; size: number }> {
    // Okay. How do I create a fingerprint from three summarisable storages?

    const [subspaceDisjoint, pathDisjoint, timeDisjoint] = product;

    let fingerprint = this.fingerprintScheme.neutral;
    let size = 0;

    for (const subspaceRange of subspaceDisjoint) {
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
      );

      let payloadLimitUsed = BigInt(0);
      let countLimitUsed = 0;

      const qualifyingEntries: [
        Uint8Array | undefined,
        Uint8Array | undefined,
      ] = [undefined, undefined];

      const updateQualifyingEntries = (key: Uint8Array) => {
        if (qualifyingEntries[0] === undefined) {
          qualifyingEntries[0] = key;
        } else {
          qualifyingEntries[1] = key;
        }
      };

      const updateFingerprint = async () => {
        const [fst, last] = qualifyingEntries;

        if (!fst) {
          return;
        }

        const { fingerprint: includedFp, size: includedSize } = await this
          .sptStorage.summarise(
            fst,
            last ? last : new Uint8Array(),
          );

        fingerprint = this.fingerprintScheme.fingerprintCombine(
          fingerprint,
          includedFp,
        );
        size += includedSize;

        qualifyingEntries[0] = undefined;
        qualifyingEntries[1] = undefined;

        return;
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
          "path",
          this.subspaceScheme,
          values.pathLength,
        );

        // Check that decoded time and subspace are included by both other dimensions
        let pathIncluded = false;

        for (const range of pathDisjoint) {
          if (
            Products.rangeIncludesValue(
              { order: Products.orderPaths },
              range,
              path,
            )
          ) {
            pathIncluded = true;
            break;
          }
        }

        if (!pathIncluded) {
          // Update the fingerprint using the last accepted thing, if there.
          await updateFingerprint();

          continue;
        }

        let timeIncluded = false;

        for (const range of timeDisjoint) {
          if (
            Products.rangeIncludesValue(
              { order: Products.orderTimestamps },
              range,
              timestamp,
            )
          ) {
            timeIncluded = true;
            break;
          }
        }

        if (!timeIncluded) {
          // Update fingerprint using last updated thing, if there

          await updateFingerprint();

          continue;
        }

        payloadLimitUsed += values.payloadLength;

        if (sizeLimit && payloadLimitUsed > sizeLimit) {
          break;
        }

        countLimitUsed += 1;

        if (countLimit && countLimitUsed > countLimit) {
          break;
        }

        updateQualifyingEntries(subspaceEntry.key);
      }

      await updateFingerprint();
    }

    return {
      fingerprint,
      size,
    };
  }

  async *entriesByProduct(
    product: Products.CanonicProduct<SubspaceKey>,
    countLimit?: number | undefined,
    sizeLimit?: bigint | undefined,
  ): AsyncIterable<{
    entry: Entry<NamespaceKey, SubspaceKey, PayloadDigest>;
    authTokenHash: PayloadDigest;
  }> {
    const [subspaceDisjoint, pathDisjoint, timeDisjoint] = product;

    if (
      subspaceDisjoint.length === 0 && pathDisjoint.length === 0 &&
      timeDisjoint.length === 0
    ) {
      //empty product
      return;
    }

    let payloadLimitUsed = BigInt(0);
    let countLimitUsed = 0;

    // Iterate over each interval in the path disjoint.
    for (const interval of pathDisjoint) {
      const iter = interval.kind === "closed_exclusive"
        ? this.ptsStorage.entries(interval.start, interval.end)
        : this.ptsStorage.entries(interval.start, undefined);

      for await (const { key, value } of iter) {
        const values = decodeSummarisableStorageValue(
          value,
          this.payloadScheme,
          this.pathLengthScheme,
        );

        // Decode the key.
        const { subspace, timestamp, path } = decodeEntryKey(
          key,
          "path",
          this.subspaceScheme,
          values.pathLength,
        );

        // Check that decoded time and subspace are included by both other dimensions
        let subspaceIncluded = false;

        for (const range of subspaceDisjoint) {
          if (
            Products.rangeIncludesValue(
              { order: this.subspaceScheme.order },
              range,
              subspace,
            )
          ) {
            subspaceIncluded = true;
            break;
          }
        }

        if (!subspaceIncluded) {
          continue;
        }

        let timeIncluded = false;

        for (const range of timeDisjoint) {
          if (
            Products.rangeIncludesValue(
              { order: Products.orderTimestamps },
              range,
              timestamp,
            )
          ) {
            timeIncluded = true;
            break;
          }
        }

        if (!timeIncluded) {
          continue;
        }

        payloadLimitUsed += values.payloadLength;

        if (sizeLimit && payloadLimitUsed > sizeLimit) {
          break;
        }

        countLimitUsed += 1;

        if (countLimit && countLimitUsed > countLimit) {
          break;
        }

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

      if (entriesYielded === query.limit) {
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
