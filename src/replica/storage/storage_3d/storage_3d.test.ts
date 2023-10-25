import { assert } from "https://deno.land/std@0.202.0/assert/assert.ts";
import { concat, encodeBase64, Products } from "../../../../deps.ts";
import {
  makeNamespaceKeypair,
  makeSubspaceKeypair,
} from "../../../test/crypto.ts";
import {
  testSchemeAuthorisation,
  testSchemeFingerprint,
  testSchemeNamespace,
  testSchemePathLength,
  testSchemePayload,
  testSchemeSubspace,
} from "../../../test/test_schemes.ts";
import { getSubspaces, randomTimestamp } from "../../../test/utils.ts";
import { bigintToBytes, compareBytes } from "../../../util/bytes.ts";
import { Replica } from "../../replica.ts";
import { OptionalBounds, ProtocolParameters, Query } from "../../types.ts";
import { RadixishTree } from "../prefix_iterators/radixish_tree.ts";
import { MonoidRbTree } from "../summarisable_storage/monoid_rbtree.ts";
import { TripleStorage } from "./triple_storage.ts";
import { Storage3d } from "./types.ts";
import { sample } from "https://deno.land/std@0.198.0/collections/sample.ts";
import { encodeEntry } from "../../../entries/encode_decode.ts";
import { assertEquals } from "https://deno.land/std@0.202.0/assert/assert_equals.ts";
import { Entry } from "../../../entries/types.ts";

const emptyUi8 = new Uint8Array();

type Storage3dScenario<
  NamespaceKey,
  SubspaceKey,
  PayloadDigest,
  AuthorisationOpts,
  AuthorisationToken,
  Fingerprint,
> = {
  name: string;
  makeScenario: (
    namespace: NamespaceKey,
    params: ProtocolParameters<
      NamespaceKey,
      SubspaceKey,
      PayloadDigest,
      AuthorisationOpts,
      AuthorisationToken,
      Fingerprint
    >,
  ) => Promise<
    {
      storage: Storage3d<
        NamespaceKey,
        SubspaceKey,
        PayloadDigest,
        Fingerprint
      >;
      dispose: () => Promise<void>;
    }
  >;
};

const tripleStorageScenario = {
  name: "Triple storage",
  makeScenario: <
    NamespaceKey,
    SubspaceKey,
    PayloadDigest,
    AuthorisationOpts,
    AuthorisationToken,
    Fingerprint,
  >(
    namespace: NamespaceKey,
    params: ProtocolParameters<
      NamespaceKey,
      SubspaceKey,
      PayloadDigest,
      AuthorisationOpts,
      AuthorisationToken,
      Fingerprint
    >,
  ) => {
    const storage = new TripleStorage({
      namespace,
      ...params,
      createSummarisableStorage: (monoid) => {
        return new MonoidRbTree({
          monoid,
          compare: compareBytes,
        });
      },
    });

    return Promise.resolve({ storage, dispose: () => Promise.resolve() });
  },
};

const scenarios = [tripleStorageScenario];

Deno.test("Storage3d.insert, get, and remove", async (test) => {
  for (const scenario of scenarios) {
    const namespaceKeypair = await makeNamespaceKeypair();

    const { storage, dispose } = await scenario.makeScenario(
      namespaceKeypair.namespace,
      {
        namespaceScheme: testSchemeNamespace,
        subspaceScheme: testSchemeSubspace,
        pathLengthScheme: testSchemePathLength,
        payloadScheme: testSchemePayload,
        fingerprintScheme: testSchemeFingerprint,
        authorisationScheme: testSchemeAuthorisation,
      },
    );

    await test.step(scenario.name, async () => {
      const subspace = await makeSubspaceKeypair();
      const pathAndPayload = crypto.getRandomValues(new Uint8Array(8));

      const payloadHash = crypto.getRandomValues(new Uint8Array(32));
      const authTokenHash = crypto.getRandomValues(new Uint8Array(32));

      await storage.insert({
        path: pathAndPayload,
        payloadHash,
        authTokenHash,
        length: BigInt(8),
        subspace: subspace.subspace,
        timestamp: BigInt(1000),
      });

      const res = await storage.get(subspace.subspace, pathAndPayload);

      assert(res);

      assertEquals(res.entry.identifier.subspace, subspace.subspace);
      assertEquals(res.entry.identifier.path, pathAndPayload);
      assertEquals(res.entry.record.hash, payloadHash);
      assertEquals(res.authTokenHash, authTokenHash);

      await storage.remove(res.entry);

      const res2 = await storage.get(subspace.subspace, pathAndPayload);

      assert(res2 === undefined);
    });

    await dispose();
  }
});

Deno.test("Storage3d.summarise", async () => {
  // Design a totally ordered monoid
  const specialFingerprintScheme = {
    fingerprintSingleton(
      entry: Entry<null, number, Uint8Array>,
    ): Promise<[number, Uint8Array, bigint][]> {
      return Promise.resolve([[
        entry.identifier.subspace,
        entry.identifier.path,
        entry.record.timestamp,
      ]]);
    },
    fingerprintCombine(
      a: Array<[number, Uint8Array, bigint]>,
      b: Array<[number, Uint8Array, bigint]>,
    ) {
      const newFingerprint = [...a];

      for (const element of b) {
        const existing = newFingerprint.find(
          ([subspaceA, pathA, timestampA]) => {
            const [subspaceB, pathB, timestampB] = element;

            if (subspaceA !== subspaceB) return false;
            if (Products.orderPaths(pathA, pathB) !== 0) return false;
            if (Products.orderTimestamps(timestampA, timestampB) !== 0) {
              return false;
            }

            return true;
          },
        );

        if (existing) {
          continue;
        }

        newFingerprint.push(element);
      }

      newFingerprint.sort((a, b) => {
        const [subspaceA, pathA, timestampA] = a;
        const [subspaceB, pathB, timestampB] = b;

        if (subspaceA < subspaceB) return -1;
        if (subspaceA > subspaceB) return 1;
        if (Products.orderPaths(pathA, pathB) === -1) return -1;
        if (Products.orderPaths(pathA, pathB) === 1) return 1;
        if (Products.orderTimestamps(timestampA, timestampB) === -1) {
          return -1;
        }
        if (Products.orderTimestamps(timestampA, timestampB) === 1) {
          return 1;
        }

        return 0;
      });

      return newFingerprint;
    },
    neutral: [] as Array<[number, Uint8Array, bigint]>,
  };

  for (const scenario of scenarios) {
    const { storage, dispose } = await scenario.makeScenario(
      null,
      {
        namespaceScheme: {
          encode() {
            return new Uint8Array();
          },
          decode() {
            return null;
          },
          encodedLength() {
            return 0;
          },
          isEqual() {
            return true;
          },
        },
        subspaceScheme: {
          encode(value: number) {
            return new Uint8Array([value]);
          },
          decode(encoded) {
            return encoded[0];
          },
          encodedLength() {
            return 1;
          },
          isEqual(a, b) {
            return a === b;
          },
          order(a: number, b: number) {
            if (a < b) return -1;
            if (a > b) return 1;
            return 0;
          },
          minimalSubspaceKey: 0,
          successor(a) {
            return a + 1;
          },
        },
        payloadScheme: {
          encode(value: Uint8Array) {
            return value;
          },
          decode(encoded) {
            return encoded;
          },
          encodedLength(value: Uint8Array) {
            return value.byteLength;
          },
          fromBytes(bytes: Uint8Array | ReadableStream<Uint8Array>) {
            return Promise.resolve(bytes as Uint8Array);
          },
          order: compareBytes,
        },
        pathLengthScheme: {
          encode(value: number) {
            return new Uint8Array([value]);
          },
          decode(encoded) {
            return encoded[0];
          },
          encodedLength() {
            return 1;
          },
          maxLength: 4,
        },
        authorisationScheme: {
          isAuthorised() {
            return Promise.resolve(true);
          },
          authorise() {
            return Promise.resolve(null);
          },
          tokenEncoding: {
            encode() {
              return new Uint8Array();
            },
            decode() {
              return null;
            },
            encodedLength() {
              return 0;
            },
          },
        },
        fingerprintScheme: specialFingerprintScheme,
      },
    );

    // Create some random products using these (pull from Meadowcap)

    const summariseParams: {
      product: Products.ThreeDimensionalProduct<number>;
      countLimits?: { subspace?: number; path?: number; time?: number };
      sizeLimits?: { subspace?: bigint; path?: bigint; time?: bigint };
    }[] = [];

    for (let i = 0; i < 100; i++) {
      const randomCount = () => {
        return Math.random() > 0.5
          ? Math.floor(Math.random() * (3 - 1 + 1) + 1)
          : undefined;
      };

      const randomSize = () => {
        return Math.random() > 0.5
          ? BigInt(Math.floor(Math.random() * (64 - 16 + 1) + 16))
          : undefined;
      };

      const randomCounts = () => {
        return Math.random() > 0.5
          ? {
            subspace: randomCount(),
            path: randomCount(),
            time: randomCount(),
          }
          : undefined;
      };

      const randomSizes = () => {
        return Math.random() > 0.5
          ? {
            subspace: randomSize(),
            path: randomSize(),
            time: randomSize(),
          }
          : undefined;
      };

      summariseParams.push({
        product: getRandom3dProduct({
          noEmpty: true,
        }),
        countLimits: randomCounts(),
        sizeLimits: randomSizes(),
      });
    }

    // Define includedByProduct fn
    const includedBySummariseParams = (
      subspace: number,
      path: Uint8Array,
      time: bigint,
    ): {
      product: Products.ThreeDimensionalProduct<number>;
      countLimits?: { subspace?: number; path?: number; time?: number };
      sizeLimits?: { subspace?: bigint; path?: bigint; time?: bigint };
    }[] => {
      const includedProducts = [];

      for (const { product, countLimits, sizeLimits } of summariseParams) {
        if (
          Products.disjointIntervalIncludesValue(
            { order: orderNumbers },
            product[0],
            subspace,
          ) === false
        ) {
          continue;
        }

        if (
          Products.disjointIntervalIncludesValue(
            { order: Products.orderPaths },
            product[1],
            path,
          ) === false
        ) {
          continue;
        }

        if (
          Products.disjointIntervalIncludesValue(
            { order: Products.orderTimestamps },
            product[2],
            time,
          ) === false
        ) {
          continue;
        }

        includedProducts.push({ product, countLimits, sizeLimits });
      }

      return includedProducts;
    };

    // Define expected fingerprint map
    const actualFingerprintMap = new Map<
      Products.ThreeDimensionalProduct<number>,
      {
        fingerprint: [number, Uint8Array, bigint][];
        count: number;
        size: bigint;
      }
    >();

    for (const { product } of summariseParams) {
      actualFingerprintMap.set(product, {
        fingerprint: specialFingerprintScheme.neutral,
        count: 0,
        size: BigInt(0),
      });
    }

    const occupiedPaths = new Map<number, Set<number>>();

    const entries: Entry<null, number, Uint8Array>[] = [];

    // Generate some entries
    for (let i = 0; i < 100; i++) {
      const subspace = Math.floor(Math.random() * 100);

      const pathLastByte = Math.floor(Math.random() * 256);

      const pathAndPayload = new Uint8Array([
        0,
        0,
        0,
        pathLastByte,
      ]);
      const timestamp = randomTimestamp();

      if (occupiedPaths.get(subspace)?.has(pathLastByte)) {
        continue;
      }

      await storage.insert({
        subspace,
        path: pathAndPayload,
        timestamp: timestamp,
        length: BigInt(4),
        authTokenHash: new Uint8Array(),
        payloadHash: pathAndPayload,
      });

      const entry: Entry<null, number, Uint8Array> = {
        identifier: {
          namespace: null,
          subspace: subspace,
          path: pathAndPayload,
        },
        record: {
          hash: pathAndPayload,
          length: BigInt(4),
          timestamp,
        },
      };

      entries.push(entry);

      const usedPaths = occupiedPaths.get(subspace);

      if (!usedPaths) {
        occupiedPaths.set(subspace, new Set([pathLastByte]));
      } else {
        usedPaths.add(pathLastByte);
      }
    }

    entries.sort((a, b) => {
      const aKey = concat(
        new Uint8Array([a.identifier.subspace]),
        a.identifier.path,
        bigintToBytes(a.record.timestamp),
      );
      const bKey = concat(
        new Uint8Array([b.identifier.subspace]),
        b.identifier.path,
        bigintToBytes(b.record.timestamp),
      );

      return Products.orderPaths(aKey, bKey) * -1;
    });

    for (const entry of entries) {
      const includedBy = includedBySummariseParams(
        entry.identifier.subspace,
        entry.identifier.path,
        entry.record.timestamp,
      );

      for (const { product, countLimits, sizeLimits } of includedBy) {
        const { fingerprint, count, size } = actualFingerprintMap.get(product)!;

        const nextCount = count + 1;
        const nextSize = size + entry.record.length;

        const sclExceeded = countLimits?.subspace &&
          nextCount > countLimits.subspace;
        const pclExceeded = countLimits?.path && nextCount > countLimits.path;
        const tclExceeded = countLimits?.time && nextCount > countLimits.time;

        const sslExceeded = sizeLimits?.subspace &&
          nextSize > sizeLimits.subspace;
        const pslExceeded = sizeLimits?.path && nextSize > sizeLimits.path;
        const tslExceeded = sizeLimits?.time && nextSize > sizeLimits.time;

        if (
          sclExceeded || pclExceeded || tclExceeded || sslExceeded ||
          pslExceeded || tslExceeded
        ) {
          continue;
        }

        const lifted = await specialFingerprintScheme.fingerprintSingleton(
          entry,
        );

        actualFingerprintMap.set(
          product,
          {
            fingerprint: specialFingerprintScheme.fingerprintCombine(
              fingerprint,
              lifted,
            ),
            size: nextSize,
            count: nextCount,
          },
        );
      }
    }

    // For all products, see if fingerprint matches the expected one.
    for (const { product, countLimits, sizeLimits } of summariseParams) {
      const actual = await storage.summarise(product, countLimits, sizeLimits);
      const expected = actualFingerprintMap.get(product)!;

      assertEquals(actual.fingerprint, expected.fingerprint);
      assertEquals(actual.size, expected.count);

      let actualPayloadSize = 0;

      for (const element of actual.fingerprint) {
        actualPayloadSize += element[1].byteLength;
      }

      assertEquals(BigInt(actualPayloadSize), expected.size);
    }

    await dispose();
  }
});

Deno.test.ignore("Storage3d.entriesByProduct", async () => {
  // Items included by product appear
  // count limit is respected.
  // size limit is respected.

  // Generate limited range of subspaces
  // Paths
  // Timestamps

  // Create some random products using these (pull from Meadowcap)

  // Define includedByProduct fn

  // Define product inclusion map

  // Add event listener for entry removals

  // Generate some entries

  // For all products, see if all the products included are there.
});

Deno.test("Storage3d.entriesByQuery", async (test) => {
  for (const scenario of scenarios) {
    const namespaceKeypair = await makeNamespaceKeypair();

    const { storage, dispose } = await scenario.makeScenario(
      namespaceKeypair.namespace,
      {
        namespaceScheme: testSchemeNamespace,
        subspaceScheme: testSchemeSubspace,
        pathLengthScheme: testSchemePathLength,
        payloadScheme: testSchemePayload,
        fingerprintScheme: testSchemeFingerprint,
        authorisationScheme: testSchemeAuthorisation,
      },
    );

    const replica = new Replica({
      namespace: namespaceKeypair.namespace,
      protocolParameters: {
        authorisationScheme: testSchemeAuthorisation,
        namespaceScheme: testSchemeNamespace,
        subspaceScheme: testSchemeSubspace,
        pathLengthScheme: testSchemePathLength,
        payloadScheme: testSchemePayload,
        fingerprintScheme: testSchemeFingerprint,
      },
      entryDriver: {
        makeStorage: () => {
          return storage;
        },
        prefixIterator: new RadixishTree<Uint8Array>(),
        writeAheadFlag: {
          wasInserting: () => Promise.resolve(undefined),
          wasRemoving: () => Promise.resolve(undefined),
          flagInsertion: () => Promise.resolve(),
          flagRemoval: () => Promise.resolve(),
          unflagInsertion: () => Promise.resolve(),
          unflagRemoval: () => Promise.resolve(),
        },
      },
    });

    await test.step(scenario.name, async () => {
      // Generate the test queries

      const subspaces = await getSubspaces(10);
      const bytes = [];

      for (let i = 0; i < 50; i++) {
        bytes.push(crypto.getRandomValues(new Uint8Array(4)));
      }

      const timestamps = [];

      for (let i = 0; i < 25; i++) {
        timestamps.push(randomTimestamp());
      }

      // Bounds

      const subspaceBounds = manyRandomBounds(
        100,
        subspaces.map((s) => s.subspace),
        compareBytes,
      );
      const pathBounds = manyRandomBounds(100, bytes, Products.orderPaths);
      const timeBounds = manyRandomBounds(
        100,
        timestamps,
        Products.orderTimestamps,
      );

      const queries: Query<Uint8Array>[] = [];

      const includedByQueries = (
        subspace: Uint8Array,
        path: Uint8Array,
        time: bigint,
      ): Query<Uint8Array>[] => {
        const includedQueries = [];

        for (const query of queries) {
          if (query.subspace) {
            const range = rangeFromOptionalBounds(query.subspace, emptyUi8);

            const isIncluded = Products.rangeIncludesValue(
              { order: compareBytes },
              range,
              subspace,
            );

            if (!isIncluded) {
              continue;
            }
          }

          if (query.path) {
            const range = rangeFromOptionalBounds(query.path, emptyUi8);

            const isIncluded = Products.rangeIncludesValue(
              { order: Products.orderPaths },
              range,
              path,
            );

            if (!isIncluded) {
              continue;
            }
          }

          if (query.time) {
            const range = rangeFromOptionalBounds(query.time, BigInt(0));

            const isIncluded = Products.rangeIncludesValue(
              { order: Products.orderTimestamps },
              range,
              time,
            );

            if (!isIncluded) {
              continue;
            }
          }

          includedQueries.push(query);
        }

        return includedQueries;
      };

      for (let i = 0; i < 500; i++) {
        const orderRoll = Math.random();

        const query: Query<Uint8Array> = {
          limit: Math.random() < 0.1
            ? Math.floor(Math.random() * 10)
            : undefined,
          reverse: Math.random() < 0.25 ? true : false,
          order: orderRoll < 0.33
            ? "subspace"
            : orderRoll < 0.66
            ? "path"
            : "timestamp",
          subspace: Math.random() < 0.5 ? sample(subspaceBounds) : undefined,
          path: Math.random() < 0.5 ? sample(pathBounds) : undefined,
          time: Math.random() < 0.5 ? sample(timeBounds) : undefined,
        };

        queries.push(query);
      }

      const queryInclusionMap = new Map<
        Query<Uint8Array>,
        Set<string>
      >();

      for (const query of queries) {
        queryInclusionMap.set(query, new Set());
      }

      replica.addEventListener("entryremove", (event) => {
        const { detail: { removed } } = event as CustomEvent<
          { removed: Entry<Uint8Array, Uint8Array, Uint8Array> }
        >;

        const encodedEntry = encodeEntry(removed, {
          namespaceScheme: testSchemeNamespace,
          subspaceScheme: testSchemeSubspace,
          pathLengthScheme: testSchemePathLength,
          payloadScheme: testSchemePayload,
        });

        testSchemePayload.fromBytes(encodedEntry).then((hash) => {
          const b64 = encodeBase64(hash);

          for (const [, set] of queryInclusionMap) {
            set.delete(b64);
          }
        });
      });

      const entryAuthHashMap = new Map<string, string>();

      // Generate the entries
      for (let i = 0; i < 50; i++) {
        const pathAndPayload = sample(bytes)!;
        const chosenSubspace = sample(subspaces)!;
        const timestamp = sample(timestamps)!;

        const result = await replica.set({
          path: pathAndPayload,
          payload: pathAndPayload,
          subspace: chosenSubspace.subspace,
          timestamp,
        }, chosenSubspace.privateKey);

        if (result.kind !== "success") {
          continue;
        }

        // See if it belongs to any of the test queries.
        const correspondingQueries = includedByQueries(
          chosenSubspace.subspace,
          pathAndPayload,
          timestamp,
        );

        const encodedEntry = encodeEntry(result.entry, {
          namespaceScheme: testSchemeNamespace,
          subspaceScheme: testSchemeSubspace,
          pathLengthScheme: testSchemePathLength,
          payloadScheme: testSchemePayload,
        });

        const entryHash = await testSchemePayload.fromBytes(encodedEntry);

        const b64EntryHash = encodeBase64(entryHash);

        // We'll use a base64 encoding of the entry's signature as an ID
        const authTokenHash = await testSchemePayload.fromBytes(
          new Uint8Array(result.authToken),
        );
        const b64AuthHash = encodeBase64(authTokenHash);

        entryAuthHashMap.set(b64EntryHash, b64AuthHash);

        for (const query of correspondingQueries) {
          const set = queryInclusionMap.get(query)!;

          set.add(b64EntryHash);
        }
      }

      for (const query of queries) {
        let entriesRead = 0;

        const awaiting = new Set(queryInclusionMap.get(query));

        const prevIsCorrectOrder = (
          prev: Entry<Uint8Array, Uint8Array, ArrayBuffer>,
          curr: Entry<Uint8Array, Uint8Array, ArrayBuffer>,
          ord: "path" | "timestamp" | "subspace",
        ): boolean => {
          switch (ord) {
            case "path": {
              const order = compareBytes(
                prev.identifier.path,
                curr.identifier.path,
              );

              if (order === 0) {
                return prevIsCorrectOrder(prev, curr, "timestamp");
              }

              if (query.reverse) {
                return order === 1;
              }

              return order === -1;
            }
            case "timestamp": {
              const order = Products.orderTimestamps(
                prev.record.timestamp,
                curr.record.timestamp,
              );

              if (order === 0) {
                return prevIsCorrectOrder(prev, curr, "subspace");
              }

              if (query.reverse) {
                return order === 1;
              }

              return order === -1;
            }
            case "subspace": {
              const order = compareBytes(
                prev.identifier.subspace,
                curr.identifier.subspace,
              );

              if (order === 0) {
                return prevIsCorrectOrder(prev, curr, "path");
              }

              if (query.reverse) {
                return order === 1;
              }

              return order === -1;
            }
          }
        };

        let prevEntry: Entry<Uint8Array, Uint8Array, ArrayBuffer> | undefined;

        for await (
          const { entry, authTokenHash } of storage.entriesByQuery(query)
        ) {
          const encodedEntry = encodeEntry(entry, {
            namespaceScheme: testSchemeNamespace,
            subspaceScheme: testSchemeSubspace,
            pathLengthScheme: testSchemePathLength,
            payloadScheme: testSchemePayload,
          });

          const entryHash = await testSchemePayload.fromBytes(encodedEntry);

          const b64EntryHash = encodeBase64(entryHash);

          // We'll use a base64 encoding of the entry's signature as an ID
          const b64AuthHash = encodeBase64(authTokenHash);

          assertEquals(entryAuthHashMap.get(b64EntryHash), b64AuthHash);

          // Test order
          if (prevEntry) {
            assert(prevIsCorrectOrder(prevEntry, entry, query.order));
          }

          assert(queryInclusionMap.get(query)?.has(b64EntryHash));
          entriesRead += 1;
          prevEntry = entry;

          awaiting.delete(b64EntryHash);

          if (query.limit && entriesRead > query.limit) {
            assert(false, "Too many entries received for query");
          }
        }

        if (query.limit) {
          assertEquals(
            entriesRead,
            Math.min(query.limit, queryInclusionMap.get(query)!.size),
          );
        } else {
          assertEquals(entriesRead, queryInclusionMap.get(query)!.size);
        }
      }
    });

    await dispose();
  }
});

function manyRandomBounds<ValueType>(
  size: number,
  sampleFrom: Array<ValueType>,
  order: Products.TotalOrder<ValueType>,
) {
  const bounds = [];

  for (let i = 0; i < size; i++) {
    bounds.push(randomBounds(sampleFrom, order));
  }

  return bounds;
}

function randomBounds<ValueType>(
  sampleFrom: Array<ValueType>,
  order: Products.TotalOrder<ValueType>,
): OptionalBounds<ValueType> {
  const kindRoll = Math.random();

  if (kindRoll < 0.33) {
    return {
      lowerBound: sample(sampleFrom)!,
    };
  } else if (kindRoll < 0.66) {
    return {
      upperBound: sample(sampleFrom)!,
    };
  }

  while (true) {
    const fst = sample(sampleFrom)!;
    const snd = sample(sampleFrom)!;

    const fstSndOrder = order(fst, snd);

    if (fstSndOrder === 0) {
      continue;
    }

    if (fstSndOrder === -1) {
      return {
        lowerBound: fst,
        upperBound: snd,
      };
    }

    return {
      lowerBound: snd,
      upperBound: fst,
    };
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

// Product stuff

function getRandomDisjointInterval<ValueType>(
  { minValue, successor, order, maxSize }: {
    minValue: ValueType;
    successor: Products.SuccessorFn<ValueType>;
    maxSize: ValueType;
    order: Products.TotalOrder<ValueType>;
  },
): Products.DisjointInterval<ValueType> {
  let disjointInterval: Products.DisjointInterval<ValueType> = [];

  let start = minValue;
  let end = minValue;

  while (true) {
    start = end;

    while (true) {
      start = successor(start);

      if (Math.random() > 0.8) {
        break;
      }
    }

    end = start;

    while (true) {
      end = successor(end);

      if ((order(end, maxSize) >= 0) || Math.random() > 0.8) {
        break;
      }
    }

    if ((order(end, maxSize) >= 0)) {
      break;
    }

    disjointInterval = Products.addToDisjointInterval({ order: order }, {
      kind: "closed_exclusive",
      start,
      end,
    }, disjointInterval);

    if (Math.random() > 0.95) {
      break;
    }
  }

  const isOpen = order(end, maxSize) < 0 && Math.random() > 0.8;

  if (isOpen) {
    let openStart = end;

    while (true) {
      openStart = successor(openStart);

      if (order(end, maxSize) >= 0 || Math.random() > 0.9) {
        break;
      }
    }

    disjointInterval = Products.addToDisjointInterval({ order: order }, {
      kind: "open",
      start,
    }, disjointInterval);
  }

  return disjointInterval;
}

function getRandom3dProduct(
  { noEmpty }: {
    noEmpty?: boolean;
  },
): Products.ThreeDimensionalProduct<number> {
  const isEmpty = Math.random() > 0.75;

  if (!noEmpty && isEmpty) {
    return [[], [], []];
  }

  return [
    getRandomDisjointInterval({
      minValue: 0,
      maxSize: 255,
      order: (a, b) => {
        if (a < b) return -1;
        if (a > b) return 1;
        return 0;
      },
      successor: (a) => a + 1,
    }),
    getRandomDisjointInterval({
      minValue: new Uint8Array(),
      maxSize: new Uint8Array([0, 0, 0, 255]),
      order: Products.orderPaths,
      successor: Products.makeSuccessorPath(4),
    }),
    getRandomDisjointInterval({
      minValue: BigInt(0),
      maxSize: BigInt(1000),
      order: Products.orderTimestamps,
      successor: Products.successorTimestamp,
    }),
  ];
}

function orderNumbers(a: number, b: number) {
  if (a < b) return -1;
  if (a > b) return 1;
  return 0;
}
