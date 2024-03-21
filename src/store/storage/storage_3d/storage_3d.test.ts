import { assert } from "https://deno.land/std@0.202.0/assert/assert.ts";
import {
  ANY_SUBSPACE,
  AreaOfInterest,
  bigintToBytes,
  concat,
  encodeBase64,
  encodeEntry,
  Entry,
  isIncluded3d,
  isIncludedRange,
  isPathPrefixed,
  OPEN_END,
  orderBytes,
  orderPath,
  orderTimestamp,
  Path,
  Range,
  Range3d,
  successorPath,
} from "../../../../deps.ts";
import {
  TestNamespace,
  testSchemeAuthorisation,
  testSchemeFingerprint,
  testSchemeNamespace,
  testSchemePath,
  testSchemePayload,
  testSchemeSubspace,
  TestSubspace,
} from "../../../test/test_schemes.ts";
import {
  ALL_SUBSPACES,
  randomNamespace,
  randomPath,
  randomSubspace,
  randomTimestamp,
} from "../../../test/utils.ts";
import { LengthyEntry, ProtocolParameters, QueryOrder } from "../../types.ts";
import { MonoidRbTree } from "../summarisable_storage/monoid_rbtree.ts";
import { encodePathWithSeparators, TripleStorage } from "./triple_storage.ts";
import { RangeOfInterest, Storage3d } from "./types.ts";
import { assertEquals } from "https://deno.land/std@0.202.0/assert/assert_equals.ts";
import { sample } from "https://deno.land/std@0.202.0/collections/mod.ts";

import { Store } from "../../store.ts";
import { RadixTree } from "../prefix_iterators/radix_tree.ts";
import { assertRejects } from "https://deno.land/std@0.202.0/assert/assert_rejects.ts";
import { assertThrows } from "https://deno.land/std@0.202.0/assert/assert_throws.ts";

export type Storage3dScenario<
  NamespaceId,
  SubspaceId,
  PayloadDigest,
  AuthorisationOpts,
  AuthorisationToken,
  Fingerprint,
> = {
  name: string;
  makeScenario: (
    namespace: NamespaceId,
    params: ProtocolParameters<
      NamespaceId,
      SubspaceId,
      PayloadDigest,
      AuthorisationOpts,
      AuthorisationToken,
      Fingerprint
    >,
  ) => Promise<
    {
      storage: Storage3d<
        NamespaceId,
        SubspaceId,
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
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    AuthorisationOpts,
    AuthorisationToken,
    Fingerprint,
  >(
    namespace: NamespaceId,
    params: ProtocolParameters<
      NamespaceId,
      SubspaceId,
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
          compare: orderBytes,
        });
      },
      getPayloadLength: () => Promise.resolve(BigInt(0)),
    });

    return Promise.resolve({ storage, dispose: () => Promise.resolve() });
  },
};

const scenarios = [tripleStorageScenario];

Deno.test("Storage3d.insert, get, and remove", async (test) => {
  for (const scenario of scenarios) {
    const { storage, dispose } = await scenario.makeScenario(
      randomNamespace(),
      {
        namespaceScheme: testSchemeNamespace,
        subspaceScheme: testSchemeSubspace,
        pathScheme: testSchemePath,
        payloadScheme: testSchemePayload,
        fingerprintScheme: testSchemeFingerprint,
        authorisationScheme: testSchemeAuthorisation,
      },
    );

    await test.step(scenario.name, async () => {
      const subspace = randomSubspace();
      const path = randomPath();

      const payloadDigest = crypto.getRandomValues(new Uint8Array(32));
      const authTokenDigest = crypto.getRandomValues(new Uint8Array(32));

      await storage.insert({
        path,
        payloadDigest,
        authTokenDigest,
        length: BigInt(8),
        subspace: subspace,
        timestamp: BigInt(1000),
      });

      const res = await storage.get(subspace, path);

      assert(res);

      assertEquals(res.entry.subspaceId, subspace);
      assertEquals(res.entry.path, path);
      assertEquals(res.entry.payloadDigest, payloadDigest);
      assertEquals(res.authTokenHash, authTokenDigest);

      await storage.remove(res.entry);

      const res2 = await storage.get(subspace, path);

      assert(res2 === undefined);
    });

    await dispose();
  }
});

Deno.test("Storage3d.summarise", async () => {
  // A 'special' fingerprint which really just lists all the items it is made from.
  const specialFingerprintScheme = {
    fingerprintSingleton(
      lengthy: LengthyEntry<TestNamespace, TestSubspace, ArrayBuffer>,
    ): Promise<[number, Path, bigint, bigint][]> {
      return Promise.resolve([[
        lengthy.entry.subspaceId,
        lengthy.entry.path,
        lengthy.entry.timestamp,
        lengthy.entry.payloadLength,
      ]]);
    },
    fingerprintCombine(
      a: Array<[number, Path, bigint, bigint]>,
      b: Array<[number, Path, bigint, bigint]>,
    ) {
      const newFingerprint = [...a];

      // Remove duplicates

      for (const element of b) {
        const existing = newFingerprint.find(
          ([subspaceA, pathA, timestampA, lengthA]) => {
            const [subspaceB, pathB, timestampB, lengthB] = element;

            if (timestampA !== timestampB) {
              return false;
            }
            if (subspaceA !== subspaceB) return false;
            if (orderPath(pathA, pathB) !== 0) return false;

            if (lengthA !== lengthB) {
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
        const [subspaceA, pathA, timestampA, lengthA] = a;
        const [subspaceB, pathB, timestampB, lengthB] = b;

        if (timestampA < timestampB) {
          return -1;
        }
        if (timestampA > timestampB) {
          return 1;
        }
        if (subspaceA < subspaceB) return -1;
        if (subspaceA > subspaceB) return 1;
        if (orderPath(pathA, pathB) === -1) return -1;
        if (orderPath(pathA, pathB) === 1) return 1;

        if (lengthA < lengthB) {
          return -1;
        }
        if (lengthA > lengthB) {
          return 1;
        }

        return 0;
      });

      return newFingerprint;
    },
    neutral: [] as Array<[number, Path, bigint, bigint]>,
  };

  for (const scenario of scenarios) {
    const namespace = randomNamespace();

    const { storage, dispose } = await scenario.makeScenario(
      namespace,
      {
        namespaceScheme: testSchemeNamespace,
        subspaceScheme: testSchemeSubspace,
        payloadScheme: testSchemePayload,
        pathScheme: testSchemePath,
        authorisationScheme: testSchemeAuthorisation,
        fingerprintScheme: specialFingerprintScheme,
      },
    );

    // Create some random products using these (pull from Meadowcap)

    const aoiParams: AreaOfInterest<number>[] = [];

    for (let i = 0; i < 100; i++) {
      const randomCount = () => {
        return Math.random() > 0.5
          ? Math.floor(Math.random() * (3 - 1 + 1) + 1)
          : 0;
      };

      const randomSize = () => {
        return Math.random() > 0.5
          ? BigInt(Math.floor(Math.random() * (64 - 16 + 1) + 16))
          : BigInt(0);
      };

      const randomTimeRange = () => {
        const isOpen = Math.random() > 0.5;

        const start = BigInt(Math.floor(Math.random() * 1000));

        if (isOpen) {
          return {
            start,
            end: OPEN_END,
          } as Range<bigint>;
        }

        const end = start + BigInt(Math.floor(Math.random() * 1000));

        return { start, end };
      };

      aoiParams.push({
        area: {
          includedSubspaceId: randomSubspace(),
          pathPrefix: randomPath(),
          timeRange: randomTimeRange(),
        },
        maxCount: randomCount(),
        maxSize: randomSize(),
      });
    }

    // A function which returns all the areas a given spt is included by
    const isIncludedByAreas = (
      subspace: number,
      path: Path,
      time: bigint,
    ): AreaOfInterest<number>[] => {
      const inclusiveAois: AreaOfInterest<number>[] = [];

      for (const aoi of aoiParams) {
        if (
          aoi.area.includedSubspaceId !== ANY_SUBSPACE &&
          aoi.area.includedSubspaceId !== subspace
        ) {
          continue;
        }

        if (
          isPathPrefixed(aoi.area.pathPrefix, path) === false
        ) {
          continue;
        }

        if (
          isIncludedRange(orderTimestamp, aoi.area.timeRange, time) === false
        ) {
          continue;
        }

        inclusiveAois.push(aoi);
      }

      return inclusiveAois;
    };

    // Define expected fingerprint map
    const actualFingerprintMap = new Map<
      AreaOfInterest<number>,
      {
        fingerprint: [number, Path, bigint, bigint][];
        count: number;
        size: bigint;
      }
    >();

    for (const aoi of aoiParams) {
      actualFingerprintMap.set(aoi, {
        fingerprint: specialFingerprintScheme.neutral,
        count: 0,
        size: BigInt(0),
      });
    }

    const occupiedPaths = new Map<number, Set<number>>();

    const entries: Entry<TestNamespace, TestSubspace, Uint8Array>[] = [];

    // Generate some entries
    for (let i = 0; i < 100; i++) {
      const subspace = randomSubspace();

      const pathLastByte = Math.floor(Math.random() * 256);

      const path = randomPath();

      const authTokenDigest = new Uint8Array(
        await crypto.subtle.digest(
          "SHA-256",
          new Uint8Array(0),
        ),
      );

      const payloadDigest = new Uint8Array(
        await crypto.subtle.digest(
          "SHA-256",
          crypto.getRandomValues(new Uint8Array(16)),
        ),
      );

      const timestamp = randomTimestamp();

      if (occupiedPaths.get(subspace)?.has(pathLastByte)) {
        continue;
      }

      await storage.insert({
        subspace,
        path: path,
        timestamp: timestamp,
        length: BigInt(4),
        authTokenDigest: authTokenDigest,
        payloadDigest: payloadDigest,
      });

      const entry: Entry<TestNamespace, TestSubspace, Uint8Array> = {
        namespaceId: namespace,
        subspaceId: subspace,
        path: path,
        payloadDigest: payloadDigest,
        payloadLength: BigInt(4),
        timestamp,
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
        bigintToBytes(a.timestamp),
        new Uint8Array([a.subspaceId]),
        encodePathWithSeparators(a.path),
      );
      const bKey = concat(
        bigintToBytes(b.timestamp),
        new Uint8Array([b.subspaceId]),
        encodePathWithSeparators(b.path),
      );

      return orderBytes(aKey, bKey) * -1;
    });

    for (const entry of entries) {
      const includedBy = isIncludedByAreas(
        entry.subspaceId,
        entry.path,
        entry.timestamp,
      );

      for (const aoi of includedBy) {
        const { fingerprint, count, size } = actualFingerprintMap.get(aoi)!;

        const nextCount = count + 1;
        const nextSize = size + entry.payloadLength;

        const countExceeded = aoi.maxCount !== 0 && nextCount > aoi.maxCount;
        const sizeExceeded = aoi.maxSize !== BigInt(0) &&
          nextSize > aoi.maxSize;

        if (
          countExceeded || sizeExceeded
        ) {
          continue;
        }

        const lifted = await specialFingerprintScheme.fingerprintSingleton(
          { entry, available: BigInt(0) },
        );

        actualFingerprintMap.set(
          aoi,
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
    for (const aoi of aoiParams) {
      const boring = await storage.removeInterest(aoi);
      const actual = await storage.summarise(boring);
      const expected = actualFingerprintMap.get(aoi)!;

      assertEquals(
        actual.fingerprint,
        expected.fingerprint,
      );
      assertEquals(actual.size, expected.count);

      let actualPayloadSize = BigInt(0);

      for (const element of actual.fingerprint) {
        actualPayloadSize += element[3];
      }

      assertEquals(BigInt(actualPayloadSize), expected.size);
    }

    await dispose();
  }
});

Deno.test("Storage3d.query", async (test) => {
  for (const scenario of scenarios) {
    const namespace = randomNamespace();

    const { storage, dispose } = await scenario.makeScenario(
      namespace,
      {
        namespaceScheme: testSchemeNamespace,
        subspaceScheme: testSchemeSubspace,
        pathScheme: testSchemePath,
        payloadScheme: testSchemePayload,
        fingerprintScheme: testSchemeFingerprint,
        authorisationScheme: testSchemeAuthorisation,
      },
    );

    const store = new Store({
      namespace,
      protocolParameters: {
        authorisationScheme: testSchemeAuthorisation,
        namespaceScheme: testSchemeNamespace,
        subspaceScheme: testSchemeSubspace,
        pathScheme: testSchemePath,
        payloadScheme: testSchemePayload,
        fingerprintScheme: testSchemeFingerprint,
      },
      entryDriver: {
        makeStorage: () => {
          return storage;
        },
        prefixIterator: new RadixTree<Uint8Array>(),
        writeAheadFlag: {
          wasInserting: () => Promise.resolve(undefined),
          wasRemoving: () => Promise.resolve(undefined),
          flagInsertion: () => Promise.resolve(),
          flagRemoval: () => Promise.resolve(),
          unflagInsertion: () => Promise.resolve(),
          unflagRemoval: () => Promise.resolve(),
        },
        payloadReferenceCounter: {
          count: () => Promise.resolve(0),
          increment: () => Promise.resolve(0),
          decrement: () => Promise.resolve(0),
        },
      },
    });

    await test.step(scenario.name, async () => {
      // Generate the test queries

      const queryParams: {
        range: RangeOfInterest<TestSubspace>;
        order: QueryOrder;
        reverse: boolean;
      }[] = [];

      const bytes = [];
      const paths = [];

      for (let i = 0; i < 50; i++) {
        paths.push(randomPath());
      }

      for (let i = 0; i < 50; i++) {
        bytes.push(crypto.getRandomValues(new Uint8Array(4)));
      }

      const timestamps = [];

      for (let i = 0; i < 25; i++) {
        timestamps.push(randomTimestamp());
      }

      for (let i = 0; i < 100; i++) {
        const randomCount = () => {
          return Math.random() > 0.5
            ? Math.floor(Math.random() * (3 - 1 + 1) + 1)
            : 0;
        };

        const randomSize = () => {
          return Math.random() > 0.5
            ? BigInt(Math.floor(Math.random() * (64 - 16 + 1) + 16))
            : BigInt(0);
        };

        const randomTimeRange = () => {
          const isOpen = Math.random() > 0.5;

          const start = BigInt(Math.floor(Math.random() * 1000));

          if (isOpen) {
            return {
              start,
              end: OPEN_END,
            } as Range<bigint>;
          }

          const end = start + BigInt(Math.floor(Math.random() * 1000));

          return { start, end };
        };

        const randomSubspaceRange = () => {
          const isOpen = Math.random() > 0.5;

          const start = Math.floor(Math.random() * 6);

          if (isOpen) {
            return {
              start,
              end: OPEN_END,
            } as Range<number>;
          }

          const end = Math.min(
            6,
            start + Math.floor(Math.random() * (6 - start)),
          );

          return { start, end };
        };

        const randomPathRange = () => {
          const isOpen = Math.random() > 0.5;

          const start = randomPath();

          if (isOpen) {
            return {
              start,
              end: OPEN_END,
            } as Range<Path>;
          }

          let end = successorPath(start, testSchemePath);

          const iterations = Math.floor(Math.random() * 100);

          for (let i = 0; i < iterations; i++) {
            if (end === null) {
              break;
            }

            end = successorPath(end, testSchemePath);
          }

          if (end === null) {
            return {
              start,
              end: OPEN_END,
            } as Range<Path>;
          }

          return { start, end };
        };

        const orderRoll = Math.random();

        queryParams.push({
          range: {
            range: {
              pathRange: randomPathRange(),
              subspaceRange: randomSubspaceRange(),
              timeRange: randomTimeRange(),
            },
            maxCount: randomCount(),
            maxSize: randomSize(),
          },
          reverse: Math.random() < 0.25 ? true : false,
          order: orderRoll < 0.33
            ? "subspace"
            : orderRoll < 0.66
            ? "path"
            : "timestamp",
        });
      }

      // A function which returns all the areas a given spt is included by
      const isIncludedByRanges = (
        subspace: TestSubspace,
        path: Path,
        time: bigint,
      ): {
        range: RangeOfInterest<TestSubspace>;
        order: QueryOrder;
        reverse: boolean;
      }[] => {
        const inclusiveParams: {
          range: RangeOfInterest<TestSubspace>;
          order: QueryOrder;
          reverse: boolean;
        }[] = [];

        for (const params of queryParams) {
          if (
            isIncluded3d(testSchemeSubspace.order, params.range.range, {
              path,
              subspace,
              time,
            })
          ) {
            inclusiveParams.push(params);
          }
        }

        return inclusiveParams;
      };

      const actualResultMap = new Map<{
        range: RangeOfInterest<TestSubspace>;
        order: QueryOrder;
        reverse: boolean;
      }, Set<string>>();

      for (const params of queryParams) {
        actualResultMap.set(params, new Set());
      }

      store.addEventListener("entryremove", (event) => {
        const { detail: { removed } } = event as CustomEvent<
          { removed: Entry<TestNamespace, TestSubspace, Uint8Array> }
        >;

        const encodedEntry = encodeEntry({
          namespaceScheme: testSchemeNamespace,
          subspaceScheme: testSchemeSubspace,
          pathScheme: testSchemePath,
          payloadScheme: testSchemePayload,
        }, removed);

        testSchemePayload.fromBytes(encodedEntry).then((hash) => {
          const b64 = encodeBase64(hash);

          for (const [, set] of actualResultMap) {
            set.delete(b64);
          }
        });
      });

      const entryAuthHashMap = new Map<string, string>();

      // Generate the entries
      for (let i = 0; i < 50; i++) {
        const path = sample(paths)!;
        const payload = sample(bytes)!;

        const chosenSubspace = sample(ALL_SUBSPACES)!;
        const timestamp = sample(timestamps)!;

        const result = await store.set({
          path: path,
          payload: payload,
          subspace: chosenSubspace,
          timestamp,
        }, chosenSubspace);

        if (result.kind !== "success") {
          continue;
        }

        // See if it belongs to any of the test queries.
        const correspondingQueries = isIncludedByRanges(
          chosenSubspace,
          path,
          timestamp,
        );

        const encodedEntry = encodeEntry({
          namespaceScheme: testSchemeNamespace,
          subspaceScheme: testSchemeSubspace,
          pathScheme: testSchemePath,
          payloadScheme: testSchemePayload,
        }, result.entry);

        const entryHash = await testSchemePayload.fromBytes(encodedEntry);

        const b64EntryHash = encodeBase64(entryHash);

        // We'll use a base64 encoding of the entry's signature as an ID
        const authTokenHash = await testSchemePayload.fromBytes(
          new Uint8Array(result.authToken),
        );

        const b64AuthHash = encodeBase64(authTokenHash);

        entryAuthHashMap.set(b64EntryHash, b64AuthHash);

        for (const query of correspondingQueries) {
          const set = actualResultMap.get(query)!;

          set.add(b64EntryHash);
        }
      }

      for (const params of queryParams) {
        let entriesRead = 0;

        const awaiting = new Set(actualResultMap.get(params));

        const prevIsCorrectOrder = (
          prev: Entry<TestNamespace, TestSubspace, ArrayBuffer>,
          curr: Entry<TestNamespace, TestSubspace, ArrayBuffer>,
          ord: "path" | "timestamp" | "subspace",
        ): boolean => {
          switch (ord) {
            case "path": {
              const order = orderPath(
                prev.path,
                curr.path,
              );

              if (order === 0) {
                return prevIsCorrectOrder(prev, curr, "timestamp");
              }

              if (params.reverse) {
                return order === 1;
              }

              return order === -1;
            }
            case "timestamp": {
              const order = orderTimestamp(
                prev.timestamp,
                curr.timestamp,
              );

              if (order === 0) {
                return prevIsCorrectOrder(prev, curr, "subspace");
              }

              if (params.reverse) {
                return order === 1;
              }

              return order === -1;
            }
            case "subspace": {
              if (prev.subspaceId === curr.subspaceId) {
                return prevIsCorrectOrder(prev, curr, "path");
              }

              if (params.reverse) {
                return prev.subspaceId > curr.subspaceId;
              }

              return prev.subspaceId < curr.subspaceId;
            }
          }
        };

        let prevEntry:
          | Entry<TestNamespace, TestSubspace, ArrayBuffer>
          | undefined;

        for await (
          const { entry, authTokenHash } of storage.query(
            params.range,
            params.order,
            params.reverse,
          )
        ) {
          const encodedEntry = encodeEntry({
            namespaceScheme: testSchemeNamespace,
            subspaceScheme: testSchemeSubspace,
            pathScheme: testSchemePath,
            payloadScheme: testSchemePayload,
          }, entry);

          const entryHash = await testSchemePayload.fromBytes(encodedEntry);

          const b64EntryHash = encodeBase64(entryHash);

          // We'll use a base64 encoding of the entry's signature as an ID
          const b64AuthHash = encodeBase64(authTokenHash);

          assertEquals(entryAuthHashMap.get(b64EntryHash), b64AuthHash);

          // Test order
          if (prevEntry) {
            assert(prevIsCorrectOrder(prevEntry, entry, params.order));
          }

          assert(actualResultMap.get(params)?.has(b64EntryHash));
          entriesRead += 1;
          prevEntry = entry;

          awaiting.delete(b64EntryHash);

          if (params.range.maxCount && entriesRead > params.range.maxCount) {
            assert(false, "Too many entries received for query");
          }
        }

        if (params.range.maxCount !== 0) {
          assert(entriesRead <= params.range.maxCount);
        }

        if (params.range.maxSize !== BigInt(0)) {
          assert(entriesRead * 4 <= params.range.maxSize);
        }

        if (params.range.maxCount === 0 && params.range.maxSize === BigInt(0)) {
          assertEquals(entriesRead, actualResultMap.get(params)!.size);
        }
      }
    });

    await dispose();
  }
});

Deno.test("Storage3d.splitRange", async () => {
  for (const scenario of scenarios) {
    const namespace = randomNamespace();

    for (let sampleSize = 2; sampleSize < 32; sampleSize++) {
      const { storage, dispose } = await scenario.makeScenario(
        namespace,
        {
          namespaceScheme: testSchemeNamespace,
          subspaceScheme: testSchemeSubspace,
          pathScheme: testSchemePath,
          payloadScheme: testSchemePayload,
          fingerprintScheme: testSchemeFingerprint,
          authorisationScheme: testSchemeAuthorisation,
        },
      );

      const store = new Store({
        namespace,
        protocolParameters: {
          authorisationScheme: testSchemeAuthorisation,
          namespaceScheme: testSchemeNamespace,
          subspaceScheme: testSchemeSubspace,
          pathScheme: testSchemePath,
          payloadScheme: testSchemePayload,
          fingerprintScheme: testSchemeFingerprint,
        },
        entryDriver: {
          makeStorage: () => {
            return storage;
          },
          prefixIterator: new RadixTree<Uint8Array>(),
          writeAheadFlag: {
            wasInserting: () => Promise.resolve(undefined),
            wasRemoving: () => Promise.resolve(undefined),
            flagInsertion: () => Promise.resolve(),
            flagRemoval: () => Promise.resolve(),
            unflagInsertion: () => Promise.resolve(),
            unflagRemoval: () => Promise.resolve(),
          },
          payloadReferenceCounter: {
            count: () => Promise.resolve(0),
            increment: () => Promise.resolve(0),
            decrement: () => Promise.resolve(0),
          },
        },
      });

      for (let i = 0; i < sampleSize; i++) {
        await store.set({
          path: [new Uint8Array([i])],
          timestamp: BigInt(Math.floor((Math.random() * sampleSize) * 1000)),
          subspace: i % 7,
          payload: new Uint8Array(4),
        }, i % 7);
      }

      const range: Range3d<TestSubspace> = {
        subspaceRange: {
          start: TestSubspace.Alfie,
          end: OPEN_END,
        },
        pathRange: {
          start: [],
          end: OPEN_END,
        },
        timeRange: {
          start: BigInt(0),
          end: OPEN_END,
        },
      };

      const [left, right] = await store.splitRange(range, sampleSize);

      const { fingerprint: fingerprintL, size: sizeL } = await store.summarise(
        left,
      );
      const { fingerprint: fingerprintR, size: sizeR } = await store.summarise(
        right,
      );

      const { fingerprint, size } = await store.summarise(range);

      assertEquals(sizeL + sizeR, size);

      const combined = testSchemeFingerprint.fingerprintCombine(
        fingerprintL,
        fingerprintR,
      );

      assert(orderBytes(combined, fingerprint) === 0);

      await dispose();
    }
  }
});
