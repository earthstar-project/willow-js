import { assert } from "https://deno.land/std@0.202.0/assert/assert.ts";
import {
  ANY_SUBSPACE,
  AreaOfInterest,
  bigintToBytes,
  concat,
  Entry,
  isIncludedRange,
  isPathPrefixed,
  OPEN_END,
  orderBytes,
  orderPath,
  orderTimestamp,
  Path,
  Range,
} from "../../../../deps.ts";
import {
  makeNamespaceKeypair,
  makeSubspaceKeypair,
} from "../../../test/crypto.ts";
import {
  testSchemeAuthorisation,
  testSchemeFingerprint,
  testSchemeNamespace,
  testSchemePath,
  testSchemePayload,
  testSchemeSubspace,
} from "../../../test/test_schemes.ts";
import { randomPath, randomTimestamp } from "../../../test/utils.ts";
import { ProtocolParameters } from "../../types.ts";
import { MonoidRbTree } from "../summarisable_storage/monoid_rbtree.ts";
import { TripleStorage } from "./triple_storage.ts";
import { Storage3d } from "./types.ts";
import { assertEquals } from "https://deno.land/std@0.202.0/assert/assert_equals.ts";

export type Storage3dScenario<
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
          compare: orderBytes,
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
        pathScheme: testSchemePath,
        payloadScheme: testSchemePayload,
        fingerprintScheme: testSchemeFingerprint,
        authorisationScheme: testSchemeAuthorisation,
      },
    );

    await test.step(scenario.name, async () => {
      const subspace = await makeSubspaceKeypair();
      const path = randomPath();

      const payloadDigest = crypto.getRandomValues(new Uint8Array(32));
      const authTokenDigest = crypto.getRandomValues(new Uint8Array(32));

      await storage.insert({
        path,
        payloadDigest,
        authTokenDigest,
        length: BigInt(8),
        subspace: subspace.subspace,
        timestamp: BigInt(1000),
      });

      const res = await storage.get(subspace.subspace, path);

      assert(res);

      assertEquals(res.entry.subspaceId, subspace.subspace);
      assertEquals(res.entry.path, path);
      assertEquals(res.entry.payloadDigest, payloadDigest);
      assertEquals(res.authTokenHash, authTokenDigest);

      await storage.remove(res.entry);

      const res2 = await storage.get(subspace.subspace, path);

      assert(res2 === undefined);
    });

    await dispose();
  }
});

Deno.test("Storage3d.summarise", async () => {
  // A 'special' fingerprint which really just lists all the items it is made from.
  const specialFingerprintScheme = {
    fingerprintSingleton(
      entry: Entry<null, number, Uint8Array>,
    ): Promise<[number, Path, bigint, bigint][]> {
      return Promise.resolve([[
        entry.subspaceId,
        entry.path,
        entry.timestamp,
        entry.payloadLength,
      ]]);
    },
    fingerprintCombine(
      a: Array<[number, Path, bigint, bigint]>,
      b: Array<[number, Path, bigint, bigint]>,
    ) {
      const newFingerprint = [...a];

      // Remove dupleicates

      for (const element of b) {
        const existing = newFingerprint.find(
          ([subspaceA, pathA, timestampA, lengthA]) => {
            const [subspaceB, pathB, timestampB, lengthB] = element;

            if (subspaceA !== subspaceB) return false;
            if (orderPath(pathA, pathB) !== 0) return false;
            if (orderTimestamp(timestampA, timestampB) !== 0) {
              return false;
            }
            if (orderTimestamp(lengthA, lengthB) !== 0) {
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

        if (subspaceA < subspaceB) return -1;
        if (subspaceA > subspaceB) return 1;
        if (orderPath(pathA, pathB) === -1) return -1;
        if (orderPath(pathA, pathB) === 1) return 1;
        if (orderTimestamp(timestampA, timestampB) === -1) {
          return -1;
        }
        if (orderTimestamp(timestampA, timestampB) === 1) {
          return 1;
        }
        if (orderTimestamp(lengthA, lengthB) === -1) {
          return -1;
        }
        if (orderTimestamp(lengthA, lengthB) === 1) {
          return 1;
        }

        return 0;
      });

      return newFingerprint;
    },
    neutral: [] as Array<[number, Path, bigint, bigint]>,
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
          order(a: number, b: number) {
            if (a < b) return -1;
            if (a > b) return 1;
            return 0;
          },
          minimalSubspaceKey: 0,
          successor(a: number) {
            return a + 1;
          },
        },
        payloadScheme: testSchemePayload,
        pathScheme: testSchemePath,
        authorisationScheme: {
          isAuthorisedWrite() {
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

    const areaParams: AreaOfInterest<number>[] = [];

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

      const randomSubspaceId = () => {
        return Math.random() > 0.5
          ? Math.floor(Math.random() * 255)
          : ANY_SUBSPACE;
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

      areaParams.push({
        area: {
          includedSubspaceId: randomSubspaceId(),
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
      const inclusiveAreas: AreaOfInterest<number>[] = [];

      for (const aoi of areaParams) {
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

        inclusiveAreas.push(aoi);
      }

      return inclusiveAreas;
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

    for (const areaOfInterest of areaParams) {
      actualFingerprintMap.set(areaOfInterest, {
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

      const entry: Entry<null, number, Uint8Array> = {
        namespaceId: null,
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
        new Uint8Array([a.subspaceId]),
        ...a.path,
        bigintToBytes(a.timestamp),
      );
      const bKey = concat(
        new Uint8Array([b.subspaceId]),
        ...b.path,
        bigintToBytes(b.timestamp),
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
          entry,
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
    for (const aoi of areaParams) {
      const actual = await storage.summarise(aoi);
      const expected = actualFingerprintMap.get(aoi)!;

      assertEquals(actual.fingerprint, expected.fingerprint);
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

/*
Deno.test("Storage3d.query", async (test) => {
  for (const scenario of scenarios) {
    const namespaceKeypair = await makeNamespaceKeypair();

    const { storage, dispose } = await scenario.makeScenario(
      namespaceKeypair.namespace,
      {
        namespaceScheme: testSchemeNamespace,
        subspaceScheme: testSchemeSubspace,
        pathScheme: testSchemePath,
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

      const areaParams: AreaOfInterest<number>[] = [];

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

        const randomSubspaceId = () => {
          return Math.random() > 0.5
            ? Math.floor(Math.random() * 255)
            : ANY_SUBSPACE;
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

        areaParams.push({
          area: {
            includedSubspaceId: randomSubspaceId(),
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
        const inclusiveAreas: AreaOfInterest<number>[] = [];

        for (const aoi of areaParams) {
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

          inclusiveAreas.push(aoi);
        }

        return inclusiveAreas;
      };

      const actualResultMap = new Map<
        AreaOfInterest<number>,
        Set<string>
      >();

      for (const areaOfInterest of areaParams) {
        actualResultMap.set(areaOfInterest, new Set());
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

          for (const [, set] of actualResultMap) {
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
          const set = actualResultMap.get(query)!;

          set.add(b64EntryHash);
        }
      }

      for (const query of queries) {
        let entriesRead = 0;

        const awaiting = new Set(actualResultMap.get(query));

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

          assert(actualResultMap.get(query)?.has(b64EntryHash));
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
            Math.min(query.limit, actualResultMap.get(query)!.size),
          );
        } else {
          assertEquals(entriesRead, actualResultMap.get(query)!.size);
        }
      }
    });

    await dispose();
  }
});

*/

function orderNumbers(a: number, b: number) {
  if (a < b) return -1;
  if (a > b) return 1;
  return 0;
}
