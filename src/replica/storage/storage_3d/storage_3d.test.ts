import { assert } from "https://deno.land/std@0.202.0/assert/assert.ts";
import { encodeBase32, encodeBase64, Products } from "../../../../deps.ts";
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
import { compareBytes } from "../../../util/bytes.ts";
import { Replica } from "../../replica.ts";
import { OptionalBounds, Query } from "../../types.ts";
import { RadixishTree } from "../prefix_iterators/radixish_tree.ts";
import { MonoidRbTree } from "../summarisable_storage/monoid_rbtree.ts";
import { TripleStorage } from "./triple_storage.ts";
import { Storage3d } from "./types.ts";
import { sample } from "https://deno.land/std@0.198.0/collections/sample.ts";
import { encodeEntry } from "../../../entries/encode_decode.ts";
import { assertEquals } from "https://deno.land/std@0.202.0/assert/assert_equals.ts";
import { Entry } from "../../../entries/types.ts";

const emptyUi8 = new Uint8Array();

type Storage3dScenario<NamespaceKey, SubspaceKey, PayloadDigest, Fingerprint> =
  {
    name: string;
    makeScenario: (namespace: NamespaceKey) => Promise<
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

const tripleStorageScenario: Storage3dScenario<
  Uint8Array,
  Uint8Array,
  ArrayBuffer,
  Uint8Array
> = {
  name: "Triple storage",
  makeScenario: (namespace) => {
    const storage = new TripleStorage({
      namespace,
      subspaceScheme: testSchemeSubspace,
      pathLengthScheme: testSchemePathLength,
      payloadScheme: testSchemePayload,
      fingerprintScheme: testSchemeFingerprint,
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

Deno.test("entriesByQuery", async () => {
  // Items must come in right order
  // Must use bound correctly
  // Must respect limit
  // Must respect reverse

  // Create a replica.

  // Insert a random number of entries.

  // Hold onto these entries for checking presence later

  for (const scenario of scenarios) {
    const namespaceKeypair = await makeNamespaceKeypair();

    const { storage, dispose } = await scenario.makeScenario(
      namespaceKeypair.namespace,
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
        limit: Math.random() < 0.1 ? Math.floor(Math.random() * 10) : undefined,
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
