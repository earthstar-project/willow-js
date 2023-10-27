import { assertEquals } from "https://deno.land/std@0.202.0/testing/asserts.ts";
import { RadixishTree } from "./radixish_tree.ts";
import { PrefixIterator } from "./types.ts";
import { KeyHopTree } from "./key_hop_tree.ts";
import { KvDriverDeno } from "../kv/kv_driver_deno.ts";
import { SimpleKeyIterator } from "./simple_key_iterator.ts";

const MAX_PATH_LENGTH = 100;
const MAX_PATH_SETS = 64;

function generateRandomPath() {
  const pathLength = Math.floor(Math.random() * MAX_PATH_LENGTH + 1);

  const path = new Uint8Array(pathLength);

  for (let i = 0; i < pathLength; i++) {
    const byte = Math.floor(Math.random() * (255));

    path.set([byte], i);
  }

  return path;
}

function prefixesFromPath(path: Uint8Array) {
  const prefixes: Uint8Array[] = [];

  for (let i = 1; i < path.byteLength; i++) {
    if (Math.random() > 0.5) {
      const prefix = path.subarray(0, i);

      prefixes.push(prefix);
    }
  }

  return prefixes;
}

type PathSet = Uint8Array[];

function getRandomPathAndPrefixes() {
  const path = generateRandomPath();
  const prefixes = prefixesFromPath(path);
  return [...prefixes, path];
}

function getPaths() {
  const pathSets: PathSet[] = [];

  for (let i = 0; i < MAX_PATH_SETS; i++) {
    pathSets.push(getRandomPathAndPrefixes());
  }

  return pathSets;
}

type PrefixIteratorScenario = {
  name: string;
  makeScenario: () => Promise<
    {
      iterator: PrefixIterator<Uint8Array>;
      dispose: () => Promise<void>;
    }
  >;
};

const radixishTreeScenario: PrefixIteratorScenario = {
  name: "Radixish tree",
  makeScenario: () => {
    return Promise.resolve({
      iterator: new RadixishTree<Uint8Array>(),
      dispose: () => Promise.resolve(),
    });
  },
};

const keyhopTreeScenario: PrefixIteratorScenario = {
  name: "KeyHop tree",
  makeScenario: async () => {
    const kv = await Deno.openKv();
    const kvDriver = new KvDriverDeno(kv);
    const keyhopTree = new KeyHopTree<Uint8Array>(kvDriver);
    await kvDriver.clear();

    return {
      iterator: keyhopTree,
      dispose: () => Promise.resolve(kv.close()),
    };
  },
};

const simpleKeyIteratorScenario: PrefixIteratorScenario = {
  name: "Simple key iterator",
  makeScenario: async () => {
    const kv = await Deno.openKv();
    const kvDriver = new KvDriverDeno(kv);
    const simpleKeyIteratorScenario = new SimpleKeyIterator<Uint8Array>(
      kvDriver,
    );
    await kvDriver.clear();

    return {
      iterator: simpleKeyIteratorScenario,
      dispose: () => Promise.resolve(kv.close()),
    };
  },
};

const scenarios = [
  radixishTreeScenario,
  simpleKeyIteratorScenario,
  keyhopTreeScenario,
];

Deno.test("Prefix Iterator", async (test) => {
  const allPaths = getPaths();

  for (const scenario of scenarios) {
    await test.step(scenario.name, async () => {
      for (const pathSet of allPaths) {
        const { iterator, dispose } = await scenario.makeScenario();

        const remaining = new Set(pathSet);

        while (remaining.size > 0) {
          // Get a random path
          const idx = Math.floor(Math.random() * (remaining.size - 1));
          const remainingArr = Array.from(remaining);
          const itemToInsert = remainingArr[idx];

          await iterator.insert(itemToInsert, itemToInsert);

          if (Math.random() > 0.75) {
            await iterator.insert(itemToInsert, itemToInsert);
          }

          remaining.delete(itemToInsert);
        }

        // Find a random index in the pathset length that is not zero or the path set length.
        const splitPoint = Math.floor(
          Math.random() * (pathSet.length - 2) + 1,
        );

        const expectedPrefixes = pathSet.slice(0, splitPoint);
        const expectedPrefixedBy = pathSet.slice(splitPoint + 1);

        const actualPrefixes: Uint8Array[] = [];

        for await (
          const [key, value] of iterator.prefixesOf(pathSet[splitPoint])
        ) {
          assertEquals(key, value);

          actualPrefixes.push(key);
        }

        assertEquals(actualPrefixes, expectedPrefixes);

        // Use the iterator to get actual prefixes and prefixed by...
        const actualPrefixedBy = [];

        for await (
          const [key, value] of iterator.prefixedBy(pathSet[splitPoint])
        ) {
          assertEquals(key, value);

          actualPrefixedBy.push(key);
        }

        assertEquals(actualPrefixedBy, expectedPrefixedBy);

        // Now remove a random element that isn't the split point.
        const expectedPrefixesAfterRemoval = new Set(expectedPrefixes);
        const expectedPrefixedByAfterRemoval = new Set(expectedPrefixedBy);

        for (let i = 0; i < pathSet.length; i++) {
          if (Math.random() > 0.75) {
            const item = pathSet[i];

            await iterator.remove(item);
            expectedPrefixesAfterRemoval.delete(item);
            expectedPrefixedByAfterRemoval.delete(item);
          }
        }

        const actualPrefixesAfterRemoval: Uint8Array[] = [];

        for await (
          const [key, value] of iterator.prefixesOf(pathSet[splitPoint])
        ) {
          assertEquals(key, value);

          actualPrefixesAfterRemoval.push(key);
        }

        assertEquals(
          actualPrefixesAfterRemoval,
          Array.from(expectedPrefixesAfterRemoval),
        );

        // Use the iterator to get actual prefixes and prefixed by...
        const actualPrefixedByAfterRemoval = [];

        for await (
          const [key, value] of iterator.prefixedBy(pathSet[splitPoint])
        ) {
          assertEquals(key, value);

          actualPrefixedByAfterRemoval.push(key);
        }

        assertEquals(
          actualPrefixedByAfterRemoval,
          Array.from(expectedPrefixedByAfterRemoval),
        );

        await dispose();
      }
    });
  }
});
