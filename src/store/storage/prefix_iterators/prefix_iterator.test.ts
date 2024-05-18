import { RadixTree } from "./radix_tree.ts";
import type { PrefixIterator } from "./types.ts";
import { KvDriverDeno } from "../kv/kv_driver_deno.ts";
import { SimpleKeyIterator } from "./simple_key_iterator.ts";
import { randomPath } from "../../../test/utils.ts";
import { PrefixedDriver } from "../kv/prefixed_driver.ts";
import { type Path, prefixesOf } from "@earthstar/willow-utils";
import { concat } from "@std/bytes";
import { assertEquals } from "@std/assert";

const MAX_PATH_SETS = 64;

type PathSet = Path[];

function getRandomPathAndPrefixes() {
  const path = randomPath();
  return prefixesOf(path);
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

const radixTreeScenario: PrefixIteratorScenario = {
  name: "Radix tree",
  makeScenario: () => {
    return Promise.resolve({
      iterator: new RadixTree<Uint8Array>(),
      dispose: () => Promise.resolve(),
    });
  },
};

const simpleKeyIteratorScenario: PrefixIteratorScenario = {
  name: "Simple key iterator",
  makeScenario: async () => {
    const kv = await Deno.openKv();
    const kvDriver = new PrefixedDriver(["test"], new KvDriverDeno(kv));
    const simpleKeyIteratorScenario = new SimpleKeyIterator<Uint8Array>(
      kvDriver,
    );

    return {
      iterator: simpleKeyIteratorScenario,
      dispose: async () => {
        await kvDriver.clear();

        kv.close();
      },
    };
  },
};

const scenarios = [
  radixTreeScenario,
  simpleKeyIteratorScenario,
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
          const pathToInsert = remainingArr[idx];
          const valueToInsert = concat(pathToInsert);

          await iterator.insert(pathToInsert, valueToInsert);

          if (Math.random() > 0.75) {
            await iterator.insert(pathToInsert, valueToInsert);
          }

          remaining.delete(pathToInsert);
        }

        // Find a random index in the pathset length that is not zero or the path set length.
        const splitPoint = Math.floor(
          Math.random() * (pathSet.length - 2) + 1,
        );

        const expectedPrefixes = pathSet.slice(0, splitPoint);
        const expectedPrefixedBy = pathSet.slice(splitPoint);

        const actualPrefixes: Path[] = [];

        const pathToTest = pathSet[splitPoint];

        for await (
          const [path, value] of iterator.prefixesOf(pathToTest)
        ) {
          assertEquals(concat(path), value);

          actualPrefixes.push(path);
        }

        assertEquals(actualPrefixes, expectedPrefixes);

        // Use the iterator to get actual prefixes and prefixed by...
        const actualPrefixedBy = [];

        for await (
          const [path, value] of iterator.prefixedBy(pathToTest)
        ) {
          assertEquals(concat(path), value);

          actualPrefixedBy.push(path);
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

        const actualPrefixesAfterRemoval: Path[] = [];

        for await (
          const [path, value] of iterator.prefixesOf(pathToTest)
        ) {
          assertEquals(concat(path), value);
          actualPrefixesAfterRemoval.push(path);
        }

        assertEquals(
          actualPrefixesAfterRemoval,
          Array.from(expectedPrefixesAfterRemoval),
        );

        // Use the iterator to get actual prefixes and prefixed by...
        const actualPrefixedByAfterRemoval = [];

        for await (
          const [path, value] of iterator.prefixedBy(pathSet[splitPoint])
        ) {
          assertEquals(concat(path), value);

          actualPrefixedByAfterRemoval.push(path);
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
