import { MonoidRbTree } from "./monoid_rbtree.ts";
import { assertEquals } from "$std/testing/asserts.ts";
import { Skiplist } from "./monoid_skiplist.ts";
import { concatMonoid } from "./lifting_monoid.ts";
import { KvDriverDeno } from "../kv/kv_driver_deno.ts";
import { SummarisableStorage } from "./types.ts";

// The range, the fingerprint, size, collected items.
type RangeVector = [[string, string], string, number, string[]];

const rangeVectors: RangeVector[] = [
  [["a", "a"], "abcdefg", 7, ["a", "b", "c", "d", "e", "f", "g"]],
  [["a", "d"], "abc", 3, ["a", "b", "c"]],
  [["g", "a"], "g", 1, ["g"]],
  [["c", "a"], "cdefg", 5, ["c", "d", "e", "f", "g"]],
  [["c", "g"], "cdef", 4, ["c", "d", "e", "f"]],
  [["e", "a"], "efg", 3, ["e", "f", "g"]],
  [["b", "b"], "abcdefg", 7, ["a", "b", "c", "d", "e", "f", "g"]],
  [["c", "b"], "acdefg", 6, ["a", "c", "d", "e", "f", "g"]],
  [["e", "b"], "aefg", 4, ["a", "e", "f", "g"]],
  [["m", "d"], "abc", 3, ["a", "b", "c"]],
  [["m", "z"], "", 0, []],
  [["f", "z"], "fg", 2, ["f", "g"]],
];

const compare = (a: string, b: string) => {
  if (a > b) {
    return 1;
  } else if (a < b) {
    return -1;
  } else {
    return 0;
  }
};

type SummarisableStorageScenario = {
  name: string;
  makeScenario: () => Promise<
    {
      storage: SummarisableStorage<string, string>;
      dispose: () => Promise<void>;
    }
  >;
};

const skiplistScenario: SummarisableStorageScenario = {
  name: "Skiplist",
  makeScenario: async () => {
    const kv = await Deno.openKv();
    const driver = new KvDriverDeno(kv);

    await driver.clear();

    const skiplist = new Skiplist(
      {
        monoid: concatMonoid,
        compare,
        kv: driver,
      },
    );

    return {
      storage: skiplist,
      dispose: () => Promise.resolve(kv.close()),
    };
  },
};

const rbtreeScenario: SummarisableStorageScenario = {
  name: "RBTree",
  makeScenario: () => {
    const tree = new MonoidRbTree({ monoid: concatMonoid, compare });

    return Promise.resolve({
      storage: tree,
      dispose: () => Promise.resolve(),
    });
  },
};

const scenarios = [skiplistScenario, rbtreeScenario];
const scenarioPairings = [[skiplistScenario, rbtreeScenario]];

Deno.test("Storage", async (test) => {
  for (const scenario of scenarios) {
    await test.step({
      name: scenario.name,
      fn: async () => {
        const { storage, dispose } = await scenario.makeScenario();

        const encoder = new TextEncoder();

        const keys = ["a", "b", "c", "d", "e", "f", "g"];

        const map = new Map();

        for (const letter of keys) {
          map.set(letter, encoder.encode(letter));
        }

        for (const [key, value] of map) {
          await storage.insert(key, value);
        }

        const listContents = [];

        for await (const item of storage.allEntries()) {
          listContents.push(item.value);
        }

        assertEquals(Array.from(map.values()), listContents);

        for (const [key, value] of map) {
          const storedValue = await storage.get(key);

          assertEquals(storedValue, value);
        }

        await dispose();
      },
    });
  }
});

Deno.test("Summarise (basics)", async (test) => {
  for (const scenario of scenarios) {
    await test.step({
      name: scenario.name,
      fn: async () => {
        const { storage, dispose } = await scenario.makeScenario();

        const set = ["a", "b", "c", "d", "e", "f", "g"];

        for (const item of set) {
          await storage.insert(item, new Uint8Array());
        }

        for (const vector of rangeVectors) {
          const items = [];

          for await (
            const entry of storage.entries(vector[0][0], vector[0][1])
          ) {
            items.push(entry.key);
          }

          assertEquals(
            items,
            vector[3],
          );

          const { fingerprint, size } = await storage.summarise(
            vector[0][0],
            vector[0][1],
          );

          assertEquals(
            [fingerprint, size],
            [vector[1], vector[2]],
          );
        }

        await dispose();
      },
    });
  }
});

const letters = [
  "a",
  "b",
  "c",
  "d",
  "e",
  "f",
  "g",
  "h",
  "i",
  "j",
  "k",
  "l",
  "m",
  "n",
  "o",
  "p",
  "q",
  "r",
  "s",
  "t",
  "u",
  "v",
  "w",
  "x",
  "y",
  "z",
];

function makeRandomSet() {
  const newSet: string[] = [];

  const threshold = Math.random();

  for (const letter of letters) {
    if (Math.random() > threshold) {
      newSet.push(letter);
    }
  }

  if (newSet.length === 0) {
    newSet.push(
      letters[
        Math.floor(Math.random() * letters.length)
      ],
    );
  }

  return newSet;
}

function makeRandomRange(set: string[]) {
  const startIndex = Math.floor(Math.random() * set.length);
  const endIndex = Math.floor(Math.random() * set.length);

  return { start: set[startIndex], end: set[endIndex] };
}

function makeRandomItemsQuery(set: string[]) {
  const startIndex = Math.random() > 0.1
    ? Math.floor(Math.random() * set.length)
    : undefined;
  const endIndex = Math.random() > 0.1
    ? Math.floor(Math.random() * set.length)
    : undefined;

  return {
    start: startIndex ? set[startIndex] : undefined,
    end: endIndex ? set[endIndex] : undefined,
    reverse: Math.random() > 0.5 ? true : false,
    limit: Math.random() > 0.5
      ? Math.floor(Math.random() * (set.length - 1 + 1) + 1)
      : undefined,
  };
}

Deno.test("Summarise and compare (random 100 sets x 100 ranges)", async (test) => {
  for (const [aScenario, bScenario] of scenarioPairings) {
    await test.step({
      name: `${aScenario.name} + ${bScenario.name}`,
      fn: async () => {
        const sets: string[][] = [];

        for (let i = 0; i < 100; i++) {
          sets.push(makeRandomSet());
        }

        for (const set of sets) {
          const { storage: aStorage, dispose: aDispose } = await aScenario
            .makeScenario();
          const { storage: bStorage, dispose: bDispose } = await bScenario
            .makeScenario();

          for (const item of set) {
            await aStorage.insert(item, new Uint8Array());
            await bStorage.insert(item, new Uint8Array());
          }

          // Randomly delete an element.

          const toDelete = set[Math.floor(Math.random() * set.length)];

          const aItems = [];
          const bItems = [];

          for await (const aValue of aStorage.allEntries()) {
            aItems.push(aValue.key);
          }

          for await (const bValue of bStorage.allEntries()) {
            bItems.push(bValue.key);
          }

          await aStorage.remove(toDelete);
          await bStorage.remove(toDelete);

          assertEquals(aItems, bItems);

          for (let i = 0; i < 100; i++) {
            const { start, end } = makeRandomRange(set);

            const aFingerprint = await aStorage.summarise(start, end);
            const bFingerprint = await bStorage.summarise(start, end);

            assertEquals(
              bFingerprint,
              aFingerprint,
            );

            const aItems = [];

            for await (const entry of aStorage.entries(start, end)) {
              aItems.push(entry.key);
            }

            const bItems = [];

            for await (const entry of bStorage.entries(start, end)) {
              bItems.push(entry.key);
            }

            assertEquals(
              aItems,
              bItems,
            );

            const randomQuery = makeRandomItemsQuery(set);

            const aQueryItems = [];

            for await (
              const entry of aStorage.entries(
                randomQuery.start,
                randomQuery.end,
                {
                  limit: randomQuery.limit,
                  reverse: randomQuery.reverse,
                },
              )
            ) {
              aQueryItems.push(entry.key);
            }

            const bQueryItems = [];

            for await (
              const entry of bStorage.entries(
                randomQuery.start,
                randomQuery.end,
                {
                  limit: randomQuery.limit,
                  reverse: randomQuery.reverse,
                },
              )
            ) {
              bQueryItems.push(entry.key);
            }

            assertEquals(
              aQueryItems,
              bQueryItems,
            );
          }

          await Promise.all([
            aDispose(),
            bDispose(),
          ]);
        }
      },
    });
  }
});
