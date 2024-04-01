import { MonoidRbTree } from "./monoid_rbtree.ts";
import { assertEquals } from "https://deno.land/std@0.202.0/assert/mod.ts";
import { shuffle } from "https://deno.land/x/proc@0.21.9/mod3.ts";
import { Skiplist } from "./monoid_skiplist.ts";

import { KvDriverDeno } from "../kv/kv_driver_deno.ts";
import { SummarisableStorage } from "./types.ts";
import { SimpleKv } from "./simple_kv.ts";
import { LiftingMonoid } from "./lifting_monoid.ts";

// The range, the fingerprint, size, collected items.
type RangeVector = [[string, string], string, number, string[]];

const rangeVectors: RangeVector[] = [
  [["a", "a"], "aAbBcCdDeEfFgG", 7, ["a", "b", "c", "d", "e", "f", "g"]],
  [["a", "d"], "aAbBcC", 3, ["a", "b", "c"]],
  [["g", "a"], "gG", 1, ["g"]],
  [["c", "a"], "cCdDeEfFgG", 5, ["c", "d", "e", "f", "g"]],
  [["c", "g"], "cCdDeEfF", 4, ["c", "d", "e", "f"]],
  [["e", "a"], "eEfFgG", 3, ["e", "f", "g"]],
  [["b", "b"], "aAbBcCdDeEfFgG", 7, ["a", "b", "c", "d", "e", "f", "g"]],
  [["c", "b"], "aAcCdDeEfFgG", 6, ["a", "c", "d", "e", "f", "g"]],
  [["e", "b"], "aAeEfFgG", 4, ["a", "e", "f", "g"]],
  [["m", "d"], "aAbBcC", 3, ["a", "b", "c"]],
  [["m", "z"], "", 0, []],
  [["f", "z"], "fFgG", 2, ["f", "g"]],
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

/** A monoid which lifts the member as a string, and combines by concatenating together. */
const concatMonoid: LiftingMonoid<string, string> = {
  lift: (key: string, value: Uint8Array) =>
    Promise.resolve(key + new TextDecoder().decode(value)),
  combine: (a: string, b: string) => {
    return a + b;
  },
  neutral: "",
};

const simpleKvScenario: SummarisableStorageScenario = {
  name: "SimpleKV",
  makeScenario: async () => {
    const kv = await Deno.openKv();
    const driver = new KvDriverDeno(kv);

    await driver.clear();

    const simpleKv = new SimpleKv(
      {
        monoid: concatMonoid,
        compare,
        kv: driver,
      },
    );

    return {
      storage: simpleKv,
      dispose: () => Promise.resolve(kv.close()),
    };
  },
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

const scenarios = [simpleKvScenario, skiplistScenario, rbtreeScenario];
const scenarioPairings = [
  [simpleKvScenario, rbtreeScenario],
  [skiplistScenario, rbtreeScenario],
];

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
          map.set(letter, encoder.encode(letter.toUpperCase()));
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
          await storage.insert(
            item,
            new TextEncoder().encode(item.toUpperCase()),
          );
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

function makeRandomLetters() {
  const arr: string[] = [];

  const threshold = 0.8;

  for (const letter of letters) {
    if (Math.random() > threshold) {
      arr.push(letter);
    }
  }

  if (arr.length === 0) {
    arr.push(
      letters[
        Math.floor(Math.random() * arr.length)
      ],
    );
  }

  shuffle(arr);

  return arr;
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

Deno.test.only("Insertion and summary", async (test) => {
  for (const scenario of scenarios) {
    await test.step(scenario.name, async () => {
      const letterArrays: string[][] = [];

      for (let i = 0; i < 3; i++) {
        letterArrays.push(makeRandomLetters());
      }

      for (const letterArr of letterArrays) {
        const { storage, dispose } = await scenario
          .makeScenario();

        let expectedFingerprint = concatMonoid.neutral;

        for (const item of letterArr.toSorted()) {
          expectedFingerprint = concatMonoid.combine(expectedFingerprint, item);
        }

        for (const item of letterArr) {
          await storage.insert(item, new Uint8Array());
        }

        const { fingerprint, size } = await storage.summarise("a", "a");

        console.log({
          fingerprint,
          expectedFingerprint,
          letterArr,
        });

        if (storage instanceof Skiplist) {
          await storage.print();
        }

        assertEquals(fingerprint, expectedFingerprint);
        assertEquals(size, letterArr.length);

        await dispose();
      }
    });
  }
});

Deno.test("Summarise and compare (random 100 sets x 100 ranges)", async (test) => {
  for (const [aScenario, bScenario] of scenarioPairings) {
    await test.step({
      name: `${aScenario.name} + ${bScenario.name}`,
      fn: async () => {
        const sets: string[][] = [];

        for (let i = 0; i < 100; i++) {
          sets.push(makeRandomLetters());
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

          // console.log({ toDelete });

          await aStorage.remove(toDelete);
          await bStorage.remove(toDelete);

          assertEquals(aItems, bItems);

          for (let i = 0; i < 100; i++) {
            const { start, end } = makeRandomRange(set);

            const aFingerprint = await aStorage.summarise(start, end);
            const bFingerprint = await bStorage.summarise(start, end);

            // console.log({ start, end });

            assertEquals(
              aFingerprint,
              bFingerprint,
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
