import {
  FingerprintTree,
} from "https://deno.land/x/range_reconcile@1.0.2/mod.ts";
import { assertEquals } from "https://deno.land/std@0.158.0/testing/asserts.ts";
import { Skiplist } from "./monoid_skiplist.ts";
import { concatMonoid } from "./lifting_monoid.ts";

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

Deno.test("Skiplist summarise (basics)", async () => {
  const kv = await Deno.openKv();

  for await (const result of kv.list({ start: [-1], end: [100] })) {
    await kv.delete(result.key);
  }

  const skiplist = new Skiplist(
    {
      monoid: concatMonoid,
      compare,
      kv,
    },
  );

  const set = ["a", "b", "c", "d", "e", "f", "g"];

  for (const item of set) {
    await skiplist.insert(item);
  }

  const listContents = [];

  for await (const item of skiplist.lnrValues()) {
    listContents.push(item);
  }

  //assertEquals(set, listContents);

  for (const vector of rangeVectors) {
    const { fingerprint, size } = await skiplist.summarise(
      vector[0][0],
      vector[0][1],
    );

    assertEquals(
      [fingerprint, size],
      [vector[1], vector[2]],
    );
  }

  kv.close();
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

  return newSet;
}

function makeRandomRange(set: string[]) {
  const startIndex = Math.floor(Math.random() * (set.length - 0) + set.length);
  const endIndex = Math.floor(Math.random() * (set.length - 0) + set.length);

  return { start: set[startIndex], end: set[endIndex] };
}

Deno.test("Skiplist summarise (fuzz 10k)", async () => {
  const sets: string[][] = [];

  for (let i = 0; i < 100; i++) {
    sets.push(makeRandomSet());
  }

  for (const set of sets) {
    const tree = new FingerprintTree(concatMonoid, compare);

    const kv = await Deno.openKv();

    for await (const result of kv.list({ start: [0], end: [100] })) {
      await kv.delete(result.key);
    }

    const skiplist = new Skiplist(
      {
        monoid: concatMonoid,
        compare,
        kv,
      },
    );

    for (const item of set) {
      tree.insert(item);
      await skiplist.insert(item);
    }

    for (let i = 0; i < 100; i++) {
      const { start, end } = makeRandomRange(set);

      const treeFingerprint = tree.getFingerprint(start, end);
      const listFingeprint = await skiplist.summarise(start, end);

      assertEquals(
        {
          fingerprint: treeFingerprint.fingerprint,
          size: treeFingerprint.size,
        },
        listFingeprint,
      );
    }

    kv.close();
  }
});
