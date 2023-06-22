import { assertEquals } from "https://deno.land/std@0.177.0/testing/asserts.ts";
import { KvDriverDeno } from "../kv/kv_driver_deno.ts";
import { KvRadixTree } from "./kv_radix_tree.ts";
import { GwilsPrefixTree } from "./my_big_idea.ts";
import { RadixTreeMemory } from "./mem_radix_tree.ts";

const driver = new KvDriverDeno(await Deno.openKv("gtree"));
const driver2 = new KvDriverDeno(await Deno.openKv("straightforward"));

await driver.clear();
await driver2.clear();

const gtree = new GwilsPrefixTree<string>(driver);
const stree = new KvRadixTree<string>(driver2);
const mtree = new RadixTreeMemory<string>();

const encoder = new TextEncoder();
const decoder = new TextDecoder();

const k = (number: number) => encoder.encode(`${number}`);
const d = (b: Uint8Array) => decoder.decode(b);

// Make an array of n items
const arrayRange = (start: number, stop: number, step: number) =>
  Array.from(
    { length: (stop - start) / step + 1 },
    (value, index) => start + index * step,
  );

// n = 100000
const wordList = arrayRange(0, 10000, 1);

for (const word of wordList) {
  await gtree.insert(k(word), `<${word}>`);
  await stree.insert(k(word), `<${word}>`);
  await mtree.insert(k(word), `<${word}>`);
}

await gtree.remove(k(500));
await stree.remove(k(500));
await mtree.remove(k(500));

const gtreePrefixes = [];
const streePrefixes = [];
const mtreePrefixes = [];

const gtreePrefixed = [];
const streePrefixed = [];
const mtreePrefixed = [];

for await (const [key, value] of gtree.prefixesOf(k(231347))) {
  gtreePrefixes.push(d(key));
}

for await (const [key, value] of stree.prefixesOf(k(231347))) {
  streePrefixes.push(d(key));
}

for await (const [key, value] of mtree.prefixesOf(k(231347))) {
  mtreePrefixes.push(d(key));
}

for await (const [key, value] of gtree.prefixedBy(k(23))) {
  gtreePrefixed.push(d(key));
}

for await (const [key, value] of stree.prefixedBy(k(23))) {
  streePrefixed.push(d(key));
}

for await (const [key, value] of mtree.prefixedBy(k(23))) {
  mtreePrefixed.push(d(key));
}

assertEquals(gtreePrefixes, streePrefixes);
assertEquals(streePrefixes, mtreePrefixes);

assertEquals(gtreePrefixed, streePrefixed);
assertEquals(streePrefixed, mtreePrefixed);

Deno.bench(
  "prefixes of (unnamed tree)",
  { group: "prefixes" },
  async () => {
    for await (const [key, value] of gtree.prefixesOf(k(2313))) {
      //
    }
  },
);

Deno.bench(
  "prefixes of (simple tree)",
  { group: "prefixes" },
  async () => {
    for await (const [key, value] of stree.prefixesOf(k(2313))) {
      //
    }
  },
);

Deno.bench(
  "prefixes of (mem tree)",
  { baseline: true, group: "prefixes" },
  async () => {
    for await (const [key, value] of mtree.prefixesOf(k(2313))) {
      //
    }
  },
);

Deno.bench(
  "prefixed by (unnamed tree)",
  { group: "prefixedBy" },
  async () => {
    for await (const [key, value] of gtree.prefixedBy(k(23))) {
      //
    }
  },
);

Deno.bench(
  "prefixed by (simple tree)",
  { group: "prefixedBy" },
  async () => {
    for await (const [key, value] of stree.prefixedBy(k(23))) {
      //
    }
  },
);

Deno.bench(
  "prefixed by (mem tree)",
  { baseline: true, group: "prefixedBy" },
  async () => {
    for await (const [key, value] of mtree.prefixedBy(k(23))) {
      //
    }
  },
);
