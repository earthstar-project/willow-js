import { KvDriverDeno } from "../kv/kv_driver_deno.ts";
import { KvRadixTree } from "./kv_radix_tree.ts";
import { RadixTreeMemory } from "./mem_radix_tree.ts";
import { GwilsPrefixTree } from "./my_big_idea.ts";

const driver = new KvDriverDeno(await Deno.openKv("gtree"));
const driver2 = new KvDriverDeno(await Deno.openKv("straightforward"));

await driver.clear();
await driver2.clear();

const gtree = new GwilsPrefixTree<string>(driver);
const stree = new KvRadixTree<string>(driver2);
const mtree = new RadixTreeMemory<string>();

const encoder = new TextEncoder();
const decoder = new TextDecoder();

const k = (str: string) => encoder.encode(str);
const d = (b: Uint8Array) => decoder.decode(b);

const wordList = [
  "nib",
  "nat",
  "nature",
  "natural",
  "naturalist",
  "naturalistic",
];

for (const word of wordList) {
  await gtree.insert(k(word), `<${word}>`);
  await stree.insert(k(word), `<${word}>`);
  await mtree.insert(k(word), `<${word}>`);

  mtree.print();
  console.log("---");
}

await gtree.remove(k("natty"));
await stree.remove(k("natty"));
await mtree.remove(k("natty"));

console.group("Tree contents");

console.groupEnd();

console.group("Prefixes of naturalistic");

for await (const [key, value] of gtree.prefixesOf(k("naturalistic"))) {
  console.log(d(key), "-", value);
}

console.log("---");

for await (const [key, value] of stree.prefixesOf(k("naturalistic"))) {
  console.log(d(key), "-", value);
}

console.log("---");

for await (const [key, value] of mtree.prefixesOf(k("naturalistic"))) {
  console.log(d(key), "-", value);
}

console.groupEnd();

console.group("Items prefixed by natural");

for await (const [key, value] of gtree.prefixedBy(k("natural"))) {
  console.log(d(key), "-", value);
}

console.log("---");

for await (const [key, value] of stree.prefixedBy(k("natural"))) {
  console.log(d(key), "-", value);
}

console.log("---");

for await (const [key, value] of mtree.prefixedBy(k("natural"))) {
  console.log(d(key), "-", value);
}

console.groupEnd();

Deno.bench(
  "prefixes of (unnamed tree)",
  { baseline: true, group: "prefixes" },
  async () => {
    for await (const [key, value] of gtree.prefixesOf(k("naturalistic"))) {
      //
    }
  },
);

Deno.bench(
  "prefixes of (simple tree)",
  { baseline: true, group: "prefixes" },
  async () => {
    for await (const [key, value] of stree.prefixesOf(k("naturalistic"))) {
      //
    }
  },
);

Deno.bench(
  "prefixes of (memory tree)",
  { baseline: true, group: "prefixes" },
  async () => {
    for await (const [key, value] of mtree.prefixesOf(k("naturalistic"))) {
      //
    }
  },
);

Deno.bench(
  "prefixed by (unnamed tree)",
  { baseline: true, group: "prefixedBy" },
  async () => {
    for await (const [key, value] of gtree.prefixedBy(k("natural"))) {
      //
    }
  },
);

Deno.bench(
  "prefixed by (simple tree)",
  { baseline: true, group: "prefixedBy" },
  async () => {
    for await (const [key, value] of stree.prefixedBy(k("natural"))) {
      //
    }
  },
);

Deno.bench(
  "prefixed by (memory tree)",
  { baseline: true, group: "prefixedBy" },
  async () => {
    for await (const [key, value] of mtree.prefixedBy(k("natural"))) {
      //
    }
  },
);
