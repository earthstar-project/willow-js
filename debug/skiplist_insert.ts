import { KvDriverDeno } from "../src/replica/storage/kv/kv_driver_deno.ts";
import { concatMonoid } from "../src/replica/storage/summarisable_storage/lifting_monoid.ts";
import { Skiplist } from "../src/replica/storage/summarisable_storage/monoid_skiplist.ts";

const compare = (a: string, b: string) => {
  if (a > b) {
    return 1;
  } else if (a < b) {
    return -1;
  } else {
    return 0;
  }
};

const kv = await Deno.openKv();
const driver = new KvDriverDeno(kv);

await driver.clear();

const skiplist = new Skiplist(
  {
    compare,
    monoid: concatMonoid,
    kv: driver,
  },
);

const set: [string, number][] = [
  ["b", 2],
  ["d", 1],
  ["f", 2],
  ["k", 1],
  ["p", 1],
  ["t", 0],
  ["u", 0],
  ["w", 1],
  ["y", 0],
];

for (const [letter, level] of set) {
  await skiplist.insert(letter, new Uint8Array([0, 0, 0, 0]), {
    layer: level,
  });
}

console.log(await skiplist.summarise("t", "p"));

await skiplist.print();
