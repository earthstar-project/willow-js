import { KvDriverDeno } from "../src/store/storage/kv/kv_driver_deno.ts";
import { LiftingMonoid } from "../src/store/storage/summarisable_storage/lifting_monoid.ts";
import { Skiplist } from "../src/store/storage/summarisable_storage/monoid_skiplist.ts";

const concatMonoid: LiftingMonoid<string, string> = {
  lift: (key: string, value: Uint8Array) =>
    Promise.resolve(key + new TextDecoder().decode(value)),
  combine: (a: string, b: string) => {
    return a + b;
  },
  neutral: "",
};

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
  ["n", 1],
  ["p", 0],
  ["q", 0],
  ["w", 4],
];

for (const [letter, level] of set) {
  await skiplist.insert(letter, new Uint8Array([0, 0, 0, 0]), {
    layer: level,
  });
}

console.log(await skiplist.summarise("w", "q"));

await skiplist.print();
