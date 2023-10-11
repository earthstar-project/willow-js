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
  ["i", 0],
  ["l", 0],
];

for (const [letter, level] of set) {
  await skiplist.insert(letter, new Uint8Array(), {
    layer: level,
  });

  /*
  console.group(letter);
  await skiplist.print();
  console.groupEnd();
	*/
}

await skiplist.print();

await skiplist.remove("i");

console.log("removed i");

const res = await skiplist.summarise("i", "i");

console.log(res);

await skiplist.print();
