// deno bench -A --unstable-kv ./src/store/storage/summarisable_storage/summarisable_storage.bench.ts

import { KvDriverDeno } from "../kv/kv_driver_deno.ts";
import type { LiftingMonoid } from "./lifting_monoid.ts";
import { Skiplist } from "./monoid_skiplist.ts";

const xormonoid: LiftingMonoid<[[number], Uint8Array], number> = {
  combine: (a, b) => a ^ b,
  lift: (a) => Promise.resolve(a[0][0]),
  neutral: 0,
};

const kv2 = await Deno.openKv("./skip");
const skipDriver = new KvDriverDeno(kv2);

// Insert (1st)

const byte = new Uint8Array([37]);

/*
Deno.bench(
  "Insert 2nd (MonoidRbTree)",
  { group: "Insert (2nd)", baseline: true },
  async (bench) => {
    const tree = new MonoidRbTree({ monoid: xormonoid, compare });

    await tree.insert(2, byte);

    bench.start();

    await tree.insert(6, byte);

    bench.end();
  },
);


Deno.bench(
  "Insert 2nd (Dummy KV)",
  { group: "Insert (2nd)" },
  async (bench) => {
    const simple = new SimpleKv(
      {
        monoid: xormonoid,
        compare,
        kv: simpleDriver,
      },
    );

    await simple.insert(2, new Uint8Array());

    bench.start();

    await simple.insert(6, new Uint8Array());

    bench.end();

    await simpleDriver.clear();
  },
);
*/

Deno.bench(
  "Insert 2nd (Monoid Skiplist)",
  { group: "Insert (2nd)" },
  async (bench) => {
    const skiplist = new Skiplist<[number], Uint8Array, number>(
      {
        monoid: xormonoid,
        logicalValueEq: (arr1, arr2) => {
          if (arr1.length !== arr2.length) {
            return false;
          }

          return arr1.every((value, index) => value === arr2[index]);
        },
        kv: skipDriver,
      },
    );

    await skiplist.insert([2], byte);

    bench.start();

    await skiplist.insert([6], byte);

    bench.end();

    await skipDriver.clear();
  },
);

// Insert (100th)

/*
Deno.bench(
  "Insert 100th (MonoidRbTree)",
  { group: "Insert (100th)", baseline: true },
  async (bench) => {
    const tree = new MonoidRbTree({ monoid: xormonoid, compare });

    for (let i = 0; i < 100; i++) {
      await tree.insert(i, byte);
    }

    bench.start();

    await tree.insert(1000, byte);

    bench.end();
  },
);


Deno.bench(
  "Insert 100th (Dummy KV)",
  { group: "Insert (100th)" },
  async (bench) => {
    const simple = new SimpleKv(
      {
        monoid: xormonoid,
        compare,
        kv: simpleDriver,
      },
    );

    for (let i = 0; i < 100; i++) {
      await simple.insert(i, new Uint8Array());
    }

    bench.start();

    await simple.insert(100, new Uint8Array());

    bench.end();

    await simpleDriver.clear();
  },
);
*/

Deno.bench(
  "Insert 100th (Monoid Skiplist)",
  { group: "Insert (100th)" },
  async (bench) => {
    const skiplist = new Skiplist<[number], Uint8Array, number>(
      {
        monoid: xormonoid,
        logicalValueEq: (arr1, arr2) => {
          if (arr1.length !== arr2.length) {
            return false;
          }

          return arr1.every((value, index) => value === arr2[index]);
        },
        kv: skipDriver,
      },
    );

    for (let i = 0; i < 100; i++) {
      await skiplist.insert([i], byte);
    }

    bench.start();

    await skiplist.insert([100], byte);

    bench.end();

    await skipDriver.clear();
  },
);

// Insert (1000th)

/*
Deno.bench(
  "Insert 1000th (MonoidRbTree)",
  { group: "Insert (1000th)", baseline: true },
  async (bench) => {
    const tree = new MonoidRbTree({ monoid: xormonoid, compare });

    for (let i = 0; i < 1000; i++) {
      await tree.insert(i, byte);
    }

    bench.start();

    await tree.insert(1000, byte);

    bench.end();
  },
);


Deno.bench(
  "Insert 1000th (Dummy KV)",
  { group: "Insert (1000th)" },
  async (bench) => {
    const simple = new SimpleKv(
      {
        monoid: xormonoid,
        compare,
        kv: simpleDriver,
      },
    );

    for (let i = 0; i < 1000; i++) {
      await simple.insert(i, new Uint8Array());
    }

    bench.start();

    await simple.insert(1000, new Uint8Array());

    bench.end();

    await simpleDriver.clear();
  },
);
*/

Deno.bench(
  "Insert 1000th (Monoid Skiplist)",
  { group: "Insert (1000th)" },
  async (bench) => {
    const skiplist = new Skiplist<[number], Uint8Array, number>(
      {
        monoid: xormonoid,
        logicalValueEq: (arr1, arr2) => {
          if (arr1.length !== arr2.length) {
            return false;
          }

          return arr1.every((value, index) => value === arr2[index]);
        },
        kv: skipDriver,
      },
    );

    for (let i = 0; i < 1000; i++) {
      await skiplist.insert([i], byte);
    }

    bench.start();

    await skiplist.insert([1000], byte);

    bench.end();

    await skipDriver.clear();
  },
);

// Summarise

Deno.bench(
  "Summarise 60 (Iterate array)",
  { group: "Summarise 20 - 80", baseline: true },
  async (bench) => {
    const arr = [];

    for (let i = 0; i < 100; i++) {
      arr.push(i);
    }

    bench.start();

    let fingerprint = xormonoid.neutral;

    for (let i = 20; i < 80; i++) {
      const lifted = await xormonoid.lift([[arr[i]], byte]);
      fingerprint = xormonoid.combine(fingerprint, lifted);
    }

    bench.end();
  },
);

/*
Deno.bench(
  "Summarise 60 (MonoidRbTree)",
  { group: "Summarise 20 - 80" },
  async (bench) => {
    const tree = new MonoidRbTree({ monoid: xormonoid, compare });

    for (let i = 0; i < 100; i++) {
      await tree.insert(i, byte);
    }

    bench.start();

    await tree.summarise(20, 80);

    bench.end();
  },
);


Deno.bench(
  "Summarise 60 (Dummy KV)",
  { group: "Summarise 20 - 80" },
  async (bench) => {
    const simple = new SimpleKv(
      {
        monoid: xormonoid,
        compare,
        kv: simpleDriver,
      },
    );

    for (let i = 0; i < 100; i++) {
      await simple.insert(i, new Uint8Array());
    }

    bench.start();

    await simple.summarise(20, 80);

    bench.end();

    await simpleDriver.clear();
  },
);
*/

Deno.bench(
  "Summarise 60 (Skiplist)",
  { group: "Summarise 20 - 80" },
  async (bench) => {
    const skiplist = new Skiplist<[number], Uint8Array, number>(
      {
        monoid: xormonoid,
        logicalValueEq: (arr1, arr2) => {
          if (arr1.length !== arr2.length) {
            return false;
          }

          return arr1.every((value, index) => value === arr2[index]);
        },
        kv: skipDriver,
      },
    );

    for (let i = 0; i < 100; i++) {
      await skiplist.insert([i], byte);
    }

    bench.start();

    await skiplist.summarise([20], [80]);

    bench.end();

    await skipDriver.clear();
  },
);

// 160

Deno.bench(
  "Summarise 160 (Iterate array)",
  { group: "Summarise 20 - 180", baseline: true },
  async (bench) => {
    const arr = [];

    for (let i = 0; i < 200; i++) {
      arr.push(i);
    }

    bench.start();

    let fingerprint = xormonoid.neutral;

    for (let i = 20; i < 180; i++) {
      const lifted = await xormonoid.lift([[arr[i]], byte]);
      fingerprint = xormonoid.combine(fingerprint, lifted);
    }

    bench.end();
  },
);

/*
Deno.bench(
  "Summarise 160 (MonoidRbTree)",
  { group: "Summarise 20 - 180" },
  async (bench) => {
    const tree = new MonoidRbTree({ monoid: xormonoid, compare });

    for (let i = 0; i < 200; i++) {
      await tree.insert(i, byte);
    }

    bench.start();

    await tree.summarise(20, 180);

    bench.end();
  },
);


Deno.bench(
  "Summarise 160 (Dummy KV)",
  { group: "Summarise 20 - 180" },
  async (bench) => {
    const simple = new SimpleKv(
      {
        monoid: xormonoid,
        compare,
        kv: simpleDriver,
      },
    );

    for (let i = 0; i < 200; i++) {
      await simple.insert(i, new Uint8Array());
    }

    bench.start();

    await simple.summarise(20, 180);

    bench.end();

    await simpleDriver.clear();
  },
);
*/

Deno.bench(
  "Summarise 160 (Skiplist)",
  { group: "Summarise 20 - 180" },
  async (bench) => {
    const skiplist = new Skiplist<[number], Uint8Array, number>(
      {
        monoid: xormonoid,
        logicalValueEq: (arr1, arr2) => {
          if (arr1.length !== arr2.length) {
            return false;
          }

          return arr1.every((value, index) => value === arr2[index]);
        },
        kv: skipDriver,
      },
    );

    for (let i = 0; i < 200; i++) {
      await skiplist.insert([i], byte);
    }

    bench.start();

    await skiplist.summarise([20], [180]);

    bench.end();

    await skipDriver.clear();
  },
);

// Lots

Deno.bench(
  "Summarise 1960 (Iterate array)",
  { group: "Summarise 20 - 1980", baseline: true },
  async (bench) => {
    const arr = [];

    for (let i = 0; i < 2000; i++) {
      arr.push(i);
    }

    bench.start();

    let fingerprint = xormonoid.neutral;

    for (let i = 20; i < 1980; i++) {
      const lifted = await xormonoid.lift([[arr[i]], byte]);
      fingerprint = xormonoid.combine(fingerprint, lifted);
    }

    bench.end();
  },
);

/*
Deno.bench(
  "Summarise 1960 (MonoidRbTree)",
  { group: "Summarise 20 - 1980" },
  async (bench) => {
    const tree = new MonoidRbTree({ monoid: xormonoid, compare });

    for (let i = 0; i < 2000; i++) {
      await tree.insert(i, byte);
    }

    bench.start();

    await tree.summarise(20, 1980);

    bench.end();
  },
);


Deno.bench(
  "Summarise 1960 (Dummy KV)",
  { group: "Summarise 20 - 1980" },
  async (bench) => {
    const simple = new SimpleKv(
      {
        monoid: xormonoid,
        compare,
        kv: simpleDriver,
      },
    );

    for (let i = 0; i < 2000; i++) {
      await simple.insert(i, new Uint8Array());
    }

    bench.start();

    await simple.summarise(20, 1980);

    bench.end();

    await simpleDriver.clear();
  },
);
*/

Deno.bench(
  "Summarise 1960 (Skiplist)",
  { group: "Summarise 20 - 1980" },
  async (bench) => {
    const skiplist = new Skiplist<[number], Uint8Array, number>(
      {
        monoid: xormonoid,
        logicalValueEq: (arr1, arr2) => {
          if (arr1.length !== arr2.length) {
            return false;
          }

          return arr1.every((value, index) => value === arr2[index]);
        },
        kv: skipDriver,
      },
    );

    for (let i = 0; i < 2000; i++) {
      await skiplist.insert([i], byte);
    }

    bench.start();

    await skiplist.summarise([20], [1980]);

    bench.end();

    await skipDriver.clear();
  },
);
