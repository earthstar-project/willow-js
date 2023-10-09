import { KvDriverDeno } from "../kv/kv_driver_deno.ts";
import { LiftingMonoid } from "./lifting_monoid.ts";
import { MonoidRbTree } from "./monoid_rbtree.ts";
import { Skiplist } from "./monoid_skiplist.ts";
import { SimpleKv } from "./simple_kv.ts";

const xormonoid: LiftingMonoid<number, number> = {
  combine: (a, b) => a ^ b,
  lift: (a) => Promise.resolve(a),
  neutral: 0,
};

const kv1 = await Deno.openKv("./simple");
const simpleDriver = new KvDriverDeno(kv1);

const kv2 = await Deno.openKv("./skip");
const skipDriver = new KvDriverDeno(kv2);

const compare = (a: number, b: number) => {
  if (a > b) {
    return 1;
  } else if (a < b) {
    return -1;
  } else {
    return 0;
  }
};

// Insert (1st)

Deno.bench(
  "Insert 2nd (MonoidRbTree)",
  { group: "Insert (2nd)", baseline: true },
  async (bench) => {
    const tree = new MonoidRbTree({ monoid: xormonoid, compare });

    await tree.insert(2, new Uint8Array());

    bench.start();

    await tree.insert(6, new Uint8Array());

    bench.end();
  },
);

/*
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
    const skiplist = new Skiplist(
      {
        monoid: xormonoid,
        compare,
        kv: skipDriver,
      },
    );

    await skiplist.insert(2, new Uint8Array());

    bench.start();

    await skiplist.insert(6, new Uint8Array());

    bench.end();

    await skipDriver.clear();
  },
);

// Insert (100th)

Deno.bench(
  "Insert 100th (MonoidRbTree)",
  { group: "Insert (100th)", baseline: true },
  async (bench) => {
    const tree = new MonoidRbTree({ monoid: xormonoid, compare });

    for (let i = 0; i < 100; i++) {
      await tree.insert(i, new Uint8Array());
    }

    bench.start();

    await tree.insert(1000, new Uint8Array());

    bench.end();
  },
);

/*
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
    const skiplist = new Skiplist(
      {
        monoid: xormonoid,
        compare,
        kv: skipDriver,
      },
    );

    for (let i = 0; i < 100; i++) {
      await skiplist.insert(i, new Uint8Array());
    }

    bench.start();

    await skiplist.insert(100, new Uint8Array());

    bench.end();

    await skipDriver.clear();
  },
);

// Insert (1000th)

Deno.bench(
  "Insert 1000th (MonoidRbTree)",
  { group: "Insert (1000th)", baseline: true },
  async (bench) => {
    const tree = new MonoidRbTree({ monoid: xormonoid, compare });

    for (let i = 0; i < 1000; i++) {
      await tree.insert(i, new Uint8Array());
    }

    bench.start();

    await tree.insert(1000, new Uint8Array());

    bench.end();
  },
);

/*
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
    const skiplist = new Skiplist(
      {
        monoid: xormonoid,
        compare,
        kv: skipDriver,
      },
    );

    for (let i = 0; i < 1000; i++) {
      await skiplist.insert(i, new Uint8Array());
    }

    bench.start();

    await skiplist.insert(1000, new Uint8Array());

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
      const lifted = await xormonoid.lift(arr[i]);
      fingerprint = xormonoid.combine(fingerprint, lifted);
    }

    bench.end();
  },
);

Deno.bench(
  "Summarise 60 (MonoidRbTree)",
  { group: "Summarise 20 - 80" },
  async (bench) => {
    const tree = new MonoidRbTree({ monoid: xormonoid, compare });

    for (let i = 0; i < 100; i++) {
      await tree.insert(i, new Uint8Array());
    }

    bench.start();

    await tree.summarise(20, 80);

    bench.end();
  },
);

/*
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
    const skiplist = new Skiplist(
      {
        monoid: xormonoid,
        compare,
        kv: skipDriver,
      },
    );

    for (let i = 0; i < 100; i++) {
      await skiplist.insert(i, new Uint8Array());
    }

    bench.start();

    await skiplist.summarise(20, 80);

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
      const lifted = await xormonoid.lift(arr[i]);
      fingerprint = xormonoid.combine(fingerprint, lifted);
    }

    bench.end();
  },
);

Deno.bench(
  "Summarise 160 (MonoidRbTree)",
  { group: "Summarise 20 - 180" },
  async (bench) => {
    const tree = new MonoidRbTree({ monoid: xormonoid, compare });

    for (let i = 0; i < 200; i++) {
      await tree.insert(i, new Uint8Array());
    }

    bench.start();

    await tree.summarise(20, 180);

    bench.end();
  },
);

/*
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
    const skiplist = new Skiplist(
      {
        monoid: xormonoid,
        compare,
        kv: skipDriver,
      },
    );

    for (let i = 0; i < 200; i++) {
      await skiplist.insert(i, new Uint8Array());
    }

    bench.start();

    await skiplist.summarise(20, 180);

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
      const lifted = await xormonoid.lift(arr[i]);
      fingerprint = xormonoid.combine(fingerprint, lifted);
    }

    bench.end();
  },
);

Deno.bench(
  "Summarise 1960 (MonoidRbTree)",
  { group: "Summarise 20 - 1980" },
  async (bench) => {
    const tree = new MonoidRbTree({ monoid: xormonoid, compare });

    for (let i = 0; i < 2000; i++) {
      await tree.insert(i, new Uint8Array());
    }

    bench.start();

    await tree.summarise(20, 1980);

    bench.end();
  },
);

/*
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
    const skiplist = new Skiplist(
      {
        monoid: xormonoid,
        compare,
        kv: skipDriver,
      },
    );

    for (let i = 0; i < 2000; i++) {
      await skiplist.insert(i, new Uint8Array());
    }

    bench.start();

    await skiplist.summarise(20, 1980);

    bench.end();

    await skipDriver.clear();
  },
);
