import { assertEquals } from "https://deno.land/std@0.202.0/assert/mod.ts";
import { PhysicalKey, Skiplist } from "./monoid_skiplist.ts";

import { LiftingMonoid } from "./lifting_monoid.ts";
import { KvDriverInMemory } from "../kv/kv_driver_in_memory.ts";
import { KvDriverDeno } from "../kv/kv_driver_deno.ts";
import { KvDriver } from "../kv/types.ts";
import { PhysicalValue } from "./monoid_skiplist.ts";
import { LinearStorage } from "./linear_summarisable_storage.ts";

type Operation<Key, Value> =
  | Insert<Key, Value>
  | Delete<Key>;

type Insert<Key, Value> = {
  key: Key;
  value: Value;
  level: number;
};

type Delete<Key> = { key: Key };

type Summarise<Key> = {
  start?: Key;
  end?: Key;
};

const testMonoid: LiftingMonoid<[[number], number], string> = {
  lift: (x: [[number], number]) => Promise.resolve(`${x[0][0]}_${x[1]};`),
  combine: (a: string, b: string) => {
    return `${a}${b}`;
  },
  neutral: "",
};

function compareNumberArrays(a: number[], b: number[]): number {
  for (let i = 0; i < Math.min(a.length, b.length); i++) {
    if (a[i] < b[i]) {
      return -1;
    } else if (a[i] > b[i]) {
      return 1;
    }
  }

  if (a.length < b.length) {
    return -1;
  } else if (a.length > b.length) {
    return 1;
  } else {
    return 0;
  }
}

async function newTestStore(
  useDenoKv?: boolean,
): Promise<Skiplist<[number], number, string>> {
  const keyCompare = (a: [number], b: [number]) => {
    if (a[0] < b[0]) {
      return -1;
    } else if (a[0] > b[0]) {
      return 1;
    } else {
      return 0;
    }
  };

  let kv: KvDriver<
    PhysicalKey<[number]>,
    PhysicalValue<number, string>
  > = new KvDriverInMemory<
    PhysicalKey<[number]>,
    PhysicalValue<number, string>
  >(
    compareNumberArrays,
  );

  if (useDenoKv) {
    kv = new KvDriverDeno<
      PhysicalKey<[number]>,
      PhysicalValue<number, string>
    >(await Deno.openKv(":memory:"));
    await kv.clear();
  }

  return new Skiplist<[number], number, string>({
    logicalKeyCompare: keyCompare,
    logicalValueEq: (a: number, b: number) => a === b,
    kv,
    monoid: testMonoid,
  });
}

async function runTestCase(
  ops: Operation<number, number>[],
  summaries: Summarise<number>[],
  useDenoKv?: boolean,
) {
  const store = await newTestStore(useDenoKv);

  const control = new LinearStorage<[number], number, string>({
    kv: new KvDriverInMemory<[number], number>(
      compareNumberArrays,
    ),
    monoid: testMonoid,
  });

  for (const op of ops) {
    if ("value" in op) {
      await store.insert([op.key], op.value, { layer: op.level });
      await control.insert([op.key], op.value);
    } else {
      await store.remove([op.key]);
      await control.remove([op.key]);
    }
  }

  for (const summarise of summaries) {
    const got = await store.summarise(
      summarise.start === undefined ? undefined : [summarise.start],
      summarise.end === undefined ? undefined : [summarise.end],
    );
    const expected = await control.summarise(
      summarise.start === undefined ? undefined : [summarise.start],
      summarise.end === undefined ? undefined : [summarise.end],
    );

    try {
      assertEquals(got, expected);
    } catch (_err) {
      await store.print();

      assertEquals(
        got,
        expected,
        `
    ===================
    Summary: ${JSON.stringify(summarise, undefined, 2)}
    
    Operations: ${JSON.stringify(ops, undefined, 2)}`,
      );
    }
  }

  if (useDenoKv) {
    (<KvDriverDeno<[number], number>><unknown>store["kv"]).close();
  }
}

Deno.test({
  name: "Basic stuff actually works",
  async fn() {
    const store = await newTestStore();
    await store.insert([0], 8);
    assertEquals(await collect(store.allEntries()), [{ key: [0], value: 8 }]);
  },
});

Deno.test({
  name: "Regression 2",
  async fn() {
    const store = await newTestStore();
    await store.insert([0], 8, { layer: 0 });
    await store.insert([1], 9, { layer: 1 });

    const got = await collect(store.allEntries());
    const expected = [{ key: [0], value: 8 }, { key: [1], value: 9 }];

    try {
      assertEquals(got, expected);
    } catch (_err) {
      await store.print();
      console.log(got);
      console.log(expected);
      assertEquals(got, expected);
    }
  },
});

Deno.test({
  name: "Regression 3",
  async fn() {
    const store = await newTestStore();
    await store.insert([0], 8, { layer: 0 });
    await store.insert([0], 9, { layer: 0 });

    const got = await store.summarise();
    const expected = {
      fingerprint: "0_9;",
      size: 1,
    };

    try {
      assertEquals(got, expected);
    } catch (_err) {
      await store.print();
      console.log(got);
      console.log(expected);
      assertEquals(got, expected);
    }
  },
});

Deno.test({
  name: "Regression 4",
  async fn() {
    const store = await newTestStore();
    await store.insert([0], 8, { layer: 0 });
    await store.insert([0], 9, { layer: 1 });

    const got = await store.summarise();
    const expected = {
      fingerprint: "0_9;",
      size: 1,
    };

    try {
      assertEquals(got, expected);
    } catch (_err) {
      await store.print();
      console.log(got);
      console.log(expected);
      assertEquals(got, expected);
    }
  },
});

Deno.test({
  name: "Regression 5",
  async fn() {
    const store = await newTestStore();
    await store.insert([0], 8, { layer: 0 });
    await store.remove([0]);

    const got = await collect(store.allEntries());
    const expected: { key: [number]; value: number }[] = [];

    try {
      assertEquals(got, expected);
    } catch (_err) {
      await store.print();
      console.log(got);
      console.log(expected);
      assertEquals(got, expected);
    }
  },
});

Deno.test({
  name: "Regression 6",
  async fn() {
    const store = await newTestStore();
    await store.insert([0], 8, { layer: 1 });
    await store.insert([1], 8, { layer: 0 });
    await store.insert([0], 9, { layer: 0 });

    const got = await store.summarise();
    const expected = {
      fingerprint: "0_9;1_8;",
      size: 2,
    };

    try {
      assertEquals(got, expected);
    } catch (_err) {
      await store.print();
      console.log(got);
      console.log(expected);
      assertEquals(got, expected);
    }
  },
});

Deno.test({
  name: "Regression 7",
  async fn() {
    const store = await newTestStore();
    await store.insert([0], 8, { layer: 1 });
    await store.insert([1], 8, { layer: 0 });
    await store.insert([2], 8, { layer: 0 });

    const got = await store.summarise();
    const expected = {
      fingerprint: "0_8;1_8;2_8;",
      size: 3,
    };

    try {
      assertEquals(got, expected);
    } catch (_err) {
      await store.print();
      console.log(got);
      console.log(expected);
      assertEquals(got, expected);
    }
  },
});

Deno.test({
  name: "Regression 8",
  async fn() {
    const store = await newTestStore();
    await store.insert([0], 8, { layer: 1 });
    await store.insert([1], 8, { layer: 0 });
    await store.insert([2], 8, { layer: 0 });
    await store.remove([1]);

    const got = await store.summarise();
    const expected = {
      fingerprint: "0_8;2_8;",
      size: 2,
    };

    try {
      assertEquals(got, expected);
    } catch (_err) {
      await store.print();
      console.log(got);
      console.log(expected);
      assertEquals(got, expected);
    }
  },
});

Deno.test({
  name: "Random Tests",
  async fn() {
    const numKeys = 8;
    const numLayers = 5;

    const iterations = 10000;

    for (let i = 0; i < iterations; i++) {
      for (let numOps = 5; numOps < 16; numOps++) {
        const ops: Operation<number, number>[] = [];
        for (let opNr = 0; opNr < numOps; opNr++) {
          ops.push(randomOperation(numKeys, numLayers));
        }

        const ranges = collectSync(exhaustivelyGenerateRanges(numKeys));
        await runTestCase(ops, ranges);
      }
    }
  },
});

Deno.test({
  name: "Even More Random Tests",
  async fn() {
    const numKeys = 16;
    const numLayers = 6;

    const iterations = 10000;

    for (let i = 0; i < iterations; i++) {
      for (let numOps = 5; numOps < 40; numOps++) {
        const ops: Operation<number, number>[] = [];
        for (let opNr = 0; opNr < numOps; opNr++) {
          ops.push(randomOperation(numKeys, numLayers));
        }

        const ranges: Summarise<number>[] = [];
        for (let summariseNr = 0; summariseNr < 8; summariseNr++) {
          ranges.push(randomSummary(numKeys));
        }

        await runTestCase(ops, ranges);
      }
    }
  },
});

Deno.test({
  name: "Randomly Test DenoKV",
  async fn() {
    const numKeys = 8;
    const numLayers = 5;

    const iterations = 100;

    for (let i = 0; i < iterations; i++) {
      for (let numOps = 5; numOps < 16; numOps++) {
        const ops: Operation<number, number>[] = [];
        for (let opNr = 0; opNr < numOps; opNr++) {
          ops.push(randomOperation(numKeys, numLayers));
        }

        const ranges = collectSync(exhaustivelyGenerateRanges(numKeys));
        await runTestCase(ops, ranges, true);
      }
    }
  },
});

Deno.test({
  name: "Even More Random Tests For DenoKV",
  async fn() {
    const numKeys = 16;
    const numLayers = 6;

    const iterations = 50;

    for (let i = 0; i < iterations; i++) {
      for (let numOps = 5; numOps < 40; numOps++) {
        const ops: Operation<number, number>[] = [];
        for (let opNr = 0; opNr < numOps; opNr++) {
          ops.push(randomOperation(numKeys, numLayers));
        }

        const ranges: Summarise<number>[] = [];
        for (let summariseNr = 0; summariseNr < 8; summariseNr++) {
          ranges.push(randomSummary(numKeys));
        }

        await runTestCase(ops, ranges, true);
      }
    }
  },
});

Deno.test({
  name: "Exhaustive Tests",
  async fn() {
    const numKeys = 5;
    const numLayers = 4;

    // Test stores with a single operation.
    for (const op of exhaustivelyGenerateOperations(numKeys, numLayers)) {
      const ranges = collectSync(exhaustivelyGenerateRanges(numKeys));
      await runTestCase([op], ranges);
    }

    // Test stores with two operations.
    for (const op1 of exhaustivelyGenerateOperations(numKeys, numLayers)) {
      for (const op2 of exhaustivelyGenerateOperations(numKeys, numLayers)) {
        const ranges = collectSync(exhaustivelyGenerateRanges(numKeys));
        await runTestCase([op1, op2], ranges);
      }
    }

    // Test stores with three operations.
    for (const op1 of exhaustivelyGenerateOperations(numKeys, numLayers)) {
      for (const op2 of exhaustivelyGenerateOperations(numKeys, numLayers)) {
        for (const op3 of exhaustivelyGenerateOperations(numKeys, numLayers)) {
          const ranges = collectSync(exhaustivelyGenerateRanges(numKeys));
          await runTestCase([op1, op2, op3], ranges);
        }
      }
    }

    // Takes too long
    // // Test stores with four operations.
    // for (const op1 of exhaustivelyGenerateOperations(numKeys, numLayers)) {
    //   for (const op2 of exhaustivelyGenerateOperations(numKeys, numLayers)) {
    //     for (const op3 of exhaustivelyGenerateOperations(numKeys, numLayers)) {
    //       for (
    //         const op4 of exhaustivelyGenerateOperations(numKeys, numLayers)
    //       ) {
    //         const ranges = collectSync(exhaustivelyGenerateRanges(numKeys));
    //         await runTestCase([op1, op2, op3, op4], ranges);
    //       }
    //     }
    //   }
    // }
  },
});

function* exhaustivelyGenerateOperations(numKeys: number, numLayers: number) {
  // Insertions
  for (let key = 0; key < numKeys; key++) {
    for (let value = 8; value < 10; value++) {
      for (let level = 0; level < numLayers; level++) {
        yield { key, value, level };
      }
    }
  }

  // Deletions
  for (let key = 0; key < numKeys; key++) {
    yield { key };
  }
}

function* exhaustivelyGenerateRanges(numKeys: number) {
  for (let start = 0; start <= numKeys; start++) {
    for (let end = start; end <= numKeys; end++) {
      yield {
        start: start === 0 ? undefined : start - 1,
        end: end < numKeys ? end : undefined,
      };
    }
  }
}

function getRandomInt(max: number): number {
  return Math.floor(Math.random() * max);
}

function randomOperation(
  numKeys: number,
  numLayers: number,
): Operation<number, number> {
  const rand = Math.random();

  if (rand < 0.5) {
    // Generate insertion op.
    return {
      key: getRandomInt(numKeys),
      value: getRandomInt(10),
      level: getRandomInt(numLayers),
    };
  } else {
    // Generate deletion op.
    return {
      key: getRandomInt(numKeys),
    };
  }
}

function randomSummary(
  numKeys: number,
): Summarise<number> {
  const lower = getRandomInt(numKeys + 1);
  const start = lower === numKeys ? undefined : lower;

  const upper = getRandomInt(numKeys);
  const end = (start !== undefined && upper < start) ? undefined : upper;

  return { start, end };
}

/**
 * Collect an async iterator into an array.
 */
async function collect<T>(iter: AsyncIterable<T>): Promise<T[]> {
  const arr: T[] = [];

  for await (const item of iter) {
    arr.push(item);
  }

  return arr;
}

function collectSync<T>(iter: Iterable<T>): T[] {
  const arr: T[] = [];

  for (const item of iter) {
    arr.push(item);
  }

  return arr;
}
