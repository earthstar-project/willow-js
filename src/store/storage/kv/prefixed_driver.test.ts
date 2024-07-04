// deno test --unstable-kv ./src/store/storage/kv/prefixed_driver.test.ts

import { KvDriverInMemory } from "./kv_driver_in_memory.ts";
import { KvDriverDeno } from "./kv_driver_deno.ts";
import { PrefixedDriver } from "./prefixed_driver.ts";
import type { KvDriver } from "./types.ts";
import { assertEquals } from "@std/assert";

type Operation<Key, Value> =
  | Insert<Key, Value>
  | Delete<Key>
  | Clear<Key>;

type Insert<Key, Value> = {
  key: Key;
  value: Value;
  prefix: number;
};

type Delete<Key> = { key: Key; prefix: number };

type Clear<Key> = {
  clear: {
    start?: Key[];
    end?: Key[];
    prefix?: number[];
  };
  store: number;
};

type DoList<Key> = {
  selector: {
    start?: Key[];
    end?: Key[];
    prefix?: Key[];
  };
  opts?: {
    reverse?: boolean;
    limit?: number;
    batchSize?: number;
  };
};

async function runTestCase(
  ops: Operation<number, number>[],
  listings: DoList<number>[],
  numberOfPrefixes: number,
  useDenoKv?: boolean,
) {
  let underlyingStore: KvDriver = new KvDriverInMemory();

  if (useDenoKv) {
    underlyingStore = new KvDriverDeno(await Deno.openKv(":memory:"));
    await underlyingStore.clear();
  }

  const stores = [];
  const controlStores = [];
  for (let i = 0; i < numberOfPrefixes; i++) {
    stores.push(new PrefixedDriver([i], underlyingStore));
    controlStores.push(new KvDriverInMemory());
  }

  for (const op of ops) {
    if ("clear" in op) {
      await stores[op.store].clear(op.clear);
      await controlStores[op.store].clear(op.clear);
    } else if ("value" in op) {
      await stores[op.prefix].set([op.key], op.value);
      await controlStores[op.prefix].set([op.key], op.value);
    } else {
      await stores[op.prefix].delete([op.key]);
      await controlStores[op.prefix].delete([op.key]);
    }
  }

  for (const doList of listings) {
    for (let i = 0; i < numberOfPrefixes; i++) {
      const got = await collect(stores[i].list(doList.selector, doList.opts));
      const expected = await collect(
        controlStores[i].list(doList.selector, doList.opts),
      );

      try {
        assertEquals(got, expected);
      } catch (_err) {
        //   await store.print();

        assertEquals(
          got,
          expected,
          `
        ===================
        Summary: ${JSON.stringify(doList, undefined, 2)}
        
        Operations: ${JSON.stringify(ops, undefined, 2)}
        
        In prefix ${i}
        
        got: ${JSON.stringify(got)}
        
        expected: ${JSON.stringify(expected)}`,
        );
      }
    }
  }

  if (useDenoKv) {
    (<KvDriverDeno> <unknown> underlyingStore).close();
  }
}

Deno.test({
  name: "Regression 1",
  async fn() {
    const ops = [
      {
        "key": 0,
        "value": 42,
        "prefix": 1,
      },
    ];
    const listings = [{
      "selector": {},
      "opts": {},
    }];
    await runTestCase(ops, listings, 3);
  },
});

Deno.test({
  name: "Regression 2",
  async fn() {
    const ops = [
      {
        "key": 7,
        "value": 6,
        "prefix": 2,
      },
      {
        "key": 5,
        "value": 8,
        "prefix": 1,
      },
      {
        "clear": {},
        "store": 1,
      },
    ];
    const listings = [{
      "selector": {
        "start": [
          0,
        ],
        "end": [
          6,
        ],
      },
      "opts": {
        "reverse": false,
        "limit": 3,
      },
    }];
    await runTestCase(ops, listings, 3);
  },
});

Deno.test.ignore({
  name: "Random Tests",
  async fn() {
    const numKeys = 8;
    const numPrefixes = 3;

    const iterations = 10000;

    for (let i = 0; i < iterations; i++) {
      for (let numOps = 5; numOps < 16; numOps++) {
        const ops: Operation<number, number>[] = [];
        for (let opNr = 0; opNr < numOps; opNr++) {
          ops.push(randomOperation(numKeys, numPrefixes));
        }

        const ranges: DoList<number>[] = [];
        for (let summariseNr = 0; summariseNr < 8; summariseNr++) {
          ranges.push(randomDoList(numKeys));
        }

        await runTestCase(ops, ranges, numPrefixes);
      }
    }
  },
});

Deno.test.ignore({
  name: "More random Tests",
  async fn() {
    const numKeys = 16;
    const numPrefixes = 4;

    const iterations = 10000;

    for (let i = 0; i < iterations; i++) {
      for (let numOps = 5; numOps < 40; numOps++) {
        const ops: Operation<number, number>[] = [];
        for (let opNr = 0; opNr < numOps; opNr++) {
          ops.push(randomOperation(numKeys, numPrefixes));
        }

        const ranges: DoList<number>[] = [];
        for (let summariseNr = 0; summariseNr < 8; summariseNr++) {
          ranges.push(randomDoList(numKeys));
        }

        await runTestCase(ops, ranges, numPrefixes);
      }
    }
  },
});

Deno.test({
  name: "Random Tests DenoKV",
  async fn() {
    const numKeys = 8;
    const numPrefixes = 3;

    const iterations = 50;

    for (let i = 0; i < iterations; i++) {
      for (let numOps = 5; numOps < 16; numOps++) {
        const ops: Operation<number, number>[] = [];
        for (let opNr = 0; opNr < numOps; opNr++) {
          ops.push(randomOperation(numKeys, numPrefixes));
        }

        const ranges: DoList<number>[] = [];
        for (let summariseNr = 0; summariseNr < 8; summariseNr++) {
          ranges.push(randomDoList(numKeys));
        }

        await runTestCase(ops, ranges, numPrefixes, true);
      }
    }
  },
});

Deno.test({
  name: "More random Tests DenoKV",
  async fn() {
    const numKeys = 16;
    const numPrefixes = 4;

    const iterations = 50;

    for (let i = 0; i < iterations; i++) {
      for (let numOps = 5; numOps < 40; numOps++) {
        const ops: Operation<number, number>[] = [];
        for (let opNr = 0; opNr < numOps; opNr++) {
          ops.push(randomOperation(numKeys, numPrefixes));
        }

        const ranges: DoList<number>[] = [];
        for (let summariseNr = 0; summariseNr < 8; summariseNr++) {
          ranges.push(randomDoList(numKeys));
        }

        await runTestCase(ops, ranges, numPrefixes, true);
      }
    }
  },
});

// Deno.test({
//   name: "Randomly Test DenoKV",
//   async fn() {
//     const numKeys = 8;
//     const numLayers = 5;

//     const iterations = 100;

//     for (let i = 0; i < iterations; i++) {
//       for (let numOps = 5; numOps < 16; numOps++) {
//         const ops: Operation<number, number>[] = [];
//         for (let opNr = 0; opNr < numOps; opNr++) {
//           ops.push(randomOperation(numKeys, numLayers));
//         }

//         const ranges = collectSync(exhaustivelyGenerateRanges(numKeys));
//         await runTestCase(ops, ranges, true);
//       }
//     }
//   },
// });

// Deno.test({
//   name: "Even More Random Tests For DenoKV",
//   async fn() {
//     const numKeys = 16;
//     const numLayers = 6;

//     const iterations = 50;

//     for (let i = 0; i < iterations; i++) {
//       for (let numOps = 5; numOps < 40; numOps++) {
//         const ops: Operation<number, number>[] = [];
//         for (let opNr = 0; opNr < numOps; opNr++) {
//           ops.push(randomOperation(numKeys, numLayers));
//         }

//         const ranges: Summarise<number>[] = [];
//         for (let summariseNr = 0; summariseNr < 8; summariseNr++) {
//           ranges.push(randomSummary(numKeys));
//         }

//         await runTestCase(ops, ranges, true);
//       }
//     }
//   },
// });

function getRandomInt(max: number): number {
  return Math.floor(Math.random() * max);
}

function randomOperation(
  numKeys: number,
  numPrefixes: number,
): Operation<number, number> {
  const rand = Math.random();

  if (rand < 0.33333333333) {
    // Generate insertion op.
    return {
      key: getRandomInt(numKeys),
      value: getRandomInt(10),
      prefix: getRandomInt(numPrefixes),
    };
  } else if (rand < 0.666666666666) {
    // Generate deletion op.
    return {
      key: getRandomInt(numKeys),
      prefix: getRandomInt(numPrefixes),
    };
  } else {
    // Generate random clear op.

    const lower = getRandomInt(numKeys + 1);
    const start = lower === numKeys ? undefined : [lower];

    const upper = getRandomInt(numKeys);
    const end = (start !== undefined && upper < start[0]) ? undefined : [upper];

    return {
      clear: {
        start,
        end,
      },
      store: getRandomInt(numPrefixes),
    };
  }
}

// Not testing prefix functionality =(
function randomDoList(
  numKeys: number,
): DoList<number> {
  const lower = getRandomInt(numKeys + 1);
  const start = lower === numKeys ? undefined : lower;

  const upper = getRandomInt(numKeys);
  const end = (start !== undefined && upper < start) ? undefined : upper;

  const reverse = Math.random() < 0.5;
  const limitTmp = getRandomInt(numKeys + 4);
  const limit = limitTmp > numKeys + 2 ? undefined : limitTmp;

  return {
    selector: {
      start: start === undefined ? undefined : [start],
      end: end === undefined ? undefined : [end],
    },
    opts: { reverse, limit },
  };
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
