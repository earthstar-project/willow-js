// deno test --unstable-kv ./src/store/storage/kv/kv.test.ts

import "https://deno.land/x/indexeddb@1.3.5/polyfill_memory.ts";
import { assertEquals } from "https://deno.land/std@0.223.0/assert/assert_equals.ts";

import {
  compareKeys,
  isFirstKeyPrefixOfSecondKey,
  KvBatch,
  KvDriver,
  KvKey,
} from "./types.ts";
import { KvDriverInMemory } from "./kv_driver_in_memory.ts";
import { KvDriverDeno } from "./kv_driver_deno.ts";
import { KvDriverIndexedDB } from "./kv_driver_indexeddb.ts";

Deno.test("Test control kv store", async (t) => {
  const store = new KvDriverControl();
  await testKvStore(t, store);
});

Deno.test("Test in-memory kv store", async (t) => {
  const store = new KvDriverInMemory();
  await testKvStore(t, store);
});

Deno.test("Test deno kv store", async (t) => {
  const store = new KvDriverDeno(await Deno.openKv(":memory:"));
  await testKvStore(t, store);
  store.close();
});

Deno.test("Test IndexedDB kv store", async (t) => {
  const store = new KvDriverIndexedDB();
  await testKvStore(t, store);
});

async function testKvStore(
  t: Deno.TestContext,
  store: KvDriver,
) {
  await store.clear();

  await t.step("Check store is initially empty", async () => {
    assertEquals(await collect(store.list({ prefix: [] })), []);
    assertEquals(await store.get([42]), undefined);
    assertEquals(await store.get([43]), undefined);
  });

  await t.step(
    "Listing with arguments in empty store does not break anything",
    async () => {
      assertEquals(await collect(store.list({ prefix: [42] })), []);
      assertEquals(await collect(store.list({ start: [1], end: [44, 0] })), []);
    },
  );

  await t.step("Insert into empty store", async () => {
    await store.set([42], "foo");

    assertEquals(await store.get([42]), "foo");
    assertEquals(await store.get([43]), undefined);
    assertEquals(await collect(store.list({ prefix: [] })), [{
      key: [42],
      value: "foo",
    }]);

    assertEquals(await collect(store.list({ prefix: [42] })), [{
      key: [42],
      value: "foo",
    }]);
    assertEquals(await collect(store.list({ start: [5, 8, 1], end: [999] })), [{
      key: [42],
      value: "foo",
    }]);
    assertEquals(await collect(store.list({ prefix: [42, 6] })), []);
    assertEquals(await collect(store.list({ prefix: [4] })), []);
    assertEquals(await collect(store.list({ start: [1], end: [24, 0] })), []);
    assertEquals(await collect(store.list({ start: [42, 0], end: [99] })), []);
  });

  await t.step("Insert second entry", async () => {
    await store.set([42, 44], "bar");

    assertEquals(await store.get([42]), "foo");
    assertEquals(await store.get([42, 44]), "bar");
    assertEquals(await store.get([43]), undefined);
    assertEquals(await collect(store.list({ prefix: [] })), [{
      key: [42],
      value: "foo",
    }, {
      key: [42, 44],
      value: "bar",
    }]);
    assertEquals(await collect(store.list({ prefix: [42] })), [{
      key: [42],
      value: "foo",
    }, {
      key: [42, 44],
      value: "bar",
    }]);
    assertEquals(await collect(store.list({ start: [5, 8, 1], end: [999] })), [{
      key: [42],
      value: "foo",
    }, {
      key: [42, 44],
      value: "bar",
    }]);
    assertEquals(await collect(store.list({ start: [5], end: [42, 1] })), [{
      key: [42],
      value: "foo",
    }]);
    assertEquals(await collect(store.list({ start: [5], end: [42, 44] })), [{
      key: [42],
      value: "foo",
    }]);
    assertEquals(await collect(store.list({ start: [5], end: [42, 45] })), [{
      key: [42],
      value: "foo",
    }, {
      key: [42, 44],
      value: "bar",
    }]);
    assertEquals(await collect(store.list({ prefix: [42, 6] })), []);
    assertEquals(await collect(store.list({ prefix: [4] })), []);
    assertEquals(await collect(store.list({ start: [1], end: [24, 0] })), []);
    assertEquals(
      await collect(store.list({ start: [42, 999], end: [99] })),
      [],
    );
    assertEquals(await collect(store.list({ start: [42, 0], end: [99] })), [{
      key: [42, 44],
      value: "bar",
    }]);
  });

  await t.step("Delete", async () => {
    await store.delete([42, 44]);

    assertEquals(await store.get([42]), "foo");
    assertEquals(await store.get([42, 44]), undefined);
    assertEquals(await store.get([43]), undefined);
    assertEquals(await collect(store.list({ prefix: [] })), [{
      key: [42],
      value: "foo",
    }]);
    assertEquals(await collect(store.list({ prefix: [42] })), [{
      key: [42],
      value: "foo",
    }]);
    assertEquals(await collect(store.list({ start: [5, 8, 1], end: [999] })), [{
      key: [42],
      value: "foo",
    }]);
    assertEquals(await collect(store.list({ prefix: [42, 6] })), []);
    assertEquals(await collect(store.list({ prefix: [4] })), []);
    assertEquals(await collect(store.list({ start: [1], end: [24, 0] })), []);
    assertEquals(await collect(store.list({ start: [42, 0], end: [99] })), []);
  });

  await t.step("Update", async () => {
    await store.set([42], "b");

    assertEquals(await store.get([42]), "b");
    assertEquals(await store.get([43]), undefined);
    assertEquals(await collect(store.list({ prefix: [] })), [{
      key: [42],
      value: "b",
    }]);
    assertEquals(await collect(store.list({ prefix: [42] })), [{
      key: [42],
      value: "b",
    }]);
    assertEquals(await collect(store.list({ start: [5, 8, 1], end: [999] })), [{
      key: [42],
      value: "b",
    }]);
    assertEquals(await collect(store.list({ prefix: [42, 6] })), []);
    assertEquals(await collect(store.list({ prefix: [4] })), []);
    assertEquals(await collect(store.list({ start: [1], end: [24, 0] })), []);
    assertEquals(await collect(store.list({ start: [42, 0], end: [99] })), []);
  });

  await t.step("Test a more populated store store", async () => {
    await store.set([1], "a");
    await store.set([42, 44, 7], "e");
    await store.set([42], "b");
    await store.set([47, 1], "h");
    await store.set([42, 44, 3], "d");
    await store.set([42, 44], "c");
    await store.set([42, 55], "f");
    await store.set([47, 0, 0], "g");
    await store.set([99], "i");

    assertEquals(
      (await collect(store.list({ prefix: [] }))).map((entry) => entry.value),
      ["a", "b", "c", "d", "e", "f", "g", "h", "i"],
    );
    assertEquals(
      (await collect(store.list({ prefix: [] }, { limit: 3 }))).map((entry) =>
        entry.value
      ),
      ["a", "b", "c"],
    );
    assertEquals(
      (await collect(store.list({ prefix: [] }, { reverse: true }))).map(
        (entry) => entry.value,
      ),
      ["i", "h", "g", "f", "e", "d", "c", "b", "a"],
    );
    assertEquals(
      (await collect(store.list({ prefix: [] }, { reverse: true, limit: 3 })))
        .map((entry) => entry.value),
      ["i", "h", "g"],
    );

    assertEquals(
      (await collect(store.list({ prefix: [42] }))).map((entry) => entry.value),
      ["b", "c", "d", "e", "f"],
    );
    assertEquals(
      (await collect(store.list({ prefix: [42] }, { limit: 3 }))).map((entry) =>
        entry.value
      ),
      ["b", "c", "d"],
    );
    assertEquals(
      (await collect(store.list({ prefix: [42] }, { reverse: true }))).map(
        (entry) => entry.value,
      ),
      ["f", "e", "d", "c", "b"],
    );
    assertEquals(
      (await collect(store.list({ prefix: [42] }, { reverse: true, limit: 3 })))
        .map((entry) => entry.value),
      ["f", "e", "d"],
    );

    assertEquals(
      (await collect(store.list({ start: [42, 44, 3], end: [47, 1] }))).map(
        (entry) => entry.value,
      ),
      ["d", "e", "f", "g"],
    );
    assertEquals(
      (await collect(
        store.list({ start: [42, 44, 3], end: [47, 1] }, { limit: 3 }),
      )).map((entry) => entry.value),
      ["d", "e", "f"],
    );
    assertEquals(
      (await collect(
        store.list({ start: [42, 44, 3], end: [47, 1] }, { reverse: true }),
      )).map((entry) => entry.value),
      ["g", "f", "e", "d"],
    );
    assertEquals(
      (await collect(
        store.list({ start: [42, 44, 3], end: [47, 1] }, {
          reverse: true,
          limit: 3,
        }),
      )).map((entry) => entry.value),
      ["g", "f", "e"],
    );
  });

  await t.step("Clear a store", async () => {
    await store.clear({
      prefix: [42, 44],
      start: [0],
      end: [99999],
    });
    assertEquals(
      (await collect(store.list({ prefix: [] }))).map((entry) => entry.value),
      ["a", "b", "f", "g", "h", "i"],
    );

    await store.clear({ start: [6], end: [47, 1] });
    assertEquals(
      (await collect(store.list({ prefix: [] }))).map((entry) => entry.value),
      ["a", "h", "i"],
    );

    await store.clear();
    assertEquals(
      (await collect(store.list({ prefix: [] }))).map((entry) => entry.value),
      [],
    );
  });

  await t.step("Batch operations", async () => {
    await store.set([42], "foo");
    await store.set([44], "bar");
    await store.set([46], "baz");

    const batch = store.batch();
    batch.set([46], "qux");
    batch.delete([42]);
    batch.delete([99]);
    batch.set([99], "bla");
    batch.set([23], "tmp");
    batch.delete([23]);
    batch.set([11], "hrumph");
    batch.set([11], "floop");

    assertEquals(
      (await collect(store.list({ prefix: [] }))).map((entry) => entry.value),
      ["foo", "bar", "baz"],
    );
    await batch.commit();
    assertEquals(
      (await collect(store.list({ prefix: [] }))).map((entry) => entry.value),
      ["floop", "bar", "qux", "bla"],
    );
  });
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

/**
 * An inefficient KV driver that definitely implements the interface correctly.
 */
class KvDriverControl implements KvDriver {
  private data: [KvKey, unknown][]; // always sorted by the key.

  constructor() {
    this.data = [];
  }

  get<Value>(key: KvKey): Promise<Value | undefined> {
    for (const entry of this.data) {
      if (compareKeys(key, entry[0]) === 0) {
        return Promise.resolve(<Value> entry[1]);
      }
    }
    return Promise.resolve(undefined);
  }

  set<Value>(key: KvKey, value: Value): Promise<void> {
    if (this.data.length === 0 || compareKeys(key, this.data[0][0]) < 0) {
      // Should become the new first element.
      this.data.unshift([key, value]);
      return Promise.resolve();
    }

    for (let i = 0; i < this.data.length; i++) {
      if (compareKeys(key, this.data[i][0]) === 0) {
        // Replace a value if the key already exists.
        this.data[i][1] = value;
        return Promise.resolve();
      } else if (
        (compareKeys(key, this.data[i][0]) > 0) &&
        ((i + 1 === this.data.length) ||
          (compareKeys(key, this.data[i + 1][0]) < 0))
      ) {
        // Found the i where the key is greater than that of entry i but less than that of entry i + 1, insert here.
        this.data.splice(i + 1, 0, [key, value]);
        return Promise.resolve();
      }
    }

    // If we reach this, all values were less, so we append to the end.
    this.data.push([key, value]);
    return Promise.resolve();
  }

  delete(key: KvKey): Promise<boolean> {
    for (let i = 0; i < this.data.length; i++) {
      if (compareKeys(key, this.data[i][0]) === 0) {
        this.data.splice(i, 1);
        return Promise.resolve(true);
      }
    }

    return Promise.resolve(false);
  }

  async *list<Value>(
    selector: {
      start?: KvKey | undefined;
      end?: KvKey | undefined;
      prefix?: KvKey | undefined;
    },
    opts_?: {
      reverse?: boolean | undefined;
      limit?: number | undefined;
      batchSize?: number | undefined;
    } | undefined,
  ): AsyncIterable<{ key: KvKey; value: Value }> {
    const opts = opts_ === undefined ? {} : opts_;
    const prefix = selector.prefix === undefined ? [] : selector.prefix;

    let count = 0;
    for (let i_ = 0; i_ < this.data.length; i_++) {
      if (opts.limit !== undefined && count >= opts.limit) {
        return;
      }

      const i = opts.reverse ? this.data.length - (i_ + 1) : i_;
      const [key, value] = this.data[i];

      if (
        isFirstKeyPrefixOfSecondKey(prefix, key) &&
        (!selector.start || (compareKeys(key, selector.start) >= 0)) &&
        (!selector.end || (compareKeys(key, selector.end) < 0))
      ) {
        yield { key, value: <Value> value };
        count += 1;
      }
    }
  }

  clear(
    opts_?: {
      prefix?: KvKey | undefined;
      start?: KvKey | undefined;
      end?: KvKey | undefined;
    } | undefined,
  ): Promise<void> {
    const opts = opts_ === undefined ? {} : opts_;
    const prefix = opts.prefix === undefined ? [] : opts.prefix;
    const data = this.data;

    /**
     * Clear the first matching entry of index start or greater. Report its index, or -1 if there was none.
     */
    function clearNextFromIndex(start: number): number {
      for (let i = start; i < data.length; i++) {
        const key = data[i][0];
        if (
          isFirstKeyPrefixOfSecondKey(prefix, key) &&
          ((opts.start === undefined) || (compareKeys(key, opts.start) >= 0)) &&
          ((opts.end === undefined) || (compareKeys(key, opts.end) < 0))
        ) {
          data.splice(i, 1);
          return i;
        }
      }

      return -1;
    }

    let i = 0;
    while (i !== -1) {
      i = clearNextFromIndex(i);
    }

    return Promise.resolve();
  }

  batch(): KvBatch {
    const operations: BatchOperation[] = [];

    return {
      set: <Value>(key: KvKey, value: Value) =>
        operations.push({ set: { key, value } }),
      delete: (key: KvKey) => operations.push({ delete: { key } }),
      commit: () => {
        for (const operation of operations) {
          if ("set" in operation) {
            this.set(operation.set.key, operation.set.value);
          } else {
            this.delete(operation.delete.key);
          }
        }

        return Promise.resolve();
      },
    };
  }
}

type BatchOperation = { set: { key: KvKey; value: unknown } } | {
  delete: { key: KvKey };
};

// Randomized testing stuff.

type Operation<Key, Value> =
  | Insert<Key, Value>
  | Delete<Key>
  | Clear<Key>;

type Insert<Key, Value> = {
  key: Key;
  value: Value;
};

type Delete<Key> = { key: Key };

type Clear<Key> = {
  clear: {
    start?: Key[];
    end?: Key[];
    prefix?: Key[];
  };
};

type Query<Key> = Get<Key> | DoList<Key>;

type Get<Key> = {
  key: Key;
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

function getRandomInt(max: number): number {
  return Math.floor(Math.random() * max);
}

function randomOperation(
  numKeys: number,
): Operation<number, number> {
  const rand = Math.random();

  if (rand < 0.33333333333) {
    // Generate insertion op.
    return {
      key: getRandomInt(numKeys),
      value: getRandomInt(10),
    };
  } else if (rand < 0.666666666666) {
    // Generate deletion op.
    return {
      key: getRandomInt(numKeys),
    };
  } else {
    // Generate random clear op.

    const lower = getRandomInt(numKeys + 1);
    const start = lower === numKeys ? undefined : [lower];

    const upper = getRandomInt(numKeys);
    const end = (start !== undefined && upper < start[0]) ? undefined : [upper];

    const prePrefix = getRandomInt(numKeys + 1);
    const prefix = prePrefix === numKeys ? undefined : [prePrefix];

    return {
      clear: {
        start,
        end,
        prefix,
      },
    };
  }
}

function randomQuery(
  numKeys: number,
): Query<number> {
  if (Math.random() < 0.75) {
    return randomDoList(numKeys);
  } else {
    return randomGet(numKeys);
  }
}

function randomGet(
  numKeys: number,
): Get<number> {
  return {
    key: getRandomInt(numKeys),
  };
}

function randomDoList(
  numKeys: number,
): DoList<number> {
  const lower = getRandomInt(numKeys + 1);
  const start = lower === numKeys ? undefined : lower;

  const upper = getRandomInt(numKeys);
  const end = (start !== undefined && upper < start) ? undefined : upper;

  const prePrefix = getRandomInt(numKeys + 1);
  const prefix = prePrefix === numKeys ? undefined : [prePrefix];

  const reverse = Math.random() < 0.5;
  const limitTmp = getRandomInt(numKeys + 4);
  const limit = limitTmp > numKeys + 2 ? undefined : limitTmp;

  return {
    selector: {
      start: start === undefined ? undefined : [start],
      end: end === undefined ? undefined : [end],
      prefix,
    },
    opts: { reverse, limit },
  };
}

async function runTestCase(
  ops: Operation<number, number>[],
  queries: Query<number>[],
  driverUnderTest: KvDriver,
) {
  const control = new KvDriverControl();

  for (const op of ops) {
    if ("clear" in op) {
      await driverUnderTest.clear(op.clear);
      await control.clear(op.clear);
    } else if ("value" in op) {
      await driverUnderTest.set([op.key], op.value);
      await control.set([op.key], op.value);
    } else {
      await driverUnderTest.delete([op.key]);
      await control.delete([op.key]);
    }
  }

  for (const query of queries) {
    if ("key" in query) {
      const got = await driverUnderTest.get([query.key]);
      const expected = await control.get([query.key]);

      try {
        assertEquals(got, expected);
      } catch (_err) {
        assertEquals(
          got,
          expected,
          `
          ===================
          Query: ${JSON.stringify(query, undefined, 2)}
          
          Operations: ${JSON.stringify(ops, undefined, 2)}
          
          got: ${JSON.stringify(got)}
          
          expected: ${JSON.stringify(expected)}`,
        );
      }
    } else {
      const got = await collect(
        driverUnderTest.list(query.selector, query.opts),
      );
      const expected = await collect(
        control.list(query.selector, query.opts),
      );

      try {
        assertEquals(got, expected);
      } catch (_err) {
        assertEquals(
          got,
          expected,
          `
          ===================
          Query: ${JSON.stringify(query, undefined, 2)}
          
          Operations: ${JSON.stringify(ops, undefined, 2)}
          
          got: ${JSON.stringify(got)}
          
          expected: ${JSON.stringify(expected)}`,
        );
      }
    }
  }
}

Deno.test({
  name: "Regression InMemory 1",
  async fn() {
    const ops = [
      {
        "key": 3,
        "value": 8,
      },
      {
        "key": 2,
        "value": 2,
      },
      {
        "clear": {
          "start": [
            0,
          ],
          "end": [
            4,
          ],
          "prefix": [
            2,
          ],
        },
      },
    ];

    const queries = [{
      "selector": {
        "end": [
          3,
        ],
        "prefix": [
          2,
        ],
      },
      "opts": {
        "reverse": false,
        "limit": 6,
      },
    }];
    await runTestCase(ops, queries, new KvDriverInMemory());
  },
});

Deno.test({
  name: "Random Tests InMemory",
  async fn() {
    let numKeys = 8;
    let iterations = 10000;

    for (let i = 0; i < iterations; i++) {
      for (let numOps = 5; numOps < 16; numOps++) {
        const ops: Operation<number, number>[] = [];
        for (let opNr = 0; opNr < numOps; opNr++) {
          ops.push(randomOperation(numKeys));
        }

        const queries: Query<number>[] = [];
        for (let summariseNr = 0; summariseNr < 8; summariseNr++) {
          queries.push(randomQuery(numKeys));
        }

        await runTestCase(ops, queries, new KvDriverInMemory());
      }
    }

    numKeys = 16;
    iterations = 10000;

    for (let i = 0; i < iterations; i++) {
      for (let numOps = 5; numOps < 40; numOps++) {
        const ops: Operation<number, number>[] = [];
        for (let opNr = 0; opNr < numOps; opNr++) {
          ops.push(randomOperation(numKeys));
        }

        const queries: Query<number>[] = [];
        for (let summariseNr = 0; summariseNr < 8; summariseNr++) {
          queries.push(randomQuery(numKeys));
        }

        await runTestCase(ops, queries, new KvDriverInMemory());
      }
    }
  },
});

Deno.test({
  name: "Regression Deno 1",
  async fn() {
    const ops = [
      {
        "key": 4,
        "value": 8,
      },
    ];

    const queries = [{
      "selector": {
        "start": [
          4,
        ],
        "prefix": [
          4,
        ],
      },
    }];

    const denoKv = new KvDriverDeno(await Deno.openKv(":memory:"));
    await runTestCase(ops, queries, denoKv);
    denoKv.close();
  },
});

Deno.test({
  name: "Regression Deno 2",
  async fn() {
    const ops = [
      {
        "key": 4,
        "value": 8,
      },
    ];

    const queries = [{
      "selector": {
        "end": [
          4,
        ],
        "prefix": [
          4,
        ],
      },
    }];

    const denoKv = new KvDriverDeno(await Deno.openKv(":memory:"));
    await runTestCase(ops, queries, denoKv);
    denoKv.close();
  },
});

Deno.test({
  name: "Regression Deno 3",
  async fn() {
    const ops = [
      {
        "key": 0,
        "value": 0,
      },
      {
        "clear": {
          "start": [
            7,
          ],
          "prefix": [
            5,
          ],
        },
      },
    ];

    const queries = [{ key: 0 }];

    const denoKv = new KvDriverDeno(await Deno.openKv(":memory:"));
    await runTestCase(ops, queries, denoKv);
    denoKv.close();
  },
});

Deno.test({
  name: "Random Tests Deno",
  async fn() {
    let numKeys = 8;
    let iterations = 50;

    for (let i = 0; i < iterations; i++) {
      for (let numOps = 5; numOps < 16; numOps++) {
        const ops: Operation<number, number>[] = [];
        for (let opNr = 0; opNr < numOps; opNr++) {
          ops.push(randomOperation(numKeys));
        }

        const queries: Query<number>[] = [];
        for (let summariseNr = 0; summariseNr < 8; summariseNr++) {
          queries.push(randomQuery(numKeys));
        }

        const denoKv = new KvDriverDeno(await Deno.openKv(":memory:"));
        await runTestCase(ops, queries, denoKv);
        denoKv.close();
      }
    }

    numKeys = 16;
    iterations = 50;

    for (let i = 0; i < iterations; i++) {
      for (let numOps = 5; numOps < 40; numOps++) {
        const ops: Operation<number, number>[] = [];
        for (let opNr = 0; opNr < numOps; opNr++) {
          ops.push(randomOperation(numKeys));
        }

        const queries: Query<number>[] = [];
        for (let summariseNr = 0; summariseNr < 8; summariseNr++) {
          queries.push(randomQuery(numKeys));
        }

        const denoKv = new KvDriverDeno(await Deno.openKv(":memory:"));
        await runTestCase(ops, queries, denoKv);
        denoKv.close();
      }
    }
  },
});
