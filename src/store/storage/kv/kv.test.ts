import { assertEquals } from "https://deno.land/std@0.223.0/assert/assert_equals.ts";

import { KvDriver } from "./types.ts";
import { KvDriverInMemory } from "./kv_driver_in_memory.ts";

Deno.test("Test in-memory kv store", async (t) => {
  const store = new KvDriverInMemory<number[], string>(
    compareNumberArrays,
    isPrefix,
  );
  await testKvStore(t, store);
});

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

function isPrefix(a: number[], b: number[]): boolean {
  if (a.length > b.length) {
    return false;
  } else {
    for (let i = 0; i < a.length; i++) {
      if (a[i] !== b[i]) {
        return false;
      }
    }

    return true;
  }
}

async function testKvStore(
  t: Deno.TestContext,
  store: KvDriver<number[], string>,
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
    store.set([42], "foo");

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
    store.set([42, 44], "bar");

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
    store.delete([42, 44]);

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

  await t.step("Update", async () => {
    store.set([42], "b");

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
    store.set([], "a");
    store.set([42, 44, 7], "e");
    store.set([42], "b");
    store.set([47, 1], "h");
    store.set([42, 44, 3], "d");
    store.set([42, 44], "c");
    store.set([42, 55], "f");
    store.set([47, 0, 0], "g");
    store.set([99], "i");

    assertEquals((await collect(store.list({ prefix: [] }))).map(entry => entry.value), ["a", "b", "c", "d", "e", "f", "g", "h", "i"]);
    assertEquals((await collect(store.list({ prefix: [] }, {limit: 3}))).map(entry => entry.value), ["a", "b", "c"]);
    assertEquals((await collect(store.list({ prefix: [] }, {limit: 0}))).map(entry => entry.value), []);
    assertEquals((await collect(store.list({ prefix: [] }, {reverse: true}))).map(entry => entry.value), ["i", "h", "g", "f", "e", "d", "c", "b", "a"]);
    assertEquals((await collect(store.list({ prefix: [] }, {reverse: true, limit: 3}))).map(entry => entry.value), ["i", "h", "g"]);
    assertEquals((await collect(store.list({ prefix: [] }, {reverse: true, limit: 0}))).map(entry => entry.value), []);

    assertEquals((await collect(store.list({ prefix: [42] }))).map(entry => entry.value), ["b", "c", "d", "e", "f"]);
    assertEquals((await collect(store.list({ prefix: [42] }, {limit: 3}))).map(entry => entry.value), ["b", "c", "d"]);
    assertEquals((await collect(store.list({ prefix: [42] }, {limit: 0}))).map(entry => entry.value), []);
    assertEquals((await collect(store.list({ prefix: [42] }, {reverse: true}))).map(entry => entry.value), ["f", "e", "d", "c", "b"]);
    assertEquals((await collect(store.list({ prefix: [42] }, {reverse: true, limit: 3}))).map(entry => entry.value), ["f", "e", "d"]);
    assertEquals((await collect(store.list({ prefix: [42] }, {reverse: true, limit: 0}))).map(entry => entry.value), []);

    assertEquals((await collect(store.list({ start: [42, 44, 3], end: [47, 1] }))).map(entry => entry.value), ["d", "e", "f", "g"]);
    assertEquals((await collect(store.list({ start: [42, 44, 3], end: [47, 1] }, {limit: 3}))).map(entry => entry.value), ["d", "e", "f"]);
    assertEquals((await collect(store.list({ start: [42, 44, 3], end: [47, 1] }, {limit: 0}))).map(entry => entry.value), []);
    assertEquals((await collect(store.list({ start: [42, 44, 3], end: [47, 1] }, {reverse: true}))).map(entry => entry.value), ["g", "f", "e", "d"]);
    assertEquals((await collect(store.list({ start: [42, 44, 3], end: [47, 1] }, {reverse: true, limit: 3}))).map(entry => entry.value), ["g", "f", "e"]);
    assertEquals((await collect(store.list({ start: [42, 44, 3], end: [47, 1] }, {reverse: true, limit: 0}))).map(entry => entry.value), []);
  });

  await t.step("Clear a store", async () => {
    store.clear({prefix: [42, 44], start: [], end: [99999]});
    assertEquals((await collect(store.list({ prefix: [] }))).map(entry => entry.value), ["a", "b", "f", "g", "h", "i"]);

    store.clear({prefix: [], start: [6], end: [47, 1]});
    assertEquals((await collect(store.list({ prefix: [] }))).map(entry => entry.value), ["a", "h", "i"]);

    store.clear();
    assertEquals((await collect(store.list({ prefix: [] }))).map(entry => entry.value), []);
  });

  await t.step("Batch operations", async () => {
    store.set([42], "foo");
    store.set([44], "bar");
    store.set([46], "baz");

    const batch = store.batch();
    batch.set([46], "qux");
    batch.delete([42]);
    batch.delete([99]);
    batch.set([99], "bla");
    batch.set([23], "tmp");
    batch.delete([23]);
    batch.set([11], "hrumph");
    batch.set([11], "floop");

    assertEquals((await collect(store.list({ prefix: [] }))).map(entry => entry.value), ["foo", "bar", "baz"]);
    await batch.commit();
    assertEquals((await collect(store.list({ prefix: [] }))).map(entry => entry.value), ["floop", "bar", "qux", "bla"]);
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
