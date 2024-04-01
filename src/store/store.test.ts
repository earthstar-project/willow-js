import {
  assert,
  assertEquals,
} from "https://deno.land/std@0.177.0/testing/asserts.ts";
import { Store } from "./store.ts";
import { crypto } from "https://deno.land/std@0.188.0/crypto/crypto.ts";
import {
  TestNamespace,
  testSchemeAuthorisation,
  testSchemeFingerprint,
  testSchemeNamespace,
  testSchemePath,
  testSchemePayload,
  testSchemeSubspace,
  TestSubspace,
} from "../test/test_schemes.ts";
import { fullArea, orderBytes, orderPath } from "../../deps.ts";

class TestStore extends Store<
  TestNamespace,
  TestSubspace,
  ArrayBuffer,
  TestSubspace,
  Uint8Array,
  Uint8Array
> {
  constructor(namespace = 0) {
    super({
      namespace,
      schemes: {
        namespace: testSchemeNamespace,
        subspace: testSchemeSubspace,
        path: testSchemePath,
        payload: testSchemePayload,
        authorisation: testSchemeAuthorisation,
        fingerprint: testSchemeFingerprint,
      },
    });
  }

  writeAheadFlag() {
    /* @ts-ignore */
    return this.entryDriver.writeAheadFlag;
  }

  triggerWriteAheadFlag() {
    /* @ts-ignore */
    return this.checkWriteAheadFlag();
  }
}

// ==================================
// instantiation

// Namespace length must equal protocol parameter pub key length

Deno.test("Store.set", async (test) => {
  const alfie = TestSubspace.Alfie;
  const betty = TestSubspace.Betty;

  await test.step("Fails with invalid ingestions", async () => {
    const store = new TestStore();

    // Returns an error and does not ingest payload if the entry is invalid
    const badKeypairRes = await store.set(
      {
        path: [new Uint8Array([1, 2, 3, 4])],
        payload: new Uint8Array([1, 1, 1, 1]),
        subspace: alfie,
      },
      betty,
    );

    assert(badKeypairRes.kind === "failure");
    assertEquals(badKeypairRes.reason, "invalid_entry");

    const entries = [];

    for await (
      const entry of store.query({
        area: fullArea(),
        maxCount: 0,
        maxSize: BigInt(0),
      }, "subspace")
    ) {
      entries.push(entry);
    }

    assertEquals(entries, []);
  });

  await test.step("Succeeds with valid ingestions", async () => {
    const store = new TestStore();

    const goodKeypairRes = await store.set(
      {
        path: [new Uint8Array([1, 2, 3, 4])],
        payload: new Uint8Array([1, 1, 1, 1]),
        subspace: alfie,
      },
      alfie,
    );

    assertEquals(goodKeypairRes.kind, "success");

    const entries = [];

    for await (
      const entry of store.query({
        area: fullArea(),
        maxCount: 0,
        maxSize: BigInt(0),
      }, "subspace")
    ) {
      entries.push(entry);
    }

    assert(entries[0]);
    assert(entries[0][1]);
  });

  await test.step("If a timestamp is set, it is used", async () => {
    const store = new TestStore();

    const res = await store.set(
      {
        path: [new Uint8Array([1, 2, 3, 4])],
        payload: new Uint8Array([1, 1, 1, 1]),
        timestamp: BigInt(0),
        subspace: alfie,
      },
      alfie,
    );

    assert(res.kind === "success");
    assertEquals(res.entry.timestamp, BigInt(0));
  });

  await test.step("If no timestamp is set, and there is nothing else at the same path, use the current time.", async () => {
    const store = new TestStore();

    const timestampBefore = BigInt(Date.now() * 1000);

    const res = await store.set(
      {
        path: [new Uint8Array([1, 2, 3, 4])],
        payload: new Uint8Array([1, 1, 1, 1]),
        subspace: alfie,
      },
      alfie,
    );

    assert(res.kind === "success");
    assert(res.entry.timestamp >= timestampBefore);
    assert(res.entry.timestamp <= BigInt(Date.now() * 1000));
  });
});

// ==================================
// ingestEntry

Deno.test("Store.ingestEntry", async (test) => {
  const alfie = TestSubspace.Alfie;
  const betty = TestSubspace.Betty;

  // rejects stuff from a different namespace
  await test.step("Rejects entries from a different namespace", async () => {
    const otherStore = new TestStore(4);
    const store = new TestStore();

    const otherStoreRes = await otherStore.set(
      {
        path: [new Uint8Array([0, 0, 0, 0])],
        payload: new Uint8Array(),
        subspace: alfie,
      },
      alfie,
    );

    assert(otherStoreRes.kind === "success");

    const ingestRes = await store.ingestEntry(
      otherStoreRes.entry,
      otherStoreRes.authToken,
    );

    assert(ingestRes.kind === "failure");
    assert(ingestRes.reason === "invalid_entry");
  });

  await test.step("Rejects entries with invalid auth tokens", async () => {
    const otherStore = new TestStore();
    const store = new TestStore();

    const otherStoreRes = await otherStore.set(
      {
        path: [new Uint8Array([0, 0, 0, 0])],
        payload: new Uint8Array(),
        subspace: alfie,
      },
      alfie,
    );

    assert(otherStoreRes.kind === "success");

    const badAuthorSigRes = await store.ingestEntry(
      otherStoreRes.entry,
      new Uint8Array([1, 2, 3]),
    );

    assert(badAuthorSigRes.kind === "failure");
    assert(badAuthorSigRes.reason === "invalid_entry");
  });

  // no ops entries for which there are newer entries with paths that are prefixes of that entry
  await test.step("Does not ingest entries for which there are new entries with paths which are a prefix", async () => {
    const store = new TestStore();

    await store.set(
      {
        path: [new Uint8Array([0, 0, 0, 0])],
        payload: new Uint8Array([0, 1, 2, 1]),
        timestamp: BigInt(2000),
        subspace: alfie,
      },
      alfie,
    );

    const secondRes = await store.set(
      {
        path: [new Uint8Array([0, 0, 0, 0]), new Uint8Array([1])],
        payload: new Uint8Array([0, 1, 2, 3]),
        timestamp: BigInt(1000),
        subspace: alfie,
      },
      alfie,
    );

    assert(secondRes.kind === "no_op");
    assert(secondRes.reason === "newer_prefix_found");
  });

  await test.step("Does not ingest entries for which there are newer entries with the same path and author", async () => {
    const store = new TestStore();

    await store.set(
      {
        path: [new Uint8Array([0, 0, 0, 0])],
        payload: new Uint8Array([0, 1, 2, 1]),
        timestamp: BigInt(2000),
        subspace: alfie,
      },
      alfie,
    );

    const secondRes = await store.set(
      {
        path: [new Uint8Array([0, 0, 0, 0])],
        payload: new Uint8Array([0, 1, 2, 3]),
        timestamp: BigInt(1000),
        subspace: alfie,
      },
      alfie,
    );

    assert(secondRes.kind === "no_op");
    assert(secondRes.reason === "obsolete_from_same_subspace");
  });

  await test.step("Does not ingest entries for which there are newer entries with the same path and author and timestamp but smaller hash", async () => {
    const store = new TestStore();

    await store.set(
      {
        path: [new Uint8Array([0, 0, 0, 0])],
        payload: new Uint8Array([0, 1, 2, 1]),
        timestamp: BigInt(2000),
        subspace: alfie,
      },
      alfie,
    );

    const secondRes = await store.set(
      {
        path: [new Uint8Array([0, 0, 0, 0])],
        payload: new Uint8Array([0, 1, 2, 3]),
        timestamp: BigInt(2000),
        subspace: alfie,
      },
      alfie,
    );

    assert(secondRes.kind === "no_op");
    assert(secondRes.reason === "obsolete_from_same_subspace");
  });

  await test.step({
    name:
      "Does not ingest entries for which there are newer entries with the same path and author and timestamp and hash but smaller payloadLength",
    fn: () => {
      // I don't really know how to test this path.
    },
    ignore: true,
  });

  await test.step("replaces older entries with same author and path", async () => {
    const store = new TestStore();

    await store.set(
      {
        path: [new Uint8Array([0, 0, 0, 0])],
        payload: new Uint8Array([0, 1, 2, 1]),
        timestamp: BigInt(1000),
        subspace: alfie,
      },
      alfie,
    );

    const secondRes = await store.set(
      {
        path: [new Uint8Array([0, 0, 0, 0])],
        payload: new Uint8Array([0, 1, 2, 3]),
        timestamp: BigInt(2000),
        subspace: alfie,
      },
      alfie,
    );

    assert(secondRes.kind === "success");

    const entries = [];

    for await (
      const entry of store.query({
        area: fullArea(),
        maxCount: 0,
        maxSize: BigInt(0),
      }, "path")
    ) {
      entries.push(entry);
    }

    assertEquals(entries.length, 1);
    assert(entries[0]);
    assert(entries[0][1]);
    assertEquals(await entries[0][1].bytes(), new Uint8Array([0, 1, 2, 3]));
  });

  await test.step("replaces older entries with paths prefixed by the new one", async () => {
    const store = new TestStore();

    await store.set(
      {
        path: [new Uint8Array([0]), new Uint8Array([1])],
        payload: new Uint8Array([0, 1, 2, 1]),
        timestamp: BigInt(0),
        subspace: alfie,
      },
      alfie,
    );

    await store.set(
      {
        path: [new Uint8Array([0]), new Uint8Array([2])],
        payload: new Uint8Array([0, 1, 2, 1]),
        timestamp: BigInt(0),
        subspace: alfie,
      },
      alfie,
    );

    const prefixRes = await store.set(
      {
        path: [new Uint8Array([0])],
        payload: new Uint8Array([0, 1, 2, 3]),
        timestamp: BigInt(1),
        subspace: alfie,
      },
      alfie,
    );

    assert(prefixRes.kind === "success");

    const entries = [];

    for await (
      const entry of store.query({
        area: fullArea(),
        maxCount: 0,
        maxSize: BigInt(0),
      }, "path")
    ) {
      entries.push(entry);
    }

    assertEquals(entries.length, 1);
    assert(entries[0]);
    assertEquals(entries[0][0].path, [new Uint8Array([0])]);
    assert(entries[0][1]);
    assertEquals(await entries[0][1].bytes(), new Uint8Array([0, 1, 2, 3]));
  });

  await test.step("replaces older entries with paths prefixed by the new one, EVEN when that entry was edited", async () => {
    const store = new TestStore();

    await store.set(
      {
        path: [new Uint8Array([0]), new Uint8Array([1])],
        payload: new Uint8Array([0, 1, 2, 1]),
        timestamp: BigInt(0),
        subspace: alfie,
      },
      alfie,
    );

    await store.set(
      {
        path: [new Uint8Array([0]), new Uint8Array([1])],
        payload: new Uint8Array([0, 1, 2, 3]),
        timestamp: BigInt(1),
        subspace: alfie,
      },
      alfie,
    );

    const prefixRes = await store.set(
      {
        path: [new Uint8Array([0])],
        payload: new Uint8Array([0, 1, 2, 3]),
        timestamp: BigInt(2),
        subspace: alfie,
      },
      alfie,
    );

    assert(prefixRes.kind === "success");

    const entries = [];

    for await (
      const entry of store.query({
        area: fullArea(),
        maxCount: 0,
        maxSize: BigInt(0),
      }, "path")
    ) {
      entries.push(entry);
    }

    assertEquals(entries.length, 1);
    assert(entries[0]);
    assertEquals(entries[0][0].path, [new Uint8Array([0])]);
    assert(entries[0][1]);
    assertEquals(await entries[0][1].bytes(), new Uint8Array([0, 1, 2, 3]));
  });
});

// ==================================
// ingestPayload

Deno.test("Store.ingestPayload", async (test) => {
  const alfie = TestSubspace.Alfie;
  const betty = TestSubspace.Betty;

  await test.step("does not ingest payload if corresponding entry is missing", async () => {
    const store = new TestStore();

    const res = await store.ingestPayload({
      path: [new Uint8Array([0])],
      subspace: TestSubspace.Gemma,
      timestamp: BigInt(0),
    }, new Blob([new Uint8Array()]).stream());

    assert(res.kind === "failure");
    assert(res.reason === "no_entry");
  });

  await test.step("does not ingest if payload is already held", async () => {
    const store = new TestStore();
    const otherStore = new TestStore();

    const payload = new Uint8Array(32);

    crypto.getRandomValues(payload);

    const res = await otherStore.set({
      path: [new Uint8Array([0, 2])],
      payload,
      subspace: alfie,
    }, alfie);

    assert(res.kind === "success");

    const res2 = await store.ingestEntry(res.entry, res.authToken);

    assert(res2.kind === "success");

    const res3 = await store.ingestPayload({
      path: res.entry.path,
      subspace: res.entry.subspaceId,
      timestamp: res.entry.timestamp,
    }, new Blob([new Uint8Array()]).stream());

    assert(res3.kind === "success");

    const res4 = await store.ingestPayload({
      path: res.entry.path,
      subspace: res.entry.subspaceId,
      timestamp: res.entry.timestamp,
    }, new Blob([payload]).stream());

    assert(res4.kind === "no_op");
  });

  await test.step("does not ingest if the hash doesn't match the entry's", async () => {
    const store = new TestStore();
    const otherStore = new TestStore();

    const payload = new Uint8Array(32);

    crypto.getRandomValues(payload);

    const res = await otherStore.set({
      path: [new Uint8Array([0, 2])],
      payload,
      subspace: alfie,
    }, alfie);

    assert(res.kind === "success");

    const res2 = await store.ingestEntry(res.entry, res.authToken);

    assert(res2.kind === "success");

    const res3 = await store.ingestPayload({
      path: res.entry.path,
      subspace: res.entry.subspaceId,
      timestamp: res.entry.timestamp,
    }, new Blob([new Uint8Array(32)]).stream());

    assert(res3.kind === "failure");
    assert(res3.reason === "data_mismatch");
  });

  await test.step("ingest if everything is valid", async () => {
    const store = new TestStore();
    const otherStore = new TestStore();

    const payload = new Uint8Array(32);

    crypto.getRandomValues(payload);

    const res = await otherStore.set({
      path: [new Uint8Array([0, 2])],
      payload,
      subspace: alfie,
    }, alfie);

    assert(res.kind === "success");

    const res2 = await store.ingestEntry(res.entry, res.authToken);

    assert(res2.kind === "success");

    const res3 = await store.ingestPayload({
      path: res.entry.path,
      subspace: res.entry.subspaceId,
      timestamp: res.entry.timestamp,
    }, new Blob([payload]).stream());

    assert(res3.kind === "success");

    let retrievedPayload;

    for await (
      const [_entry, payload] of store.query({
        area: fullArea(),
        maxCount: 0,
        maxSize: BigInt(0),
      }, "path")
    ) {
      retrievedPayload = await payload?.bytes();
    }

    assert(retrievedPayload);

    assert(orderBytes(payload, retrievedPayload) === 0);
  });
});

// ==================================
// query

// ==================================
// WAF

Deno.test("Write-ahead flags", async (test) => {
  const alfie = TestSubspace.Alfie;

  await test.step("Insertion flag inserts (and removes prefixes...)", async () => {
    const store = new TestStore();
    const otherStore = new TestStore();

    const res = await otherStore.set(
      {
        path: [new Uint8Array([0, 0, 0, 0])],
        payload: new Uint8Array(32),
        timestamp: BigInt(1000),
        subspace: alfie,
      },
      alfie,
    );

    assert(res.kind === "success");

    // Insert

    const result = await store.set(
      {
        path: [new Uint8Array([0, 0, 0, 0, 1])],
        payload: new Uint8Array(32),
        timestamp: BigInt(500),
        subspace: alfie,
      },
      alfie,
    );

    assert(result.kind === "success");

    await store.writeAheadFlag().flagInsertion(
      result.entry,
      result.authToken,
    );

    await store.triggerWriteAheadFlag();

    const entries = [];

    for await (
      const [entry] of store.query({
        area: fullArea(),
        maxCount: 0,
        maxSize: BigInt(0),
      }, "path")
    ) {
      entries.push(entry);
    }

    assertEquals(entries.length, 1);
    assert(entries[0]);

    assert(
      orderPath(
        entries[0].path,
        [new Uint8Array([0, 0, 0, 0, 1])],
      ) === 0,
    );
  });

  await test.step("Removal flag removes", async () => {
    const store = new TestStore();

    const res = await store.set(
      {
        path: [new Uint8Array([0, 0, 0, 0])],
        payload: new Uint8Array(32),
        timestamp: BigInt(1000),
        subspace: alfie,
      },
      alfie,
    );

    assert(res.kind === "success");

    await store.writeAheadFlag().flagRemoval(res.entry);

    await store.triggerWriteAheadFlag();

    const entries = [];

    for await (
      const [entry] of store.query({
        area: fullArea(),
        maxCount: 0,
        maxSize: BigInt(0),
      }, "path")
    ) {
      entries.push(entry);
    }

    assertEquals(entries.length, 0);
  });
});
