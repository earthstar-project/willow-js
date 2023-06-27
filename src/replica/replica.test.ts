import {
  assert,
  assertEquals,
} from "https://deno.land/std@0.177.0/testing/asserts.ts";
import { bytesConcat } from "../../deps.ts";
import {
  compareBytes,
  concatSummarisableStorageValue,
  entryKeyBytes,
} from "../util/bytes.ts";
import { Replica } from "./replica.ts";
import { crypto } from "https://deno.land/std@0.188.0/crypto/crypto.ts";
import { RadixishTree } from "./storage/prefix_iterators/radixish_tree.ts";
import { sha256XorMonoid } from "./storage/summarisable_storage/lifting_monoid.ts";
import { MonoidRbTree } from "./storage/summarisable_storage/monoid_rbtree.ts";
import { SummarisableStorage } from "./storage/summarisable_storage/types.ts";
import { EntryDriver } from "./storage/types.ts";

export class EntryDriverTest implements EntryDriver {
  private insertionFlag: [Uint8Array, Uint8Array] | undefined = undefined;
  private removalFlag: Uint8Array | undefined = undefined;

  createSummarisableStorage(): SummarisableStorage<Uint8Array, Uint8Array> {
    return new MonoidRbTree({
      monoid: sha256XorMonoid,
      compare: compareBytes,
    });
  }
  writeAheadFlag = {
    wasInserting: () => Promise.resolve(this.insertionFlag),
    wasRemoving: () => Promise.resolve(this.removalFlag),
    flagInsertion: (key: Uint8Array, value: Uint8Array) => {
      this.insertionFlag = [key, value];

      return Promise.resolve();
    },
    flagRemoval: (key: Uint8Array) => {
      this.removalFlag = key;

      return Promise.resolve();
    },
    unflagInsertion: () => {
      this.insertionFlag = undefined;

      return Promise.resolve();
    },
    unflagRemoval: () => {
      this.removalFlag = undefined;

      return Promise.resolve();
    },
  };
  prefixIterator = new RadixishTree<Uint8Array>();
}

class TestReplica extends Replica<
  { publicKey: Uint8Array; privateKey: Uint8Array }
> {
  constructor(namespace = new Uint8Array([1, 2, 3, 4])) {
    super({
      namespace,
      protocolParameters: {
        hashLength: 32,
        pubkeyLength: 4,
        signatureLength: 32,
        // We are just testing that things are signed / verified as expected
        // so use a very silly signing function here.
        sign: async (keypair, entryEncoded) => {
          if (compareBytes(keypair.publicKey, keypair.privateKey) !== 0) {
            const bytes = bytesConcat(
              keypair.publicKey,
              keypair.publicKey,
            );

            return new Uint8Array(await crypto.subtle.digest("SHA-256", bytes));
          }

          const bytes = bytesConcat(
            keypair.publicKey,
            new Uint8Array(entryEncoded),
          );

          return new Uint8Array(await crypto.subtle.digest("SHA-256", bytes));
        },
        hash: async (bytes: Uint8Array | ReadableStream<Uint8Array>) => {
          return new Uint8Array(await crypto.subtle.digest("SHA-256", bytes));
        },
        verify: async (
          publicKey,
          signature,
          encodedEntry,
        ) => {
          const bytes = bytesConcat(
            publicKey,
            encodedEntry,
          );

          const ours = new Uint8Array(
            await crypto.subtle.digest("SHA-256", bytes),
          );

          return compareBytes(ours, new Uint8Array(signature)) === 0;
        },
        pubkeyBytesFromPair(pair) {
          return Promise.resolve(pair.publicKey);
        },
      },
      entryDriver: new EntryDriverTest(),
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

Deno.test("Replica.set", async (test) => {
  const namespaceKeypair = {
    publicKey: new Uint8Array([1, 2, 3, 4]),
    privateKey: new Uint8Array([1, 2, 3, 4]),
  };

  const authorKeypair = {
    publicKey: new Uint8Array([5, 6, 7, 8]),
    privateKey: new Uint8Array([5, 6, 7, 8]),
  };

  const badNamespaceKeypair = {
    ...namespaceKeypair,
    privateKey: new Uint8Array([0, 0, 0, 0]),
  };

  await test.step("Fails with invalid ingestions", async () => {
    const replica = new TestReplica();

    // Returns an error and does not ingest payload if the entry is invalid
    const badKeypairRes = await replica.set(
      badNamespaceKeypair,
      authorKeypair,
      {
        path: new Uint8Array([1, 2, 3, 4]),
        payload: new Uint8Array([1, 1, 1, 1]),
      },
    );

    assert(badKeypairRes.kind === "failure");
    assertEquals(badKeypairRes.reason, "invalid_entry");

    const entries = [];

    for await (const entry of replica.query({ order: "path" })) {
      entries.push(entry);
    }

    assertEquals(entries, []);
  });

  await test.step("Succeeds with valid ingestions", async () => {
    const replica = new TestReplica();

    const goodKeypairRes = await replica.set(
      namespaceKeypair,
      authorKeypair,
      {
        path: new Uint8Array([1, 2, 3, 4]),
        payload: new Uint8Array([1, 1, 1, 1]),
      },
    );

    assertEquals(goodKeypairRes.kind, "success");

    const entries = [];

    for await (const entry of replica.query({ order: "path" })) {
      entries.push(entry);
    }

    assert(entries[0]);
    assert(entries[0][1]);
  });

  await test.step("If a timestamp is set, it is used", async () => {
    const replica = new TestReplica();

    const res = await replica.set(
      namespaceKeypair,
      authorKeypair,
      {
        path: new Uint8Array([1, 2, 3, 4]),
        payload: new Uint8Array([1, 1, 1, 1]),
        timestamp: BigInt(0),
      },
    );

    assert(res.kind === "success");
    assertEquals(res.signed.entry.record.timestamp, BigInt(0));
  });

  await test.step("If no timestamp is set, and there is nothing else at the same path, use the current time.", async () => {
    const replica = new TestReplica();

    const timestampBefore = BigInt(Date.now() * 1000);

    const res = await replica.set(
      namespaceKeypair,
      authorKeypair,
      {
        path: new Uint8Array([1, 2, 3, 4]),
        payload: new Uint8Array([1, 1, 1, 1]),
      },
    );

    assert(res.kind === "success");
    assert(res.signed.entry.record.timestamp >= timestampBefore);
    assert(res.signed.entry.record.timestamp < BigInt(Date.now() * 1000));
  });

  await test.step("If no timestamp is set, and there is something else at the same path, the timestamp is that timestamp + 1", async () => {
    const replica = new TestReplica();

    const first = await replica.set(
      namespaceKeypair,
      {
        publicKey: new Uint8Array([7, 8, 9, 10]),
        privateKey: new Uint8Array([7, 8, 9, 10]),
      },
      {
        path: new Uint8Array([1, 2, 3, 4]),
        payload: new Uint8Array([2, 2, 2, 2]),
      },
    );

    const second = await replica.set(
      namespaceKeypair,
      authorKeypair,
      {
        path: new Uint8Array([1, 2, 3, 4]),
        payload: new Uint8Array([1, 1, 1, 1]),
      },
    );

    assert(first.kind === "success");
    assert(second.kind === "success");
    assertEquals(
      second.signed.entry.record.timestamp,
      first.signed.entry.record.timestamp + BigInt(1),
    );
  });

  // if a timestamp is set,
});

// ==================================
// ingestEntry

Deno.test("Replica.ingestEntry", async (test) => {
  const namespaceKeypair = {
    publicKey: new Uint8Array([1, 2, 3, 4]),
    privateKey: new Uint8Array([1, 2, 3, 4]),
  };

  const authorKeypair = {
    publicKey: new Uint8Array([5, 6, 7, 8]),
    privateKey: new Uint8Array([5, 6, 7, 8]),
  };

  // rejects stuff from a different namespace
  await test.step("Rejects entries from a different namespace", async () => {
    const otherReplica = new TestReplica(new Uint8Array([9, 9, 9, 9]));
    const replica = new TestReplica();

    const otherReplicaRes = await otherReplica.set(
      {
        publicKey: new Uint8Array([9, 9, 9, 9]),
        privateKey: new Uint8Array([9, 9, 9, 9]),
      },
      authorKeypair,
      {
        path: new Uint8Array([0, 0, 0, 0]),
        payload: new Uint8Array(),
      },
    );

    assert(otherReplicaRes.kind === "success");

    const ingestRes = await replica.ingestEntry(otherReplicaRes.signed);

    assert(ingestRes.kind === "failure");
    assert(ingestRes.reason === "invalid_entry");
  });

  await test.step("Rejects entries with bad signatures", async () => {
    const otherReplica = new TestReplica();
    const replica = new TestReplica();

    const otherReplicaRes = await otherReplica.set(
      namespaceKeypair,
      authorKeypair,
      {
        path: new Uint8Array([0, 0, 0, 0]),
        payload: new Uint8Array(),
      },
    );

    assert(otherReplicaRes.kind === "success");

    const entryBadAuthorSignature = {
      ...otherReplicaRes.signed,
      authorSignature: new Uint8Array(32),
    };

    const badAuthorSigRes = await replica.ingestEntry(entryBadAuthorSignature);

    assert(badAuthorSigRes.kind === "failure");
    assert(badAuthorSigRes.reason === "invalid_entry");

    const entryBadNamespaceSignature = {
      ...otherReplicaRes.signed,
      namespaceSignature: new Uint8Array(32),
    };

    const badNamespaceSigRes = await replica.ingestEntry(
      entryBadNamespaceSignature,
    );

    assert(badNamespaceSigRes.kind === "failure");
    assert(badNamespaceSigRes.reason === "invalid_entry");
  });

  // no ops entries for which there are newer entries with paths that are prefixes of that entry
  await test.step("Does not ingest entries for which there are new entries with paths which are a prefix", async () => {
    const replica = new TestReplica();

    await replica.set(
      namespaceKeypair,
      authorKeypair,
      {
        path: new Uint8Array([0, 0, 0, 0]),
        payload: new Uint8Array([0, 1, 2, 1]),
        timestamp: BigInt(2000),
      },
    );

    const secondRes = await replica.set(
      namespaceKeypair,
      authorKeypair,
      {
        path: new Uint8Array([0, 0, 0, 0, 1]),
        payload: new Uint8Array([0, 1, 2, 3]),
        timestamp: BigInt(1000),
      },
    );

    assert(secondRes.kind === "no_op");
    assert(secondRes.reason === "newer_prefix_found");
  });

  await test.step("Does not ingest entries for which there are newer entries with the same path and author", async () => {
    const replica = new TestReplica();

    await replica.set(
      namespaceKeypair,
      authorKeypair,
      {
        path: new Uint8Array([0, 0, 0, 0]),
        payload: new Uint8Array([0, 1, 2, 1]),
        timestamp: BigInt(2000),
      },
    );

    const secondRes = await replica.set(
      namespaceKeypair,
      authorKeypair,
      {
        path: new Uint8Array([0, 0, 0, 0]),
        payload: new Uint8Array([0, 1, 2, 3]),
        timestamp: BigInt(1000),
      },
    );

    assert(secondRes.kind === "no_op");
    assert(secondRes.reason === "obsolete_from_same_author");
  });

  await test.step("Does not ingest entries for which there are newer entries with the same path and author and timestamp but smaller hash", async () => {
    const replica = new TestReplica();

    await replica.set(
      namespaceKeypair,
      authorKeypair,
      {
        path: new Uint8Array([0, 0, 0, 0]),
        payload: new Uint8Array([0, 1, 2, 1]),
        timestamp: BigInt(2000),
      },
    );

    const secondRes = await replica.set(
      namespaceKeypair,
      authorKeypair,
      {
        path: new Uint8Array([0, 0, 0, 0]),
        payload: new Uint8Array([0, 1, 2, 3]),
        timestamp: BigInt(2000),
      },
    );

    assert(secondRes.kind === "no_op");
    assert(secondRes.reason === "obsolete_from_same_author");
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
    const replica = new TestReplica();

    await replica.set(
      namespaceKeypair,
      authorKeypair,
      {
        path: new Uint8Array([0, 0, 0, 0]),
        payload: new Uint8Array([0, 1, 2, 1]),
      },
    );

    const secondRes = await replica.set(
      namespaceKeypair,
      authorKeypair,
      {
        path: new Uint8Array([0, 0, 0, 0]),
        payload: new Uint8Array([0, 1, 2, 3]),
      },
    );

    assert(secondRes.kind === "success");

    const entries = [];

    for await (const entry of replica.query({ order: "path" })) {
      entries.push(entry);
    }

    assertEquals(entries.length, 1);
    assert(entries[0]);
    assert(entries[0][1]);
    assertEquals(await entries[0][1].bytes(), new Uint8Array([0, 1, 2, 3]));
  });

  await test.step("replaces older entries with paths prefixed by the new one", async () => {
    const replica = new TestReplica();

    await replica.set(
      namespaceKeypair,
      authorKeypair,
      {
        path: new Uint8Array([0, 1]),
        payload: new Uint8Array([0, 1, 2, 1]),
      },
    );

    await replica.set(
      namespaceKeypair,
      authorKeypair,
      {
        path: new Uint8Array([0, 2]),
        payload: new Uint8Array([0, 1, 2, 1]),
      },
    );

    const prefixRes = await replica.set(
      namespaceKeypair,
      authorKeypair,
      {
        path: new Uint8Array([0]),
        payload: new Uint8Array([0, 1, 2, 3]),
      },
    );

    assert(prefixRes.kind === "success");

    const entries = [];

    for await (const entry of replica.query({ order: "path" })) {
      entries.push(entry);
    }

    assertEquals(entries.length, 1);
    assert(entries[0]);
    assertEquals(entries[0][0].entry.identifier.path, new Uint8Array([0]));
    assert(entries[0][1]);
    assertEquals(await entries[0][1].bytes(), new Uint8Array([0, 1, 2, 3]));
  });
});

// ==================================
// ingestPayload

Deno.test("Replica.ingestPayload", async (test) => {
  const namespaceKeypair = {
    publicKey: new Uint8Array([1, 2, 3, 4]),
    privateKey: new Uint8Array([1, 2, 3, 4]),
  };

  const authorKeypair = {
    publicKey: new Uint8Array([5, 6, 7, 8]),
    privateKey: new Uint8Array([5, 6, 7, 8]),
  };

  await test.step("does not ingest payload if corresponding entry is missing", async () => {
    const replica = new TestReplica();

    const res = await replica.ingestPayload({
      path: new Uint8Array([0]),
      author: new Uint8Array([0]),
      timestamp: BigInt(0),
    }, new Uint8Array());

    assert(res.kind === "failure");
    assert(res.reason === "no_entry");
  });

  await test.step("does not ingest if payload is already held", async () => {
    const replica = new TestReplica();
    const otherReplica = new TestReplica();

    const payload = new Uint8Array(32);

    crypto.getRandomValues(payload);

    const res = await otherReplica.set(namespaceKeypair, authorKeypair, {
      path: new Uint8Array([0, 2]),
      payload,
    });

    assert(res.kind === "success");

    const res2 = await replica.ingestEntry(res.signed);

    assert(res2.kind === "success");

    const res3 = await replica.ingestPayload({
      path: new Uint8Array(res.signed.entry.identifier.path),
      author: new Uint8Array(res.signed.entry.identifier.author),
      timestamp: res.signed.entry.record.timestamp,
    }, payload);

    assert(res3.kind === "success");

    const res4 = await replica.ingestPayload({
      path: new Uint8Array(res.signed.entry.identifier.path),
      author: new Uint8Array(res.signed.entry.identifier.author),
      timestamp: res.signed.entry.record.timestamp,
    }, payload);

    assert(res4.kind === "no_op");
  });

  await test.step("does not ingest if the hash doesn't match the entry's", async () => {
    const replica = new TestReplica();
    const otherReplica = new TestReplica();

    const payload = new Uint8Array(32);

    crypto.getRandomValues(payload);

    const res = await otherReplica.set(namespaceKeypair, authorKeypair, {
      path: new Uint8Array([0, 2]),
      payload,
    });

    assert(res.kind === "success");

    const res2 = await replica.ingestEntry(res.signed);

    assert(res2.kind === "success");

    const res3 = await replica.ingestPayload({
      path: new Uint8Array(res.signed.entry.identifier.path),
      author: new Uint8Array(res.signed.entry.identifier.author),
      timestamp: res.signed.entry.record.timestamp,
    }, new Uint8Array(32));

    assert(res3.kind === "failure");
    assert(res3.reason === "mismatched_hash");
  });

  await test.step("ingest if everything is valid", async () => {
    const replica = new TestReplica();
    const otherReplica = new TestReplica();

    const payload = new Uint8Array(32);

    crypto.getRandomValues(payload);

    const res = await otherReplica.set(namespaceKeypair, authorKeypair, {
      path: new Uint8Array([0, 2]),
      payload,
    });

    assert(res.kind === "success");

    const res2 = await replica.ingestEntry(res.signed);

    assert(res2.kind === "success");

    const res3 = await replica.ingestPayload({
      path: new Uint8Array(res.signed.entry.identifier.path),
      author: new Uint8Array(res.signed.entry.identifier.author),
      timestamp: res.signed.entry.record.timestamp,
    }, payload);

    assert(res3.kind === "success");

    let retrievedPayload;

    for await (const [_entry, payload] of replica.query({ order: "path" })) {
      retrievedPayload = await payload?.bytes();
    }

    assert(retrievedPayload);

    assert(compareBytes(payload, retrievedPayload) === 0);
  });
});

// ==================================
// query

// ==================================
// WAF

Deno.test("Write-ahead flags", async (test) => {
  const namespaceKeypair = {
    publicKey: new Uint8Array([1, 2, 3, 4]),
    privateKey: new Uint8Array([1, 2, 3, 4]),
  };

  const authorKeypair = {
    publicKey: new Uint8Array([5, 6, 7, 8]),
    privateKey: new Uint8Array([5, 6, 7, 8]),
  };

  await test.step("Insertion flag inserts (and removes prefixes...)", async () => {
    const replica = new TestReplica();
    const otherReplica = new TestReplica();

    const res = await otherReplica.set(
      namespaceKeypair,
      authorKeypair,
      {
        path: new Uint8Array([0, 0, 0, 0]),
        payload: new Uint8Array(32),
        timestamp: BigInt(1000),
      },
    );

    assert(res.kind === "success");

    // Create PTA flag.
    const keys = entryKeyBytes(
      new Uint8Array(res.signed.entry.identifier.path),
      res.signed.entry.record.timestamp,
      new Uint8Array(res.signed.entry.identifier.author),
    );

    // Create storage value.
    const storageValue = concatSummarisableStorageValue({
      payloadHash: res.signed.entry.record.hash,
      payloadLength: res.signed.entry.record.length,
      authorSignature: res.signed.authorSignature,
      namespaceSignature: res.signed.namespaceSignature,
    });

    // Insert

    await replica.set(
      namespaceKeypair,
      authorKeypair,
      {
        path: new Uint8Array([0, 0, 0, 0, 1]),
        payload: new Uint8Array(32),
        timestamp: BigInt(500),
      },
    );
    await replica.writeAheadFlag().flagInsertion(keys.pta, storageValue);

    await replica.triggerWriteAheadFlag();

    const entries = [];

    for await (const [entry] of replica.query({ order: "path" })) {
      entries.push(entry);
    }

    assertEquals(entries.length, 1);
    assert(entries[0]);
    assert(
      compareBytes(
        new Uint8Array(entries[0].entry.identifier.path),
        new Uint8Array([0, 0, 0, 0]),
      ) === 0,
    );
  });

  await test.step("Removal flag removes", async () => {
    const replica = new TestReplica();

    const res = await replica.set(
      namespaceKeypair,
      authorKeypair,
      {
        path: new Uint8Array([0, 0, 0, 0]),
        payload: new Uint8Array(32),
        timestamp: BigInt(1000),
      },
    );

    assert(res.kind === "success");

    // Create PTA flag.
    const keys = entryKeyBytes(
      new Uint8Array(res.signed.entry.identifier.path),
      res.signed.entry.record.timestamp,
      new Uint8Array(res.signed.entry.identifier.author),
    );

    await replica.writeAheadFlag().flagRemoval(keys.pta);

    await replica.triggerWriteAheadFlag();

    const entries = [];

    for await (const [entry] of replica.query({ order: "path" })) {
      entries.push(entry);
    }

    assertEquals(entries.length, 0);
  });
});
