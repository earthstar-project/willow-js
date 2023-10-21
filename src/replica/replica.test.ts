import {
  assert,
  assertEquals,
} from "https://deno.land/std@0.177.0/testing/asserts.ts";
import { compareBytes } from "../util/bytes.ts";
import { Replica } from "./replica.ts";
import { crypto } from "https://deno.land/std@0.188.0/crypto/crypto.ts";
import { encodeEntry } from "../entries/encode_decode.ts";
import { encodeEntryKeys, encodeSummarisableStorageValue } from "./util.ts";
import { equalsBytes, Products } from "../../deps.ts";
import {
  FingerprintScheme,
  NamespaceScheme,
  PathLengthScheme,
  PayloadScheme,
  SubspaceScheme,
} from "./types.ts";

async function makeKeypair() {
  const { publicKey, privateKey } = await crypto.subtle.generateKey(
    {
      name: "ECDSA",
      namedCurve: "P-256",
    },
    true,
    ["sign", "verify"],
  );

  return {
    subspace: new Uint8Array(
      await window.crypto.subtle.exportKey("raw", publicKey),
    ),
    privateKey,
  };
}

function importPublicKey(raw: ArrayBuffer) {
  return crypto.subtle.importKey(
    "raw",
    raw,
    {
      name: "ECDSA",
      namedCurve: "P-256",
    },
    true,
    ["verify"],
  );
}

const testSchemeNamespace: NamespaceScheme<Uint8Array> = {
  encode: (v) => v,
  decode: (v) => v,
  encodedLength: (v) => v.byteLength,
  isEqual: equalsBytes,
};

const testSchemeSubspace: SubspaceScheme<Uint8Array> = {
  encode: (v) => v,
  decode: (v) => v.subarray(0, 65),
  encodedLength: () => 65,
  isEqual: equalsBytes,
  minimalSubspaceKey: new Uint8Array(65),
  order: Products.orderPaths,
  successor: Products.makeSuccessorPath(65),
};

const testSchemePathLength: PathLengthScheme = {
  encode(length) {
    return new Uint8Array([length]);
  },
  decode(bytes) {
    return bytes[0];
  },
  encodedLength() {
    return 1;
  },
  maxLength: 8,
};

const testSchemePayload: PayloadScheme<ArrayBuffer> = {
  encode(hash) {
    return new Uint8Array(hash);
  },
  decode(bytes) {
    return bytes.subarray(0, 32);
  },
  encodedLength() {
    return 32;
  },
  async fromBytes(bytes) {
    return new Uint8Array(await crypto.subtle.digest("SHA-256", bytes));
  },
  order(a, b) {
    return compareBytes(new Uint8Array(a), new Uint8Array(b)) as
      | 1
      | 0
      | -1;
  },
};

const testSchemeFingerprint: FingerprintScheme<
  Uint8Array,
  Uint8Array,
  Uint8Array,
  Uint8Array
> = {
  neutral: new Uint8Array(32),
  async fingerprintSingleton(entry) {
    const encodedEntry = encodeEntry(entry, {
      namespaceScheme: testSchemeNamespace,
      subspaceScheme: testSchemeSubspace,
      pathLengthScheme: testSchemePathLength,
      payloadScheme: testSchemePayload,
    });

    return new Uint8Array(await crypto.subtle.digest("SHA-256", encodedEntry));
  },
  fingerprintCombine(a, b) {
    const bytes = new Uint8Array(32);

    for (let i = 0; i < 32; i++) {
      bytes.set([a[i] ^ b[i]], i);
    }

    return bytes;
  },
};

class TestReplica extends Replica<
  Uint8Array,
  Uint8Array,
  ArrayBuffer,
  CryptoKey,
  ArrayBuffer,
  Uint8Array
> {
  constructor(namespace = new Uint8Array([1, 2, 3, 4])) {
    super({
      namespace,
      protocolParameters: {
        namespaceScheme: testSchemeNamespace,
        subspaceScheme: testSchemeSubspace,
        pathLengthScheme: testSchemePathLength,
        payloadScheme: testSchemePayload,
        authorisationScheme: {
          async authorise(entry, secretKey) {
            const encodedEntry = encodeEntry(entry, {
              namespaceScheme: testSchemeNamespace,
              subspaceScheme: testSchemeSubspace,
              pathLengthScheme: testSchemePathLength,
              payloadScheme: testSchemePayload,
            });

            const res = await crypto.subtle.sign(
              {
                name: "ECDSA",
                hash: { name: "SHA-256" },
              },
              secretKey,
              encodedEntry,
            );

            return new Uint8Array(res);
          },
          async isAuthorised(entry, token) {
            const cryptoKey = await importPublicKey(entry.identifier.subspace);

            const encodedEntry = encodeEntry(entry, {
              namespaceScheme: testSchemeNamespace,
              subspaceScheme: testSchemeSubspace,
              pathLengthScheme: testSchemePathLength,
              payloadScheme: testSchemePayload,
            });

            return crypto.subtle.verify(
              {
                name: "ECDSA",
                hash: { name: "SHA-256" },
              },
              cryptoKey,
              token,
              encodedEntry,
            );
          },
          tokenEncoding: {
            encode: (ab) => new Uint8Array(ab),
            decode: (bytes) => bytes.buffer,
            encodedLength: (ab) => ab.byteLength,
          },
        },
        fingerprintScheme: testSchemeFingerprint,
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

Deno.test("Replica.set", async (test) => {
  const authorKeypair = await makeKeypair();
  const author2Keypair = await makeKeypair();

  await test.step("Fails with invalid ingestions", async () => {
    const replica = new TestReplica();

    // Returns an error and does not ingest payload if the entry is invalid
    const badKeypairRes = await replica.set(
      {
        path: new Uint8Array([1, 2, 3, 4]),
        payload: new Uint8Array([1, 1, 1, 1]),
        subspace: authorKeypair.subspace,
      },
      author2Keypair.privateKey,
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
      {
        path: new Uint8Array([1, 2, 3, 4]),
        payload: new Uint8Array([1, 1, 1, 1]),
        subspace: authorKeypair.subspace,
      },
      authorKeypair.privateKey,
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
      {
        path: new Uint8Array([1, 2, 3, 4]),
        payload: new Uint8Array([1, 1, 1, 1]),
        timestamp: BigInt(0),
        subspace: authorKeypair.subspace,
      },
      authorKeypair.privateKey,
    );

    assert(res.kind === "success");
    assertEquals(res.entry.record.timestamp, BigInt(0));
  });

  await test.step("If no timestamp is set, and there is nothing else at the same path, use the current time.", async () => {
    const replica = new TestReplica();

    const timestampBefore = BigInt(Date.now() * 1000);

    const res = await replica.set(
      {
        path: new Uint8Array([1, 2, 3, 4]),
        payload: new Uint8Array([1, 1, 1, 1]),
        subspace: authorKeypair.subspace,
      },
      authorKeypair.privateKey,
    );

    assert(res.kind === "success");
    assert(res.entry.record.timestamp >= timestampBefore);
    assert(res.entry.record.timestamp <= BigInt(Date.now() * 1000));
  });

  // if a timestamp is set,
});

// ==================================
// ingestEntry

Deno.test("Replica.ingestEntry", async (test) => {
  const authorKeypair = await makeKeypair();
  const author2Keypair = await makeKeypair();

  // rejects stuff from a different namespace
  await test.step("Rejects entries from a different namespace", async () => {
    const otherReplica = new TestReplica(new Uint8Array([9, 9, 9, 9]));
    const replica = new TestReplica();

    const otherReplicaRes = await otherReplica.set(
      {
        path: new Uint8Array([0, 0, 0, 0]),
        payload: new Uint8Array(),
        subspace: authorKeypair.subspace,
      },
      authorKeypair.privateKey,
    );

    assert(otherReplicaRes.kind === "success");

    const ingestRes = await replica.ingestEntry(
      otherReplicaRes.entry,
      otherReplicaRes.authToken,
    );

    assert(ingestRes.kind === "failure");
    assert(ingestRes.reason === "invalid_entry");
  });

  await test.step("Rejects entries with bad signatures", async () => {
    const otherReplica = new TestReplica();
    const replica = new TestReplica();

    const otherReplicaRes = await otherReplica.set(
      {
        path: new Uint8Array([0, 0, 0, 0]),
        payload: new Uint8Array(),
        subspace: authorKeypair.subspace,
      },
      authorKeypair.privateKey,
    );

    assert(otherReplicaRes.kind === "success");

    const badAuthorSigRes = await replica.ingestEntry(
      otherReplicaRes.entry,
      new Uint8Array([1, 2, 3]).buffer,
    );

    assert(badAuthorSigRes.kind === "failure");
    assert(badAuthorSigRes.reason === "invalid_entry");
  });

  // no ops entries for which there are newer entries with paths that are prefixes of that entry
  await test.step("Does not ingest entries for which there are new entries with paths which are a prefix", async () => {
    const replica = new TestReplica();

    await replica.set(
      {
        path: new Uint8Array([0, 0, 0, 0]),
        payload: new Uint8Array([0, 1, 2, 1]),
        timestamp: BigInt(2000),
        subspace: authorKeypair.subspace,
      },
      authorKeypair.privateKey,
    );

    const secondRes = await replica.set(
      {
        path: new Uint8Array([0, 0, 0, 0, 1]),
        payload: new Uint8Array([0, 1, 2, 3]),
        timestamp: BigInt(1000),
        subspace: authorKeypair.subspace,
      },
      authorKeypair.privateKey,
    );

    assert(secondRes.kind === "no_op");
    assert(secondRes.reason === "newer_prefix_found");
  });

  await test.step("Does not ingest entries for which there are newer entries with the same path and author", async () => {
    const replica = new TestReplica();

    await replica.set(
      {
        path: new Uint8Array([0, 0, 0, 0]),
        payload: new Uint8Array([0, 1, 2, 1]),
        timestamp: BigInt(2000),
        subspace: authorKeypair.subspace,
      },
      authorKeypair.privateKey,
    );

    const secondRes = await replica.set(
      {
        path: new Uint8Array([0, 0, 0, 0]),
        payload: new Uint8Array([0, 1, 2, 3]),
        timestamp: BigInt(1000),
        subspace: authorKeypair.subspace,
      },
      authorKeypair.privateKey,
    );

    assert(secondRes.kind === "no_op");
    assert(secondRes.reason === "obsolete_from_same_subspace");
  });

  await test.step("Does not ingest entries for which there are newer entries with the same path and author and timestamp but smaller hash", async () => {
    const replica = new TestReplica();

    await replica.set(
      {
        path: new Uint8Array([0, 0, 0, 0]),
        payload: new Uint8Array([0, 1, 2, 1]),
        timestamp: BigInt(2000),
        subspace: authorKeypair.subspace,
      },
      authorKeypair.privateKey,
    );

    const secondRes = await replica.set(
      {
        path: new Uint8Array([0, 0, 0, 0]),
        payload: new Uint8Array([0, 1, 2, 3]),
        timestamp: BigInt(2000),
        subspace: authorKeypair.subspace,
      },
      authorKeypair.privateKey,
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
    const replica = new TestReplica();

    await replica.set(
      {
        path: new Uint8Array([0, 0, 0, 0]),
        payload: new Uint8Array([0, 1, 2, 1]),
        timestamp: BigInt(1000),
        subspace: authorKeypair.subspace,
      },
      authorKeypair.privateKey,
    );

    const secondRes = await replica.set(
      {
        path: new Uint8Array([0, 0, 0, 0]),
        payload: new Uint8Array([0, 1, 2, 3]),
        timestamp: BigInt(2000),
        subspace: authorKeypair.subspace,
      },
      authorKeypair.privateKey,
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
      {
        path: new Uint8Array([0, 1]),
        payload: new Uint8Array([0, 1, 2, 1]),
        timestamp: BigInt(0),
        subspace: authorKeypair.subspace,
      },
      authorKeypair.privateKey,
    );

    await replica.set(
      {
        path: new Uint8Array([0, 2]),
        payload: new Uint8Array([0, 1, 2, 1]),
        timestamp: BigInt(0),
        subspace: authorKeypair.subspace,
      },
      authorKeypair.privateKey,
    );

    const prefixRes = await replica.set(
      {
        path: new Uint8Array([0]),
        payload: new Uint8Array([0, 1, 2, 3]),
        timestamp: BigInt(1),
        subspace: authorKeypair.subspace,
      },
      authorKeypair.privateKey,
    );

    assert(prefixRes.kind === "success");

    const entries = [];

    for await (const entry of replica.query({ order: "path" })) {
      entries.push(entry);
    }

    assertEquals(entries.length, 1);
    assert(entries[0]);
    assertEquals(entries[0][0].identifier.path, new Uint8Array([0]));
    assert(entries[0][1]);
    assertEquals(await entries[0][1].bytes(), new Uint8Array([0, 1, 2, 3]));
  });

  await test.step("replaces older entries with paths prefixed by the new one, EVEN when that entry was edited", async () => {
    const replica = new TestReplica();

    await replica.set(
      {
        path: new Uint8Array([0, 1]),
        payload: new Uint8Array([0, 1, 2, 1]),
        timestamp: BigInt(0),
        subspace: authorKeypair.subspace,
      },
      authorKeypair.privateKey,
    );

    await replica.set(
      {
        path: new Uint8Array([0, 1]),
        payload: new Uint8Array([0, 1, 2, 3]),
        timestamp: BigInt(1),
        subspace: authorKeypair.subspace,
      },
      authorKeypair.privateKey,
    );

    const prefixRes = await replica.set(
      {
        path: new Uint8Array([0]),
        payload: new Uint8Array([0, 1, 2, 3]),
        timestamp: BigInt(2),
        subspace: authorKeypair.subspace,
      },
      authorKeypair.privateKey,
    );

    assert(prefixRes.kind === "success");

    const entries = [];

    for await (const entry of replica.query({ order: "path" })) {
      entries.push(entry);
    }

    assertEquals(entries.length, 1);
    assert(entries[0]);
    assertEquals(entries[0][0].identifier.path, new Uint8Array([0]));
    assert(entries[0][1]);
    assertEquals(await entries[0][1].bytes(), new Uint8Array([0, 1, 2, 3]));
  });
});

// ==================================
// ingestPayload

Deno.test("Replica.ingestPayload", async (test) => {
  const authorKeypair = await makeKeypair();

  await test.step("does not ingest payload if corresponding entry is missing", async () => {
    const replica = new TestReplica();

    const res = await replica.ingestPayload({
      path: new Uint8Array([0]),
      subspace: new Uint8Array([0]),
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

    const res = await otherReplica.set({
      path: new Uint8Array([0, 2]),
      payload,
      subspace: authorKeypair.subspace,
    }, authorKeypair.privateKey);

    assert(res.kind === "success");

    const res2 = await replica.ingestEntry(res.entry, res.authToken);

    assert(res2.kind === "success");

    const res3 = await replica.ingestPayload({
      path: res.entry.identifier.path,
      subspace: res.entry.identifier.subspace,
      timestamp: res.entry.record.timestamp,
    }, payload);

    assert(res3.kind === "success");

    const res4 = await replica.ingestPayload({
      path: new Uint8Array(res.entry.identifier.path),
      subspace: new Uint8Array(res.entry.identifier.subspace),
      timestamp: res.entry.record.timestamp,
    }, payload);

    assert(res4.kind === "no_op");
  });

  await test.step("does not ingest if the hash doesn't match the entry's", async () => {
    const replica = new TestReplica();
    const otherReplica = new TestReplica();

    const payload = new Uint8Array(32);

    crypto.getRandomValues(payload);

    const res = await otherReplica.set({
      path: new Uint8Array([0, 2]),
      payload,
      subspace: authorKeypair.subspace,
    }, authorKeypair.privateKey);

    assert(res.kind === "success");

    const res2 = await replica.ingestEntry(res.entry, res.authToken);

    assert(res2.kind === "success");

    const res3 = await replica.ingestPayload({
      path: new Uint8Array(res.entry.identifier.path),
      subspace: new Uint8Array(res.entry.identifier.subspace),
      timestamp: res.entry.record.timestamp,
    }, new Uint8Array(32));

    assert(res3.kind === "failure");
    assert(res3.reason === "mismatched_hash");
  });

  await test.step("ingest if everything is valid", async () => {
    const replica = new TestReplica();
    const otherReplica = new TestReplica();

    const payload = new Uint8Array(32);

    crypto.getRandomValues(payload);

    const res = await otherReplica.set({
      path: new Uint8Array([0, 2]),
      payload,
      subspace: authorKeypair.subspace,
    }, authorKeypair.privateKey);

    assert(res.kind === "success");

    const res2 = await replica.ingestEntry(res.entry, res.authToken);

    assert(res2.kind === "success");

    const res3 = await replica.ingestPayload({
      path: new Uint8Array(res.entry.identifier.path),
      subspace: new Uint8Array(res.entry.identifier.subspace),
      timestamp: res.entry.record.timestamp,
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
  const authorKeypair = await makeKeypair();

  await test.step("Insertion flag inserts (and removes prefixes...)", async () => {
    const replica = new TestReplica();
    const otherReplica = new TestReplica();

    const res = await otherReplica.set(
      {
        path: new Uint8Array([0, 0, 0, 0]),
        payload: new Uint8Array(32),
        timestamp: BigInt(1000),
        subspace: authorKeypair.subspace,
      },
      authorKeypair.privateKey,
    );

    assert(res.kind === "success");

    // Create PTA flag.
    const keys = encodeEntryKeys(
      {
        path: new Uint8Array(res.entry.identifier.path),
        timestamp: res.entry.record.timestamp,
        subspace: new Uint8Array(res.entry.identifier.subspace),

        subspaceEncoding: {
          encode: (v) => v,
          decode: (v) => v.subarray(0, 65),
          encodedLength: () => 65,
        },
      },
    );

    // Create storage value.
    const storageValue = encodeSummarisableStorageValue({
      payloadHash: res.entry.record.hash,
      payloadLength: res.entry.record.length,
      authTokenHash: new Uint8Array(
        await crypto.subtle.digest("SHA-256", res.authToken),
      ),
      payloadScheme: testSchemePayload,
      pathLength: res.entry.identifier.path.byteLength,
      pathLengthEncoding: {
        encode(length) {
          return new Uint8Array([length]);
        },
        decode(bytes) {
          return bytes[0];
        },
        encodedLength() {
          return 1;
        },
      },
    });

    // Insert

    const result = await replica.set(
      {
        path: new Uint8Array([0, 0, 0, 0, 1]),
        payload: new Uint8Array(32),
        timestamp: BigInt(500),
        subspace: authorKeypair.subspace,
      },
      authorKeypair.privateKey,
    );

    assert(result.kind === "success");

    await replica.writeAheadFlag().flagInsertion(
      result.entry,
      result.authToken,
    );

    await replica.triggerWriteAheadFlag();

    const entries = [];

    for await (const [entry] of replica.query({ order: "path" })) {
      entries.push(entry);
    }

    assertEquals(entries.length, 1);
    assert(entries[0]);

    assert(
      compareBytes(
        new Uint8Array(entries[0].identifier.path),
        new Uint8Array([0, 0, 0, 0, 1]),
      ) === 0,
    );
  });

  await test.step("Removal flag removes", async () => {
    const replica = new TestReplica();

    const res = await replica.set(
      {
        path: new Uint8Array([0, 0, 0, 0]),
        payload: new Uint8Array(32),
        timestamp: BigInt(1000),
        subspace: authorKeypair.subspace,
      },
      authorKeypair.privateKey,
    );

    assert(res.kind === "success");

    await replica.writeAheadFlag().flagRemoval(res.entry);

    await replica.triggerWriteAheadFlag();

    const entries = [];

    for await (const [entry] of replica.query({ order: "path" })) {
      entries.push(entry);
    }

    assertEquals(entries.length, 0);
  });
});
