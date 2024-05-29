import { toBlob, toTransformStream } from "@std/streams";
import { type AreaOfInterest, OPEN_END } from "@earthstar/willow-utils";
import { EntryDriverKvStore } from "../store/storage/entry_drivers/kv_store.ts";
import { KvDriverInMemory } from "../store/storage/kv/kv_driver_in_memory.ts";
import { PayloadDriverMemory } from "../store/storage/payload_drivers/memory.ts";
import { Store } from "../store/store.ts";
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
import { createDrop } from "./create_drop.ts";
import { ingestDrop } from "./ingest_drop.ts";
import { assert, assertEquals } from "@std/assert";
import { notErr } from "../errors.ts";

const payloadDriverAlfie = new PayloadDriverMemory(testSchemePayload);

function getStore(namespace: TestNamespace) {
  return new Store({
    entryDriver: new EntryDriverKvStore({
      kvDriver: new KvDriverInMemory(),
      namespaceScheme: testSchemeNamespace,
      subspaceScheme: testSchemeSubspace,
      pathScheme: testSchemePath,
      payloadScheme: testSchemePayload,
      fingerprintScheme: testSchemeFingerprint,
      getPayloadLength: (digest) => payloadDriverAlfie.length(digest),
    }),
    payloadDriver: payloadDriverAlfie,
    namespace,
    schemes: {
      namespace: testSchemeNamespace,
      subspace: testSchemeSubspace,
      path: testSchemePath,
      fingerprint: testSchemeFingerprint,
      authorisation: testSchemeAuthorisation,
      payload: testSchemePayload,
    },
  });
}

Deno.test("Create and ingest drop", async () => {
  const storeAlfie = getStore(TestNamespace.Vibes);

  for (let i = 0; i < 255; i++) {
    await storeAlfie.set({
      path: [new Uint8Array([i])],
      payload: crypto.getRandomValues(new Uint8Array(32)),
      subspace: TestSubspace.Alfie,
    }, TestSubspace.Alfie);

    await storeAlfie.set({
      path: [new Uint8Array([i])],
      payload: crypto.getRandomValues(new Uint8Array(32)),
      subspace: TestSubspace.Betty,
    }, TestSubspace.Betty);
  }

  const areaOfInterest: AreaOfInterest<TestSubspace> = {
    area: {
      includedSubspaceId: TestSubspace.Alfie,
      pathPrefix: [],
      timeRange: {
        start: 0n,
        end: OPEN_END,
      },
    },
    maxCount: 0,
    maxSize: 0n,
  };

  const dropStream = createDrop({
    store: storeAlfie,
    areaOfInterest,
    schemes: {
      namespace: testSchemeNamespace,
      subspace: testSchemeSubspace,
      path: testSchemePath,
      payload: testSchemePayload,
    },
    encodeAuthorisationToken: (token) => token,
    encryptTransform: toTransformStream(async function* (src) {
      for await (const chunk of src) {
        yield chunk;
      }
    }),
  });

  const dropBlob = await toBlob(dropStream);

  const storeBetty = await ingestDrop({
    dropStream: dropBlob.stream(),
    schemes: {
      namespace: testSchemeNamespace,
      subspace: testSchemeSubspace,
      payload: testSchemePayload,
      path: testSchemePath,
    },
    decodeStreamAuthorisationToken: async (bytes) => {
      await bytes.nextAbsolute(33);

      const token = bytes.array.slice(0, 33);

      bytes.prune(33);

      return token;
    },
    decryptTransform: toTransformStream(async function* (src) {
      for await (const chunk of src) {
        yield chunk;
      }
    }),
    getStore: (namespace) => {
      const store = getStore(namespace);
      return Promise.resolve(store);
    },
  });

  assert(notErr(storeBetty));

  const rangeAlfie = await storeAlfie.areaOfInterestToRange(areaOfInterest);
  const rangeBetty = await storeBetty.areaOfInterestToRange(areaOfInterest);

  assertEquals(
    await storeAlfie.summarise(rangeAlfie),
    await storeBetty.summarise(rangeBetty),
  );
});
