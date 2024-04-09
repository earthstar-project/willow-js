import { assertEquals } from "https://deno.land/std@0.202.0/assert/mod.ts";
import { transportPairInMemory } from "./transports/in_memory.ts";
import { WgpsMessenger } from "./wgps_messenger.ts";
import {
  TestNamespace,
  TestReadCap,
  testSchemeAccessControl,
  testSchemeAuthorisation,
  testSchemeAuthorisationToken,
  testSchemeFingerprint,
  testSchemeNamespace,
  testSchemePai,
  testSchemePath,
  testSchemePayload,
  testSchemeSubspace,
  testSchemeSubspaceCap,
  TestSubspace,
} from "../test/test_schemes.ts";
import { delay } from "https://deno.land/std@0.202.0/async/delay.ts";
import {
  ANY_SUBSPACE,
  defaultRange3d,
  encodeBase32,
  OPEN_END,
} from "../../deps.ts";
import { Store } from "../store/store.ts";
import { PayloadDriverFilesystem } from "../store/storage/payload_drivers/filesystem.ts";
import { EntryDriverKvStore } from "../store/storage/entry_drivers/kv_store.ts";
import { KvDriverDeno } from "../store/storage/kv/kv_driver_deno.ts";
import { emptyDir } from "https://deno.land/std@0.202.0/fs/empty_dir.ts";

Deno.test("WgpsMessenger", async (test) => {
  const alfieDenoKv = await Deno.openKv("./test/alfie");
  const bettyDenoKv = await Deno.openKv("./test/betty");

  const ALFIE_ENTRIES = 2;
  const BETTY_ENTRIES = 2;

  await test.step("sync", async () => {
    const [alfie, betty] = transportPairInMemory();

    const challengeHash = async (bytes: Uint8Array) => {
      return new Uint8Array(await crypto.subtle.digest("SHA-256", bytes));
    };

    const alfiePayloadDriver = new PayloadDriverFilesystem(
      "./test/alfie_payloads",
      testSchemePayload,
    );
    const alfieEntryDriver = new EntryDriverKvStore({
      namespaceScheme: testSchemeNamespace,
      fingerprintScheme: testSchemeFingerprint,
      pathScheme: testSchemePath,
      payloadScheme: testSchemePayload,
      subspaceScheme: testSchemeSubspace,
      getPayloadLength: (digest) => {
        return alfiePayloadDriver.length(digest);
      },
      kvDriver: new KvDriverDeno(alfieDenoKv),
    });

    const alfieStore = new Store({
      namespace: TestNamespace.Family,
      schemes: {
        namespace: testSchemeNamespace,

        path: testSchemePath,
        subspace: testSchemeSubspace,

        fingerprint: testSchemeFingerprint,
        authorisation: testSchemeAuthorisation,
        payload: testSchemePayload,
      },
      entryDriver: alfieEntryDriver,
      payloadDriver: alfiePayloadDriver,
    });

    for (let i = 0; i < ALFIE_ENTRIES; i++) {
      const res = await alfieStore.set({
        subspace: TestSubspace.Gemma,
        payload: new TextEncoder().encode(`Originated from Alfie! (${i})`),
        path: [
          new Uint8Array([1]),
          new Uint8Array([2]),
          crypto.getRandomValues(new Uint8Array(8)),
        ],
      }, TestSubspace.Gemma);

      if (res.kind === "success") {
        console.log(encodeBase32(new Uint8Array(res.entry.payloadDigest)));
      }
    }

    const messengerAlfie = new WgpsMessenger({
      challengeHash,
      challengeLength: 128,
      challengeHashLength: 32,
      maxPayloadSizePower: 8,
      transport: alfie,
      schemes: {
        subspaceCap: testSchemeSubspaceCap,
        namespace: testSchemeNamespace,
        accessControl: testSchemeAccessControl,
        pai: testSchemePai,
        path: testSchemePath,
        subspace: testSchemeSubspace,
        authorisationToken: testSchemeAuthorisationToken,
        fingerprint: testSchemeFingerprint,
        authorisation: testSchemeAuthorisation,
        payload: testSchemePayload,
      },
      getStoreDrivers: () => {
        return {
          entryDriver: alfieEntryDriver,
          payloadDriver: alfiePayloadDriver,
        };
      },
      interests: new Map([[
        {
          capability: {
            namespace: TestNamespace.Family,
            subspace: TestSubspace.Gemma,
            path: [new Uint8Array([1])],
            receiver: TestSubspace.Alfie,
            time: {
              start: BigInt(0),
              end: OPEN_END,
            },
          } as TestReadCap,
        },
        [{
          area: {
            includedSubspaceId: TestSubspace.Gemma,
            pathPrefix: [new Uint8Array([1])],
            timeRange: {
              start: BigInt(0),
              end: OPEN_END,
            },
          },
          maxCount: 0,
          maxSize: BigInt(0),
        }],
      ]]),
    });

    const bettyPayloadDriver = new PayloadDriverFilesystem(
      "./test/betty_payloads",
      testSchemePayload,
    );
    const bettyEntryDriver = new EntryDriverKvStore({
      namespaceScheme: testSchemeNamespace,
      fingerprintScheme: testSchemeFingerprint,
      pathScheme: testSchemePath,
      payloadScheme: testSchemePayload,
      subspaceScheme: testSchemeSubspace,
      getPayloadLength: (digest) => {
        return bettyPayloadDriver.length(digest);
      },
      kvDriver: new KvDriverDeno(bettyDenoKv),
    });

    const bettyStore = new Store({
      namespace: TestNamespace.Family,
      schemes: {
        namespace: testSchemeNamespace,

        path: testSchemePath,
        subspace: testSchemeSubspace,

        fingerprint: testSchemeFingerprint,
        authorisation: testSchemeAuthorisation,
        payload: testSchemePayload,
      },
      entryDriver: bettyEntryDriver,
      payloadDriver: bettyPayloadDriver,
    });

    for (let i = 0; i < BETTY_ENTRIES; i++) {
      const res = await bettyStore.set({
        subspace: TestSubspace.Gemma,
        payload: new TextEncoder().encode(`Originated from Betty! (${i})`),
        path: [
          new Uint8Array([1]),
          new Uint8Array([2]),
          crypto.getRandomValues(new Uint8Array(8)),
        ],
      }, TestSubspace.Gemma);

      if (res.kind === "success") {
        console.log(encodeBase32(new Uint8Array(res.entry.payloadDigest)));
      }
    }

    const messengerBetty = new WgpsMessenger({
      challengeHash,
      challengeLength: 128,
      challengeHashLength: 32,
      maxPayloadSizePower: 8,
      transport: betty,
      schemes: {
        subspaceCap: testSchemeSubspaceCap,
        namespace: testSchemeNamespace,
        accessControl: testSchemeAccessControl,
        pai: testSchemePai,
        path: testSchemePath,
        subspace: testSchemeSubspace,
        authorisationToken: testSchemeAuthorisationToken,
        fingerprint: testSchemeFingerprint,
        authorisation: testSchemeAuthorisation,
        payload: testSchemePayload,
      },
      getStoreDrivers: () => {
        return {
          entryDriver: bettyEntryDriver,
          payloadDriver: bettyPayloadDriver,
        };
      },
      interests: new Map([[
        {
          capability: {
            namespace: TestNamespace.Family,
            subspace: TestSubspace.Gemma,
            path: [new Uint8Array([1])],
            receiver: TestSubspace.Betty,
            time: {
              start: BigInt(0),
              end: OPEN_END,
            },
          } as TestReadCap,
        },
        [{
          area: {
            includedSubspaceId: TestSubspace.Gemma,
            pathPrefix: [new Uint8Array([1]), new Uint8Array([2])],
            timeRange: {
              start: BigInt(0),
              end: OPEN_END,
            },
          },
          maxCount: 0,
          maxSize: BigInt(0),
        }],
      ]]),
    });

    // @ts-ignore looking at private values is fine
    const alfieChallengeAlfie = await messengerAlfie.ourChallenge;
    // @ts-ignore looking at private values is fine
    const bettyChallengeAlfie = await messengerAlfie.theirChallenge;

    // @ts-ignore looking at private values is fine
    const alfieChallengeBetty = await messengerBetty.theirChallenge;
    // @ts-ignore looking at private values is fine
    const bettyChallengeBetty = await messengerBetty.ourChallenge;

    assertEquals(alfieChallengeAlfie, alfieChallengeBetty);
    assertEquals(bettyChallengeAlfie, bettyChallengeBetty);

    await delay(50);

    // @ts-ignore looking at private values is fine
    const receivedCapsAlfie = Array.from(messengerAlfie.handlesCapsTheirs)
      .map((
        [, cap],
      ) => cap);
    // @ts-ignore looking at private values is fine
    const receivedCapsBetty = Array.from(messengerBetty.handlesCapsTheirs)
      .map((
        [, cap],
      ) => cap);

    assertEquals(receivedCapsAlfie, [{
      namespace: TestNamespace.Family,
      subspace: TestSubspace.Gemma,
      path: [new Uint8Array([1])],
      receiver: TestSubspace.Betty,
      time: {
        start: BigInt(0),
        end: OPEN_END,
      },
    }]);

    assertEquals(receivedCapsBetty, [{
      namespace: TestNamespace.Family,
      subspace: TestSubspace.Gemma,
      path: [new Uint8Array([1])],
      receiver: TestSubspace.Alfie,
      time: {
        start: BigInt(0),
        end: OPEN_END,
      },
    }]);

    await delay(1000);

    const { fingerprint: alfieFp, size: alfieSize } = await alfieStore
      .summarise(
        defaultRange3d(TestSubspace.Alfie),
      );
    const { fingerprint: bettyFp, size: bettySize } = await bettyStore
      .summarise(
        defaultRange3d(TestSubspace.Alfie),
      );

    console.log({ alfieSize, bettySize });

    let actualSizeAlfie = 0;
    let actualSizeBetty = 0;

    for await (
      const _ of alfieStore.query({
        area: {
          includedSubspaceId: ANY_SUBSPACE,
          pathPrefix: [],
          timeRange: {
            start: 0n,
            end: OPEN_END,
          },
        },
        maxCount: 0,
        maxSize: 0n,
      }, "subspace")
    ) {
      actualSizeAlfie += 1;
    }

    for await (
      const _ of bettyStore.query({
        area: {
          includedSubspaceId: ANY_SUBSPACE,
          pathPrefix: [],
          timeRange: {
            start: 0n,
            end: OPEN_END,
          },
        },
        maxCount: 0,
        maxSize: 0n,
      }, "subspace")
    ) {
      actualSizeBetty += 1;
    }

    console.log({ actualSizeAlfie, actualSizeBetty });

    // assertEquals(alfieFp, bettyFp);
    assertEquals(alfieSize, bettySize);
  });

  alfieDenoKv.close();
  bettyDenoKv.close();

  // await emptyDir("./test");
});
