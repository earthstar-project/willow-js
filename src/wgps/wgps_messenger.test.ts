import { assertEquals } from "https://deno.land/std@0.202.0/assert/mod.ts";
import { transportPairInMemory } from "./transports/in_memory.ts";
import { GetStoreFn, WgpsMessenger } from "./wgps_messenger.ts";
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
import { encodeBase64, OPEN_END, Range3d } from "../../deps.ts";
import { Store } from "../store/store.ts";
import { EntryDriver, PayloadDriver } from "../store/storage/types.ts";
import { StoreSchemes } from "../store/types.ts";
import { PayloadDriverMemory } from "../store/storage/payload_drivers/memory.ts";
import { EntryDriverKvStore } from "../store/storage/entry_drivers/kv_store.ts";
import { KvDriverInMemory } from "../store/storage/kv/kv_driver_in_memory.ts";

type WgpsScenario = {
  name: string;

  getDrivers: (id: string, namespace: TestNamespace) => Promise<
    {
      entryDriver: EntryDriver<
        TestNamespace,
        TestSubspace,
        ArrayBuffer,
        Uint8Array
      >;
      payloadDriver: PayloadDriver<ArrayBuffer>;
    }
  >;

  dispose: () => Promise<void>;
};

const scenarioMemory: WgpsScenario = {
  name: "Memory",
  getDrivers: () => {
    const payloadDriver = new PayloadDriverMemory(testSchemePayload);

    const entryDriver = new EntryDriverKvStore({
      fingerprintScheme: testSchemeFingerprint,
      subspaceScheme: testSchemeSubspace,
      pathScheme: testSchemePath,
      payloadScheme: testSchemePayload,
      getPayloadLength: (digest) => payloadDriver.length(digest),
      namespaceScheme: testSchemeNamespace,
      kvDriver: new KvDriverInMemory(),
    });

    return Promise.resolve({
      entryDriver,
      payloadDriver,
    });
  },
  dispose: () => Promise.resolve(),
};

testWgpsMessenger(scenarioMemory);

function testWgpsMessenger(scenario: WgpsScenario) {
  Deno.test(`WgpsMessenger (${scenario.name})`, async (test) => {
    const ALFIE_ENTRIES = 150;
    const BETTY_ENTRIES = 150;

    await test.step("sync", async () => {
      const [alfie, betty] = transportPairInMemory();

      const challengeHash = async (bytes: Uint8Array) => {
        return new Uint8Array(await crypto.subtle.digest("SHA-256", bytes));
      };

      const storeMapAlfie = new StoreMap(
        async (namespace) => {
          const drivers = await scenario.getDrivers("alfie", namespace);

          const store = new Store({
            namespace,
            schemes: {
              authorisation: testSchemeAuthorisation,
              fingerprint: testSchemeFingerprint,
              namespace: testSchemeNamespace,
              path: testSchemePath,
              payload: testSchemePayload,
              subspace: testSchemeSubspace,
            },
            ...drivers,
          });

          return store;
        },
        {
          authorisation: testSchemeAuthorisation,
          fingerprint: testSchemeFingerprint,
          namespace: testSchemeNamespace,
          path: testSchemePath,
          payload: testSchemePayload,
          subspace: testSchemeSubspace,
        },
      );

      const storeFamilyAlfie = await storeMapAlfie.get(TestNamespace.Family);

      for (let i = 0; i < ALFIE_ENTRIES; i++) {
        await storeFamilyAlfie.set({
          subspace: TestSubspace.Gemma,
          payload: new TextEncoder().encode(`Originated from Alfie! (${i})`),
          path: [
            new Uint8Array([1]),
            new Uint8Array([2]),
            new Uint8Array([i, i + 1, i + 2, i + 3]),
          ],
        }, TestSubspace.Gemma);
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
        getStore: (namespace) => {
          return storeMapAlfie.get(namespace);
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

      const storeMapBetty = new StoreMap(
        async (namespace) => {
          const drivers = await scenario.getDrivers("betty", namespace);
          const store = new Store({
            namespace,
            schemes: {
              authorisation: testSchemeAuthorisation,
              fingerprint: testSchemeFingerprint,
              namespace: testSchemeNamespace,
              path: testSchemePath,
              payload: testSchemePayload,
              subspace: testSchemeSubspace,
            },
            ...drivers,
          });

          return store;
        },
        {
          authorisation: testSchemeAuthorisation,
          fingerprint: testSchemeFingerprint,
          namespace: testSchemeNamespace,
          path: testSchemePath,
          payload: testSchemePayload,
          subspace: testSchemeSubspace,
        },
      );

      const storeFamilyBetty = await storeMapBetty.get(TestNamespace.Family);

      for (let i = 0; i < BETTY_ENTRIES; i++) {
        await storeFamilyBetty.set({
          subspace: TestSubspace.Gemma,
          payload: new TextEncoder().encode(`Originated from Betty! (${i})`),
          path: [
            new Uint8Array([1]),
            new Uint8Array([2]),
            new Uint8Array([i]),
          ],
        }, TestSubspace.Gemma);
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
        getStore: (namespace) => {
          return storeMapBetty.get(namespace);
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

      await delay(2000);

      const range: Range3d<TestSubspace> = {
        subspaceRange: {
          start: TestSubspace.Gemma,
          end: OPEN_END,
        },
        pathRange: {
          start: [],
          end: OPEN_END,
        },
        timeRange: {
          start: 0n,
          end: OPEN_END,
        },
      };

      const { fingerprint: alfieFp, size: alfieSize } = await storeFamilyAlfie
        .summarise(range);
      const { fingerprint: bettyFp, size: bettySize } = await storeFamilyBetty
        .summarise(range);

      console.log({ alfieSize, bettySize });

      let actualSizeAlfie = 0;
      let actualSizeBetty = 0;

      for await (
        const _ of storeFamilyAlfie.query({
          area: {
            includedSubspaceId: TestSubspace.Gemma,
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
        const _ of storeFamilyBetty.query({
          area: {
            includedSubspaceId: TestSubspace.Gemma,
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

      const expectedSize = ALFIE_ENTRIES + BETTY_ENTRIES;

      assertEquals(actualSizeAlfie, expectedSize);
      assertEquals(actualSizeBetty, expectedSize);
      assertEquals(alfieSize, expectedSize);
      assertEquals(bettySize, expectedSize);

      assertEquals(alfieFp, bettyFp);

      messengerAlfie.close();
      messengerBetty.close();
    });

    scenario.dispose();
  });
}

/** A mapping of namespace IDs to stores */
export class StoreMap<
  Prefingerprint,
  Fingerprint,
  AuthorisationToken,
  NamespaceId,
  SubspaceId,
  PayloadDigest,
  AuthorisationOpts,
> {
  private map = new Map<
    string,
    Store<
      NamespaceId,
      SubspaceId,
      PayloadDigest,
      AuthorisationOpts,
      AuthorisationToken,
      Prefingerprint,
      Fingerprint
    >
  >();

  constructor(
    readonly getStore: GetStoreFn<
      Prefingerprint,
      Fingerprint,
      AuthorisationToken,
      AuthorisationOpts,
      NamespaceId,
      SubspaceId,
      PayloadDigest
    >,
    readonly schemes: StoreSchemes<
      NamespaceId,
      SubspaceId,
      PayloadDigest,
      AuthorisationOpts,
      AuthorisationToken,
      Prefingerprint,
      Fingerprint
    >,
  ) {
  }

  private getKey(namespace: NamespaceId): string {
    const encoded = this.schemes.namespace.encode(namespace);
    return encodeBase64(encoded);
  }

  async get(
    namespace: NamespaceId,
  ): Promise<
    Store<
      NamespaceId,
      SubspaceId,
      PayloadDigest,
      AuthorisationOpts,
      AuthorisationToken,
      Prefingerprint,
      Fingerprint
    >
  > {
    const key = this.getKey(namespace);

    const store = this.map.get(key);

    if (store) {
      return store;
    }

    const newStore = await this.getStore(namespace);

    this.map.set(key, newStore);

    return newStore;
  }
}
