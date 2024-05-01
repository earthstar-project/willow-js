import {
  assertEquals,
  assertNotEquals,
} from "https://deno.land/std@0.202.0/assert/mod.ts";
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
import { ANY_SUBSPACE, encodeBase64, OPEN_END, Range3d } from "../../deps.ts";
import { Store } from "../store/store.ts";
import { EntryDriver, PayloadDriver } from "../store/storage/types.ts";
import { StoreSchemes } from "../store/types.ts";
import { PayloadDriverMemory } from "../store/storage/payload_drivers/memory.ts";
import { EntryDriverKvStore } from "../store/storage/entry_drivers/kv_store.ts";
import { KvDriverInMemory } from "../store/storage/kv/kv_driver_in_memory.ts";
import { PayloadDriverFilesystem } from "../store/storage/payload_drivers/filesystem.ts";
import { KvDriverDeno } from "../store/storage/kv/kv_driver_deno.ts";
import { emptyDir } from "https://deno.land/std@0.173.0/fs/empty_dir.ts";
import { ensureDir } from "https://deno.land/std@0.188.0/fs/ensure_dir.ts";

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
  timeMultiplier: number;
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
  timeMultiplier: 10,
  dispose: () => Promise.resolve(),
};

testWgpsMessenger(scenarioMemory);

class ScenarioPersisted implements WgpsScenario {
  name = "Filesystem";
  timeMultiplier = 50;

  kvs: Deno.Kv[] = [];

  async getDrivers(id: string, namespace: TestNamespace) {
    await ensureDir(`./test/${id}/${namespace}`);

    const payloadDriver = new PayloadDriverFilesystem(
      `./test/${id}/${namespace}/payloads`,
      testSchemePayload,
    );

    const kv = await Deno.openKv(`./test/${id}/${namespace}/kv`);

    const entryDriver = new EntryDriverKvStore({
      fingerprintScheme: testSchemeFingerprint,
      subspaceScheme: testSchemeSubspace,
      pathScheme: testSchemePath,
      payloadScheme: testSchemePayload,
      getPayloadLength: (digest) => payloadDriver.length(digest),
      namespaceScheme: testSchemeNamespace,
      kvDriver: new KvDriverDeno(kv),
    });

    this.kvs.push(kv);

    return Promise.resolve({
      entryDriver,
      payloadDriver,
    });
  }

  async dispose() {
    await emptyDir("./test");

    for (const kv of this.kvs) {
      kv.close();
    }

    this.kvs = [];
  }
}

testWgpsMessenger(new ScenarioPersisted());

function testWgpsMessenger(scenario: WgpsScenario) {
  // Things to test:

  Deno.test(`Non-reconciliation of disjoint namespaces (${scenario.name})`, async (test) => {
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

      /** Alfie knows about this one. */
      const storeFamilyAlfie = await storeMapAlfie.get(TestNamespace.Family);
      /** Alfie doesn't know about this one. */
      const storeProjectAlfie = await storeMapAlfie.get(TestNamespace.Project);

      for (let i = 0; i < 10; i++) {
        await storeFamilyAlfie.set({
          subspace: TestSubspace.Gemma,
          payload: new TextEncoder().encode(`Originated from Alfie! (${i})`),
          path: [
            crypto.getRandomValues(new Uint8Array(4)),
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
              path: [],
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
              pathPrefix: [],
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

      /** Betty doesn't know about this one. */
      const storeFamilyBetty = await storeMapBetty.get(TestNamespace.Family);
      /** Betty does know about this one. */
      const storeProjectBetty = await storeMapBetty.get(TestNamespace.Project);

      for (let i = 0; i < 10; i++) {
        await storeProjectBetty.set({
          subspace: TestSubspace.Gemma,
          payload: new TextEncoder().encode(`Originated from Betty! (${i})`),
          path: [
            new Uint8Array([1]),
            new Uint8Array([2]),
            crypto.getRandomValues(new Uint8Array(4)),
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
              namespace: TestNamespace.Project,
              subspace: TestSubspace.Gemma,
              path: [],
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
              pathPrefix: [],
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

      await delay(20 * scenario.timeMultiplier);

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

      const { fingerprint: alfieFamilyFp, size: alfieFamilySize } =
        await storeFamilyAlfie
          .summarise(range);
      const { fingerprint: bettyFamilyFp, size: bettyFamilySize } =
        await storeFamilyBetty
          .summarise(range);

      assertEquals(alfieFamilySize, 10);
      assertEquals(bettyFamilySize, 0);
      assertNotEquals(alfieFamilyFp, bettyFamilyFp);

      let actualSizeFamilyAlfie = 0;
      let actualSizeFamilyBetty = 0;

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
        actualSizeFamilyAlfie += 1;
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
        actualSizeFamilyBetty += 1;
      }

      assertEquals(actualSizeFamilyAlfie, 10);
      assertEquals(actualSizeFamilyBetty, 0);

      const { fingerprint: alfieProjectFp, size: alfieProjectSize } =
        await storeProjectAlfie
          .summarise(range);
      const { fingerprint: bettyProjectFp, size: bettyProjectSize } =
        await storeProjectBetty
          .summarise(range);

      assertEquals(alfieProjectSize, 0);
      assertEquals(bettyProjectSize, 10);
      assertNotEquals(alfieProjectFp, bettyProjectFp);

      let actualSizeProjectAlfie = 0;
      let actualSizeProjectBetty = 0;

      for await (
        const _ of storeProjectAlfie.query({
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
        actualSizeProjectAlfie += 1;
      }

      for await (
        const _ of storeProjectBetty.query({
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
        actualSizeProjectBetty += 1;
      }

      assertEquals(actualSizeProjectAlfie, 0);
      assertEquals(actualSizeProjectBetty, 10);

      messengerAlfie.close();
      messengerBetty.close();
    });

    await scenario.dispose();
  });

  Deno.test(`Non-reconciliation of disjoint subspaces (${scenario.name})`, async (test) => {
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

      for (let i = 0; i < 7; i++) {
        await storeFamilyAlfie.set({
          subspace: TestSubspace.Gemma,
          payload: new TextEncoder().encode(`Originated from Alfie! (${i})`),
          path: [
            crypto.getRandomValues(new Uint8Array(4)),
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
              path: [],
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
              pathPrefix: [],
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

      for (let i = 0; i < 3; i++) {
        await storeFamilyBetty.set({
          subspace: TestSubspace.Dalton,
          payload: new TextEncoder().encode(`Originated from Betty! (${i})`),
          path: [
            crypto.getRandomValues(new Uint8Array(4)),
          ],
        }, TestSubspace.Dalton);
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
              namespace: TestNamespace.Project,
              subspace: TestSubspace.Dalton,
              path: [],
              receiver: TestSubspace.Betty,
              time: {
                start: BigInt(0),
                end: OPEN_END,
              },
            } as TestReadCap,
          },
          [{
            area: {
              includedSubspaceId: TestSubspace.Dalton,
              pathPrefix: [],
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

      await delay(20 * scenario.timeMultiplier);

      const { size: alfieFamilySize } = await storeFamilyAlfie
        .summarise({
          subspaceRange: {
            start: TestSubspace.Dalton,
            end: TestSubspace.Epson,
          },
          pathRange: {
            start: [],
            end: OPEN_END,
          },
          timeRange: {
            start: 0n,
            end: OPEN_END,
          },
        });
      const { size: bettyFamilySize } = await storeFamilyBetty
        .summarise({
          subspaceRange: {
            start: TestSubspace.Gemma,
            end: TestSubspace.Dalton,
          },
          pathRange: {
            start: [],
            end: OPEN_END,
          },
          timeRange: {
            start: 0n,
            end: OPEN_END,
          },
        });

      assertEquals(alfieFamilySize, 0);
      assertEquals(bettyFamilySize, 0);

      let actualSizeFamilyAlfie = 0;
      let actualSizeFamilyBetty = 0;

      for await (
        const _ of storeFamilyAlfie.query({
          area: {
            includedSubspaceId: TestSubspace.Dalton,
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
        actualSizeFamilyAlfie += 1;
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
        actualSizeFamilyBetty += 1;
      }

      assertEquals(actualSizeFamilyAlfie, 0);
      assertEquals(actualSizeFamilyBetty, 0);

      messengerAlfie.close();
      messengerBetty.close();
    });

    await scenario.dispose();
  });

  Deno.test(`Non-reconciliation of disjoint subspaces (${scenario.name}), even with intersecting secondary less-specific fragments`, async (test) => {
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

      for (let i = 0; i < 7; i++) {
        await storeFamilyAlfie.set({
          subspace: TestSubspace.Gemma,
          payload: new TextEncoder().encode(`Originated from Alfie! (${i})`),
          path: [
            new Uint8Array([1]),
            new Uint8Array([2]),
            crypto.getRandomValues(new Uint8Array(4)),
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
              path: [],
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
              pathPrefix: [
                new Uint8Array([1]),
                new Uint8Array([2]),
              ],
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

      for (let i = 0; i < 3; i++) {
        await storeFamilyBetty.set({
          subspace: TestSubspace.Dalton,
          payload: new TextEncoder().encode(`Originated from Betty! (${i})`),
          path: [
            new Uint8Array([1]),
            new Uint8Array([3]),
            crypto.getRandomValues(new Uint8Array(4)),
          ],
        }, TestSubspace.Dalton);
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
              namespace: TestNamespace.Project,
              subspace: TestSubspace.Dalton,
              path: [],
              receiver: TestSubspace.Betty,
              time: {
                start: BigInt(0),
                end: OPEN_END,
              },
            } as TestReadCap,
          },
          [{
            area: {
              includedSubspaceId: TestSubspace.Dalton,
              pathPrefix: [new Uint8Array([1]), new Uint8Array([3])],
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

      await delay(20 * scenario.timeMultiplier);

      const { size: alfieFamilySize } = await storeFamilyAlfie
        .summarise({
          subspaceRange: {
            start: TestSubspace.Dalton,
            end: TestSubspace.Epson,
          },
          pathRange: {
            start: [],
            end: OPEN_END,
          },
          timeRange: {
            start: 0n,
            end: OPEN_END,
          },
        });
      const { size: bettyFamilySize } = await storeFamilyBetty
        .summarise({
          subspaceRange: {
            start: TestSubspace.Gemma,
            end: TestSubspace.Dalton,
          },
          pathRange: {
            start: [],
            end: OPEN_END,
          },
          timeRange: {
            start: 0n,
            end: OPEN_END,
          },
        });

      assertEquals(alfieFamilySize, 0);
      assertEquals(bettyFamilySize, 0);

      let actualSizeFamilyAlfie = 0;
      let actualSizeFamilyBetty = 0;

      for await (
        const _ of storeFamilyAlfie.query({
          area: {
            includedSubspaceId: TestSubspace.Dalton,
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
        actualSizeFamilyAlfie += 1;
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
        actualSizeFamilyBetty += 1;
      }

      assertEquals(actualSizeFamilyAlfie, 0);
      assertEquals(actualSizeFamilyBetty, 0);

      messengerAlfie.close();
      messengerBetty.close();
    });

    await scenario.dispose();
  });

  Deno.test(`Non-reconciliation of disjoint path prefixes (${scenario.name})`, async (test) => {
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

      for (let i = 0; i < 7; i++) {
        await storeFamilyAlfie.set({
          subspace: TestSubspace.Gemma,
          payload: new TextEncoder().encode(`Originated from Alfie! (${i})`),
          path: [
            new Uint8Array([1]),
            crypto.getRandomValues(new Uint8Array(4)),
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

      for (let i = 0; i < 3; i++) {
        await storeFamilyBetty.set({
          subspace: TestSubspace.Gemma,
          payload: new TextEncoder().encode(`Originated from Betty! (${i})`),
          path: [
            new Uint8Array([2]),
            crypto.getRandomValues(new Uint8Array(4)),
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
              path: [new Uint8Array([2])],
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
              pathPrefix: [new Uint8Array([2])],
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

      await delay(20 * scenario.timeMultiplier);

      const range: Range3d<TestSubspace> = {
        subspaceRange: {
          start: TestSubspace.Gemma,
          end: TestSubspace.Dalton,
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

      const { size: alfieFamilySize } = await storeFamilyAlfie
        .summarise(range);
      const { size: bettyFamilySize } = await storeFamilyBetty
        .summarise(range);

      assertEquals(alfieFamilySize, 7);
      assertEquals(bettyFamilySize, 3);

      let actualSizeFamilyAlfie = 0;
      let actualSizeFamilyBetty = 0;

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
        actualSizeFamilyAlfie += 1;
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
        actualSizeFamilyBetty += 1;
      }

      assertEquals(actualSizeFamilyAlfie, 7);
      assertEquals(actualSizeFamilyBetty, 3);

      messengerAlfie.close();
      messengerBetty.close();
    });

    await scenario.dispose();
  });

  // Partial reconciliation of overlapping capabilities (via prefix)
  Deno.test(
    `Partial reconciliation via subspace capability (${scenario.name})`,
    async (test) => {
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

        for (let i = 0; i < 5; i++) {
          await storeFamilyAlfie.set({
            subspace: TestSubspace.Gemma,
            payload: new TextEncoder().encode(`Originated from Alfie! (${i})`),
            path: [
              new Uint8Array([99]),
              crypto.getRandomValues(new Uint8Array(4)),
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
                path: [],
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
                pathPrefix: [],
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

        for (let i = 0; i < 5; i++) {
          await storeFamilyBetty.set({
            subspace: TestSubspace.Gemma,
            payload: new TextEncoder().encode(`Originated from Betty! (${i})`),
            path: [
              new Uint8Array([99]),
              crypto.getRandomValues(new Uint8Array(4)),
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
                subspace: ANY_SUBSPACE,
                path: [new Uint8Array([99])],
                receiver: TestSubspace.Betty,
                time: {
                  start: BigInt(0),
                  end: OPEN_END,
                },
              } as TestReadCap,
              subspaceCapability: {
                namespace: TestNamespace.Family,
                path: [new Uint8Array([99])],
                receiver: TestSubspace.Betty,
                time: {
                  start: BigInt(0),
                  end: OPEN_END,
                },
              },
            },
            [{
              area: {
                includedSubspaceId: ANY_SUBSPACE,
                pathPrefix: [new Uint8Array([99])],
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

        await delay(20 * scenario.timeMultiplier);

        const range: Range3d<TestSubspace> = {
          subspaceRange: {
            start: TestSubspace.Gemma,
            end: TestSubspace.Dalton,
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

        const { size: alfieFamilySize } = await storeFamilyAlfie
          .summarise(range);
        const { size: bettyFamilySize } = await storeFamilyBetty
          .summarise(range);

        assertEquals(alfieFamilySize, 10);
        assertEquals(bettyFamilySize, 10);

        let actualSizeFamilyAlfie = 0;
        let actualSizeFamilyBetty = 0;

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
          actualSizeFamilyAlfie += 1;
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
          actualSizeFamilyBetty += 1;
        }

        assertEquals(actualSizeFamilyAlfie, 10);
        assertEquals(actualSizeFamilyBetty, 10);

        messengerAlfie.close();
        messengerBetty.close();
      });

      await scenario.dispose();
    },
  );

  // Partial reconciliation of overlapping capabilities (via prefix)
  Deno.test(
    `Partial reconciliation of overlapping path prefixes (${scenario.name})`,
    async (test) => {
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

        for (let i = 0; i < 5; i++) {
          await storeFamilyAlfie.set({
            subspace: TestSubspace.Gemma,
            payload: new TextEncoder().encode(`Originated from Alfie! (${i})`),
            path: [
              new Uint8Array([1]),
              crypto.getRandomValues(new Uint8Array(4)),
            ],
          }, TestSubspace.Gemma);
        }

        for (let i = 0; i < 5; i++) {
          await storeFamilyAlfie.set({
            subspace: TestSubspace.Gemma,
            payload: new TextEncoder().encode(`Originated from Alfie! (${i})`),
            path: [
              new Uint8Array([1]),
              new Uint8Array([2]),
              crypto.getRandomValues(new Uint8Array(4)),
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

        for (let i = 0; i < 5; i++) {
          await storeFamilyBetty.set({
            subspace: TestSubspace.Gemma,
            payload: new TextEncoder().encode(`Originated from Betty! (${i})`),
            path: [
              new Uint8Array([1]),
              new Uint8Array([2]),
              crypto.getRandomValues(new Uint8Array(4)),
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
                path: [new Uint8Array([1]), new Uint8Array([2])],
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

        await delay(20 * scenario.timeMultiplier);

        const range: Range3d<TestSubspace> = {
          subspaceRange: {
            start: TestSubspace.Gemma,
            end: TestSubspace.Dalton,
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

        const { size: alfieFamilySize } = await storeFamilyAlfie
          .summarise(range);
        const { size: bettyFamilySize } = await storeFamilyBetty
          .summarise(range);

        assertEquals(alfieFamilySize, 15);
        assertEquals(bettyFamilySize, 10);

        let actualSizeFamilyAlfie = 0;
        let actualSizeFamilyBetty = 0;

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
          actualSizeFamilyAlfie += 1;
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
          actualSizeFamilyBetty += 1;
        }

        assertEquals(actualSizeFamilyAlfie, 15);
        assertEquals(actualSizeFamilyBetty, 10);

        messengerAlfie.close();
        messengerBetty.close();
      });

      await scenario.dispose();
    },
  );

  Deno.test(
    `Full Reconciliation of joint equivalent capabilities (${scenario.name})`,
    async (test) => {
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

        for (let i = 0; i < 10; i++) {
          await storeFamilyAlfie.set({
            subspace: TestSubspace.Gemma,
            payload: new TextEncoder().encode(`Originated from Alfie! (${i})`),
            path: [
              new Uint8Array([1]),
              crypto.getRandomValues(new Uint8Array(4)),
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

        for (let i = 0; i < 10; i++) {
          await storeFamilyBetty.set({
            subspace: TestSubspace.Gemma,
            payload: new TextEncoder().encode(`Originated from Betty! (${i})`),
            path: [
              new Uint8Array([1]),
              new Uint8Array([2]),
              crypto.getRandomValues(new Uint8Array(4)),
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

        await delay(20 * scenario.timeMultiplier);

        const range: Range3d<TestSubspace> = {
          subspaceRange: {
            start: TestSubspace.Gemma,
            end: TestSubspace.Dalton,
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

        const { size: alfieFamilySize } = await storeFamilyAlfie
          .summarise(range);
        const { size: bettyFamilySize } = await storeFamilyBetty
          .summarise(range);

        assertEquals(alfieFamilySize, 20);
        assertEquals(bettyFamilySize, 20);

        let actualSizeFamilyAlfie = 0;
        let actualSizeFamilyBetty = 0;

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
          actualSizeFamilyAlfie += 1;
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
          actualSizeFamilyBetty += 1;
        }

        assertEquals(actualSizeFamilyAlfie, 20);
        assertEquals(actualSizeFamilyBetty, 20);

        messengerAlfie.close();
        messengerBetty.close();
      });

      await scenario.dispose();
    },
  );

  Deno.test(
    `Reconciliation of many namespace (${scenario.name})`,
    async (test) => {
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

        for (let i = 0; i < 10; i++) {
          await storeFamilyAlfie.set({
            subspace: TestSubspace.Gemma,
            payload: new TextEncoder().encode(`Originated from Alfie! (${i})`),
            path: [
              new Uint8Array([1]),
              crypto.getRandomValues(new Uint8Array(4)),
            ],
          }, TestSubspace.Gemma);
        }

        const storeProjectAlfie = await storeMapAlfie.get(
          TestNamespace.Project,
        );

        for (let i = 0; i < 10; i++) {
          await storeProjectAlfie.set({
            subspace: TestSubspace.Dalton,
            payload: new TextEncoder().encode(`Originated from Alfie! (${i})`),
            path: [
              new Uint8Array([2]),
              crypto.getRandomValues(new Uint8Array(4)),
            ],
          }, TestSubspace.Dalton);
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
          ], [
            {
              capability: {
                namespace: TestNamespace.Project,
                subspace: TestSubspace.Dalton,
                path: [new Uint8Array([2])],
                receiver: TestSubspace.Alfie,
                time: {
                  start: BigInt(0),
                  end: OPEN_END,
                },
              } as TestReadCap,
            },
            [{
              area: {
                includedSubspaceId: TestSubspace.Dalton,
                pathPrefix: [new Uint8Array([2])],
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

        for (let i = 0; i < 10; i++) {
          await storeFamilyBetty.set({
            subspace: TestSubspace.Gemma,
            payload: new TextEncoder().encode(`Originated from Betty! (${i})`),
            path: [
              new Uint8Array([1]),
              new Uint8Array([2]),
              crypto.getRandomValues(new Uint8Array(4)),
            ],
          }, TestSubspace.Gemma);
        }

        const storeProjectBetty = await storeMapBetty.get(
          TestNamespace.Project,
        );

        for (let i = 0; i < 10; i++) {
          await storeProjectBetty.set({
            subspace: TestSubspace.Dalton,
            payload: new TextEncoder().encode(`Originated from Betty! (${i})`),
            path: [
              new Uint8Array([2]),
              crypto.getRandomValues(new Uint8Array(4)),
            ],
          }, TestSubspace.Dalton);
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
                pathPrefix: [new Uint8Array([1])],
                timeRange: {
                  start: BigInt(0),
                  end: OPEN_END,
                },
              },
              maxCount: 0,
              maxSize: BigInt(0),
            }],
          ], [
            {
              capability: {
                namespace: TestNamespace.Project,
                subspace: TestSubspace.Dalton,
                path: [new Uint8Array([2])],
                receiver: TestSubspace.Betty,
                time: {
                  start: BigInt(0),
                  end: OPEN_END,
                },
              } as TestReadCap,
            },
            [{
              area: {
                includedSubspaceId: TestSubspace.Dalton,
                pathPrefix: [new Uint8Array([2])],
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

        await delay(20 * scenario.timeMultiplier);

        const range: Range3d<TestSubspace> = {
          subspaceRange: {
            start: TestSubspace.Gemma,
            end: TestSubspace.Dalton,
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

        const { size: alfieFamilySize } = await storeFamilyAlfie
          .summarise(range);
        const { size: bettyFamilySize } = await storeFamilyBetty
          .summarise(range);

        assertEquals(alfieFamilySize, 20);
        assertEquals(bettyFamilySize, 20);

        let actualSizeFamilyAlfie = 0;
        let actualSizeFamilyBetty = 0;

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
          actualSizeFamilyAlfie += 1;
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
          actualSizeFamilyBetty += 1;
        }

        assertEquals(actualSizeFamilyAlfie, 20);
        assertEquals(actualSizeFamilyBetty, 20);

        const range2: Range3d<TestSubspace> = {
          subspaceRange: {
            start: TestSubspace.Dalton,
            end: TestSubspace.Epson,
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

        const { size: alfieProjectSize } = await storeProjectAlfie
          .summarise(range2);
        const { size: bettyProjectSize } = await storeProjectBetty
          .summarise(range2);

        assertEquals(alfieProjectSize, 20);
        assertEquals(bettyProjectSize, 20);

        let actualSizeProjectAlfie = 0;
        let actualSizeProjectBetty = 0;

        for await (
          const _ of storeProjectAlfie.query({
            area: {
              includedSubspaceId: TestSubspace.Dalton,
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
          actualSizeProjectAlfie += 1;
        }

        for await (
          const _ of storeProjectBetty.query({
            area: {
              includedSubspaceId: TestSubspace.Dalton,
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
          actualSizeProjectBetty += 1;
        }

        assertEquals(actualSizeProjectAlfie, 20);
        assertEquals(actualSizeProjectBetty, 20);

        messengerAlfie.close();
        messengerBetty.close();
      });

      await scenario.dispose();
    },
  );
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
