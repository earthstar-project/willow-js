import { assertEquals } from "https://deno.land/std@0.202.0/assert/mod.ts";
import { transportPairInMemory } from "./transports/in_memory.ts";
import { WgpsMessenger } from "./wgps_messenger.ts";
import {
  TestNamespace,
  TestReadCap,
  testSchemeAccessControl,
  testSchemeAuthorisationToken,
  testSchemeNamespace,
  testSchemePai,
  testSchemePath,
  testSchemeSubspace,
  testSchemeSubspaceCap,
  TestSubspace,
} from "../test/test_schemes.ts";
import { delay } from "https://deno.land/std@0.202.0/async/delay.ts";
import { OPEN_END } from "../../deps.ts";

Deno.test("WgpsMessenger establishes challenge", async () => {
  const [alfie, betty] = transportPairInMemory();

  const challengeHash = async (bytes: Uint8Array) => {
    return new Uint8Array(await crypto.subtle.digest("SHA-256", bytes));
  };

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

  await delay(10);

  // @ts-ignore looking at private values is fine
  const receivedCapsAlfie = Array.from(messengerAlfie.handlesCapsTheirs).map((
    [, cap],
  ) => cap);
  // @ts-ignore looking at private values is fine
  const receivedCapsBetty = Array.from(messengerBetty.handlesCapsTheirs).map((
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
});
