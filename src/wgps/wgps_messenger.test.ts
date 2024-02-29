import { assertEquals } from "https://deno.land/std@0.202.0/assert/mod.ts";
import { transportPairInMemory } from "./transports/in_memory.ts";
import { WgpsMessenger } from "./wgps_messenger.ts";
import {
  TestNamespace,
  testSchemeNamespace,
  testSchemePai,
  testSchemeSubspaceCap,
  TestSubspace,
} from "../test/test_schemes.ts";
import { delay } from "https://deno.land/std@0.202.0/async/delay.ts";

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
    subspaceCapScheme: testSchemeSubspaceCap,
    namespaceScheme: testSchemeNamespace,
    paiScheme: testSchemePai,
    readAuthorisations: [
      {
        capability: {
          namespace: TestNamespace.Family,
          subspace: TestSubspace.Gemma,
          path: [new Uint8Array([1])],
          receiver: TestSubspace.Alfie,
        },
        signature: TestSubspace.Alfie,
      },
    ],
  });

  const messengerBetty = new WgpsMessenger({
    challengeHash,
    challengeLength: 128,
    challengeHashLength: 32,
    maxPayloadSizePower: 8,
    transport: betty,
    subspaceCapScheme: testSchemeSubspaceCap,
    namespaceScheme: testSchemeNamespace,
    paiScheme: testSchemePai,
    readAuthorisations: [
      {
        capability: {
          namespace: TestNamespace.Family,
          subspace: TestSubspace.Gemma,
          path: [new Uint8Array([1]), new Uint8Array([2])],
          receiver: TestSubspace.Betty,
        },
        signature: TestSubspace.Betty,
      },
    ],
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
});
