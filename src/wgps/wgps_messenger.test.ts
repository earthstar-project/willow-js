import { assertEquals } from "https://deno.land/std@0.202.0/assert/mod.ts";
import { transportPairInMemory } from "./transports/in_memory.ts";
import { WgpsMessenger } from "./wgps_messenger.ts";

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
  });

  const messengerBetty = new WgpsMessenger({
    challengeHash,
    challengeLength: 128,
    challengeHashLength: 32,
    maxPayloadSizePower: 8,
    transport: betty,
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
});
