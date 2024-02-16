import { assertEquals } from "https://deno.land/std@0.202.0/assert/mod.ts";
import { delay } from "https://deno.land/std@0.202.0/async/mod.ts";
import { ReadyTransport } from "./ready_transport.ts";
import { transportPairInMemory } from "./transports/in_memory.ts";
import { concat } from "../../deps.ts";

Deno.test("Ready transport receives max payload ", async () => {
  // Happy path
  {
    const [alfie, betty] = transportPairInMemory();

    const readyTransport = new ReadyTransport({
      transport: alfie,
      challengeHashLength: 4,
    });

    let received = new Uint8Array();

    (async () => {
      for await (const bytes of readyTransport) {
        received = concat(received, bytes);
      }
    })();

    await betty.send(new Uint8Array([8]));
    await betty.send(new Uint8Array([1, 2, 3, 4]));
    await betty.send(new Uint8Array([7, 7, 7, 7]));

    const maxPayloadSize = await readyTransport.maximumPayloadSize;
    const receivedCommitment = await readyTransport.receivedCommitment;

    assertEquals(maxPayloadSize, BigInt(256));
    assertEquals(receivedCommitment, new Uint8Array([1, 2, 3, 4]));

    await delay(0);

    assertEquals(received, new Uint8Array([7, 7, 7, 7]));
  }

  // All at once.
  {
    const [alfie, betty] = transportPairInMemory();

    const readyTransport = new ReadyTransport({
      transport: alfie,
      challengeHashLength: 4,
    });

    let received = new Uint8Array();

    (async () => {
      for await (const bytes of readyTransport) {
        received = concat(received, bytes);
      }
    })();

    await betty.send(new Uint8Array([8, 1, 2, 3, 4, 7, 7, 7, 7]));

    const maxPayloadSize = await readyTransport.maximumPayloadSize;
    const receivedCommitment = await readyTransport.receivedCommitment;

    assertEquals(maxPayloadSize, BigInt(256));
    assertEquals(receivedCommitment, new Uint8Array([1, 2, 3, 4]));

    await delay(0);

    assertEquals(received, new Uint8Array([7, 7, 7, 7]));
  }

  // Partial commitment.
  {
    const [alfie, betty] = transportPairInMemory();

    const readyTransport = new ReadyTransport({
      transport: alfie,
      challengeHashLength: 4,
    });

    let received = new Uint8Array();

    (async () => {
      for await (const bytes of readyTransport) {
        received = concat(received, bytes);
      }
    })();

    await betty.send(new Uint8Array([8, 1, 2]));
    await betty.send(new Uint8Array([3, 4, 7, 7, 7, 7]));

    const maxPayloadSize = await readyTransport.maximumPayloadSize;
    const receivedCommitment = await readyTransport.receivedCommitment;

    assertEquals(maxPayloadSize, BigInt(256));
    assertEquals(receivedCommitment, new Uint8Array([1, 2, 3, 4]));

    await delay(0);

    assertEquals(received, new Uint8Array([7, 7, 7, 7]));
  }

  // Even more partial commitment.
  {
    const [alfie, betty] = transportPairInMemory();

    const readyTransport = new ReadyTransport({
      transport: alfie,
      challengeHashLength: 4,
    });

    let received = new Uint8Array();

    (async () => {
      for await (const bytes of readyTransport) {
        received = concat(received, bytes);
      }
    })();

    await betty.send(new Uint8Array([8, 1, 2]));
    await betty.send(new Uint8Array([3]));
    await betty.send(new Uint8Array([4, 7, 7, 7, 7]));

    const maxPayloadSize = await readyTransport.maximumPayloadSize;
    const receivedCommitment = await readyTransport.receivedCommitment;

    assertEquals(maxPayloadSize, BigInt(256));
    assertEquals(receivedCommitment, new Uint8Array([1, 2, 3, 4]));

    await delay(0);

    assertEquals(received, new Uint8Array([7, 7, 7, 7]));
  }
});
