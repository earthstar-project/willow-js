import { assertEquals } from "https://deno.land/std@0.202.0/assert/mod.ts";
import { delay } from "https://deno.land/std@0.202.0/async/delay.ts";
import { GuaranteedQueue } from "./guaranteed_queue.ts";

Deno.test("Guaranteed queue", async () => {
  const received: Uint8Array[] = [];

  const queue = new GuaranteedQueue();

  (async () => {
    for await (const bytes of queue) {
      received.push(bytes);
    }
  })();

  queue.push(new Uint8Array([0]));
  queue.push(new Uint8Array([1, 2, 3]));
  queue.push(new Uint8Array([4]));

  assertEquals(received, []);
  assertEquals(queue.guarantees, BigInt(0));

  queue.addGuarantees(BigInt(1));

  await delay(0);

  assertEquals(received, [new Uint8Array([0])]);
  assertEquals(queue.guarantees, BigInt(0));

  queue.addGuarantees(BigInt(2));

  await delay(0);

  assertEquals(received, [new Uint8Array([0])]);
  assertEquals(queue.guarantees, BigInt(2));

  queue.addGuarantees(BigInt(2));

  await delay(1);

  assertEquals(received, [
    new Uint8Array([0]),
    new Uint8Array([1, 2, 3]),
    new Uint8Array([4]),
  ]);
  assertEquals(queue.guarantees, BigInt(0));

  queue.addGuarantees(BigInt(4));

  queue.push(new Uint8Array([5, 6]));

  await delay(1);

  assertEquals(received, [
    new Uint8Array([0]),
    new Uint8Array([1, 2, 3]),
    new Uint8Array([4]),
    new Uint8Array([5, 6]),
  ]);
  assertEquals(queue.guarantees, BigInt(2));
});

Deno.test("Guaranteed queue (pleading for absolution)", () => {
  const queue = new GuaranteedQueue();

  queue.addGuarantees(BigInt(16));
  queue.addGuarantees(BigInt(16));

  queue.push(new Uint8Array(18));

  const absolved = queue.plead(BigInt(8));

  assertEquals(absolved, BigInt(6));
  assertEquals(queue.guarantees, BigInt(8));
});
