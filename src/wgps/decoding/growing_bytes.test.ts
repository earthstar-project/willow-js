import { assertEquals } from "https://deno.land/std@0.202.0/assert/mod.ts";
import { FIFO } from "../../../deps.ts";
import { GrowingBytes } from "./growing_bytes.ts";
import { delay } from "https://deno.land/std@0.202.0/async/delay.ts";

Deno.test("GrowingBytes (relative)", async () => {
  const fifo = new FIFO<Uint8Array>();

  const bytes = new GrowingBytes(fifo);

  assertEquals(bytes.array, new Uint8Array());

  fifo.push(new Uint8Array([0]));

  await delay(0);

  assertEquals(bytes.array, new Uint8Array([0]));

  fifo.push(new Uint8Array([1]));
  fifo.push(new Uint8Array([2, 3]));

  await delay(0);

  assertEquals(bytes.array, new Uint8Array([0, 1, 2, 3]));

  let received = new Uint8Array();

  bytes.nextRelative(4).then((bytes) => {
    received = bytes;
  });

  fifo.push(new Uint8Array([4]));
  await delay(0);

  assertEquals(received, new Uint8Array());

  fifo.push(new Uint8Array([5, 6]));
  await delay(0);

  assertEquals(received, new Uint8Array());

  fifo.push(new Uint8Array([7]));
  await delay(0);

  assertEquals(received, new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7]));

  bytes.prune(4);

  assertEquals(bytes.array, new Uint8Array([4, 5, 6, 7]));
});

Deno.test("GrowingBytes (absolute)", async () => {
  const fifo = new FIFO<Uint8Array>();

  const bytes = new GrowingBytes(fifo);

  assertEquals(bytes.array, new Uint8Array());

  fifo.push(new Uint8Array([0]));

  await delay(0);

  assertEquals(bytes.array, new Uint8Array([0]));

  fifo.push(new Uint8Array([1]));
  fifo.push(new Uint8Array([2, 3]));

  await delay(0);

  assertEquals(bytes.array, new Uint8Array([0, 1, 2, 3]));

  let received = new Uint8Array();

  bytes.nextAbsolute(8).then((bytes) => {
    received = bytes;
  });

  fifo.push(new Uint8Array([4]));
  await delay(0);

  assertEquals(received, new Uint8Array());

  fifo.push(new Uint8Array([5, 6]));
  await delay(0);

  assertEquals(received, new Uint8Array());

  fifo.push(new Uint8Array([7]));
  await delay(0);

  assertEquals(received, new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7]));

  bytes.prune(4);

  assertEquals(bytes.array, new Uint8Array([4, 5, 6, 7]));
});
