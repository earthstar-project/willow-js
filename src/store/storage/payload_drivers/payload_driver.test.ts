import type { PayloadDriver } from "../types.ts";
import { collectUint8Arrays } from "./util.ts";
import { PayloadDriverMemory } from "./memory.ts";
import { testSchemePayload } from "../../../test/test_schemes.ts";
import { PayloadDriverFilesystem } from "./filesystem.ts";
import { PayloadDriverIndexedDb } from "./indexeddb.ts";
import "https://deno.land/x/indexeddb@1.3.5/polyfill_memory.ts";
import { notErr } from "../../../errors.ts";
import { assert, assertEquals, assertNotEquals } from "@std/assert";
import { delay } from "@std/async";

testPayloadDriver("Memory", () => {
  return new PayloadDriverMemory(testSchemePayload);
});

testPayloadDriver("Filesystem", () => {
  return new PayloadDriverFilesystem("test", testSchemePayload);
});

testPayloadDriver("IndexedDB", () => {
  return new PayloadDriverIndexedDb(testSchemePayload);
});

function testPayloadDriver(
  name: string,
  makeDriver: () => PayloadDriver<ArrayBuffer>,
) {
  Deno.test(`set and Payload (${name})`, async () => {
    const driver = makeDriver();

    const bytes = crypto.getRandomValues(new Uint8Array(16));

    const { digest, length, payload } = await driver.set(
      bytes,
    );

    assertEquals(length, BigInt(bytes.byteLength));
    assertEquals(await payload.length(), BigInt(bytes.byteLength));
    assertEquals(await payload.bytes(), bytes);
    assertEquals(await payload.bytes(8), bytes.slice(8));
    assertEquals(await collectUint8Arrays(await payload.stream()), bytes);
    assertEquals(
      await collectUint8Arrays(await payload.stream(8)),
      bytes.slice(8),
    );
    assertEquals(digest, await testSchemePayload.fromBytes(bytes));

    await delay(0);
  });

  Deno.test(`get and Payload (${name})`, async () => {
    const driver = makeDriver();

    const bytes = crypto.getRandomValues(new Uint8Array(16));

    await driver.set(bytes);

    const digest = await testSchemePayload.fromBytes(bytes);

    const result = await driver.get(digest);

    assert(result);

    const payloadLength = await result.length();

    assertEquals(payloadLength, BigInt(bytes.byteLength));
    assertEquals(await result.bytes(), bytes);
    assertEquals(await result.bytes(8), bytes.slice(8));
    assertEquals(await collectUint8Arrays(await result.stream()), bytes);
    assertEquals(
      await collectUint8Arrays(await result.stream(8)),
      bytes.slice(8),
    );

    await delay(0);
  });

  Deno.test(`erase (${name})`, async () => {
    const driver = makeDriver();

    const bytes = crypto.getRandomValues(new Uint8Array(16));

    await driver.set(
      bytes,
    );

    const digest = await testSchemePayload.fromBytes(bytes);

    const haveIt = await driver.get(digest);

    assert(haveIt);

    const result = await driver.erase(digest);

    assert(notErr(result));

    const doNotHaveIt = await driver.get(digest);

    assert(doNotHaveIt === undefined);

    await delay(0);
  });

  Deno.test(`length (${name})`, async () => {
    const driver = makeDriver();

    const bytes = crypto.getRandomValues(new Uint8Array(16));

    await driver.set(bytes);

    const digest = await testSchemePayload.fromBytes(bytes);

    const result = await driver.length(digest);

    assertEquals(result, BigInt(bytes.byteLength));

    await delay(0);
  });

  Deno.test(`receive (${name})`, async () => {
    const driver = makeDriver();

    {
      const bytes = crypto.getRandomValues(new Uint8Array(16));
      const actualDigest = await testSchemePayload.fromBytes(bytes);

      const resBasicBytes = await driver.receive({
        knownDigest: actualDigest,
        knownLength: 16n,
        offset: 0,
        payload: bytes,
      });

      assertEquals(resBasicBytes.digest, actualDigest);
      assertEquals(resBasicBytes.length, 16n);
    }

    {
      const bytes = crypto.getRandomValues(new Uint8Array(16));
      const actualDigest = await testSchemePayload.fromBytes(bytes);

      const resBasicStream = await driver.receive({
        knownDigest: actualDigest,
        knownLength: 16n,
        offset: 0,
        payload: new Blob([bytes]).stream(),
      });

      assertEquals(resBasicStream.digest, actualDigest);
      assertEquals(resBasicStream.length, 16n);
    }

    {
      const bytes = crypto.getRandomValues(new Uint8Array(16));
      const actualDigest = await testSchemePayload.fromBytes(bytes);

      const firstRes = await driver.receive({
        knownDigest: actualDigest,
        knownLength: 16n,
        offset: 0,
        payload: bytes.slice(0, 8),
      });

      assertNotEquals(firstRes.digest, actualDigest);
      assertEquals(firstRes.length, 8n);

      const secondRes = await driver.receive({
        knownDigest: actualDigest,
        knownLength: 16n,
        offset: 8,
        payload: bytes.slice(8),
      });

      assertEquals(secondRes.digest, actualDigest);
      assertEquals(secondRes.length, 16n);
    }

    {
      const bytes = crypto.getRandomValues(new Uint8Array(16));
      const actualDigest = await testSchemePayload.fromBytes(bytes);

      const firstRes = await driver.receive({
        knownDigest: actualDigest,
        knownLength: 16n,
        offset: 0,
        payload: new Blob([bytes.slice(0, 8)]).stream(),
      });

      assertNotEquals(firstRes.digest, actualDigest);
      assertEquals(firstRes.length, 8n);

      const secondRes = await driver.receive({
        knownDigest: actualDigest,
        knownLength: 16n,
        offset: 8,
        payload: new Blob([bytes.slice(8)]).stream(),
      });

      assertEquals(secondRes.digest, actualDigest);
      assertEquals(secondRes.length, 16n);
    }

    await delay(0);
  });
}
