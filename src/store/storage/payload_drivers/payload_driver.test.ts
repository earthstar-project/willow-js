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
}, () => Promise.resolve());

testPayloadDriver("Filesystem", () => {
  return new PayloadDriverFilesystem("test", testSchemePayload);
}, async () => {
  await Deno.remove("test", { recursive: true });
});

testPayloadDriver("IndexedDB", () => {
  return new PayloadDriverIndexedDb('test', testSchemePayload);
}, () => Promise.resolve());

function testPayloadDriver(
  name: string,
  makeDriver: () => PayloadDriver<ArrayBuffer>,
  dispose: () => Promise<void>,
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
        expectedDigest: actualDigest,
        expectedLength: 16n,
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
        expectedDigest: actualDigest,
        expectedLength: 16n,
        offset: 0,
        payload: new Blob([bytes]).stream(),
      });

      assertEquals(resBasicStream.digest, actualDigest);
      assertEquals(resBasicStream.length, 16n);
    }

    await delay(0);
  });

  Deno.test(`Partial payloads (${name})`, async () => {
    const driver = makeDriver();

    {
      const bytes = crypto.getRandomValues(new Uint8Array(16));
      const actualDigest = await testSchemePayload.fromBytes(bytes);

      // Ingest only half of the bytes.
      const resHalfBytes = await driver.receive({
        expectedDigest: actualDigest,
        expectedLength: 16n,
        offset: 0,
        payload: bytes.subarray(0, 8),
      });

      // Assert that result is what we expect.
      assertNotEquals(resHalfBytes.digest, actualDigest);
      assertEquals(resHalfBytes.length, 8n);

      // If we try to get the known digest from the store, we get nothing.
      const getResult = await driver.get(actualDigest);
      assertEquals(getResult, undefined);

      // If we commit the partial bytes and try to get the known digest from the store, we still get nothing.
      await resHalfBytes.commit(false);
      const getResult2 = await driver.get(actualDigest);
      assertEquals(getResult2, undefined);

      // Ingest the other half of the bytes.
      const resOtherHalfBytes = await driver.receive({
        expectedDigest: actualDigest,
        expectedLength: 16n,
        offset: 8,
        payload: bytes.subarray(8),
      });

      // Assert that final digest and length are same as complete payload
      assertEquals(resOtherHalfBytes.digest, actualDigest);
      assertEquals(resOtherHalfBytes.length, 16n);

      // Assert that we still can't get it from the store until we commit
      const getResult3 = await driver.get(actualDigest);
      assertEquals(getResult3, undefined);

      // Assert that we can get it after committing.
      await resOtherHalfBytes.commit(true);

      const getResult4 = await driver.get(actualDigest);
      assert(getResult4);
    }

    {
      const bytes = crypto.getRandomValues(new Uint8Array(16));
      const actualDigest = await testSchemePayload.fromBytes(bytes);

      // Ingest only the second half of the bytes.
      const resHalfBytes = await driver.receive({
        expectedDigest: actualDigest,
        expectedLength: 16n,
        offset: 8,
        payload: bytes.subarray(8),
      });

      // Assert that result is what we expect (mismatched digest, no explosions).
      assertNotEquals(resHalfBytes.digest, actualDigest);
    }

    await delay(0);

    await dispose();
  });
}
