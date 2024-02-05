import { assertEquals } from "https://deno.land/std@0.202.0/assert/assert_equals.ts";
import { Path } from "../../deps.ts";
import { decryptPath, encryptPath } from "./encryption.ts";
import { assertNotEquals } from "https://deno.land/std@0.202.0/assert/assert_not_equals.ts";

type EncryptPathVector = Path;

const encryptPathVectors: EncryptPathVector[] = [
  [],
  [new Uint8Array([1, 2, 3, 4])],
  [new Uint8Array([1]), new Uint8Array([2]), new Uint8Array([3])],
];

Deno.test("encryptPath and decryptPath", async () => {
  const key = await crypto.subtle.importKey(
    "raw",
    crypto.getRandomValues(new Uint8Array(64)),
    "HKDF",
    false, // KDF keys cannot be exported
    ["deriveKey", "deriveBits"],
  );

  const iv = crypto.getRandomValues(new Uint8Array(16));

  const encryptFn = async (key: CryptoKey, component: Uint8Array) => {
    const encryptionKey = await crypto.subtle.deriveKey(
      {
        name: "HKDF",
        hash: "SHA-256",
        salt: new Uint8Array(),
        info: new Uint8Array(),
      },
      key,
      {
        name: "AES-GCM",
        length: 256,
      },
      true,
      ["encrypt"],
    );

    const encrypted = await crypto.subtle.encrypt(
      {
        name: "AES-GCM",
        iv,
      },
      encryptionKey,
      component,
    );

    return new Uint8Array(encrypted);
  };

  const deriveKey = async (key: CryptoKey, component: Uint8Array) => {
    const bits = await crypto.subtle.deriveBits(
      {
        name: "HKDF",
        hash: "SHA-256",
        salt: component,
        info: new Uint8Array(),
      },
      key,
      64,
    );

    return crypto.subtle.importKey(
      "raw",
      bits,
      "HKDF",
      false,
      ["deriveKey", "deriveBits"],
    );
  };

  const decryptFn = async (
    key: CryptoKey,
    encrypted: Uint8Array,
  ) => {
    const decryptionKey = await crypto.subtle.deriveKey(
      {
        name: "HKDF",
        hash: "SHA-256",
        salt: new Uint8Array(),
        info: new Uint8Array(),
      },
      key,
      {
        name: "AES-GCM",
        length: 256,
      },
      true,
      ["decrypt"],
    );

    const decrypted = await crypto.subtle.decrypt(
      {
        name: "AES-GCM",
        iv,
      },
      decryptionKey,
      encrypted,
    );

    return new Uint8Array(decrypted);
  };

  for (const path of encryptPathVectors) {
    const [encryptedPath] = await encryptPath({
      key,
      encryptFn,
      deriveKey,
    }, path);

    const [decryptedPath] = await decryptPath({
      key,
      decryptFn,
      deriveKey,
    }, encryptedPath);

    assertNotEquals(encryptedPath, path);
    assertEquals(decryptedPath, path);
  }
});
