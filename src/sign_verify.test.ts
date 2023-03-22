import { signEntry, verifyEntry } from "./sign_verify.ts";
import { Entry } from "./types.ts";
import {
  assert,
  assertEquals,
} from "https://deno.land/std@0.177.0/testing/asserts.ts";

function makeKeypair() {
  return crypto.subtle.generateKey(
    {
      name: "ECDSA",
      namedCurve: "P-256",
    },
    true,
    ["sign", "verify"],
  );
}

function exportKey(key: CryptoKey) {
  return window.crypto.subtle.exportKey("raw", key);
}

Deno.test("Signs and verifies", async () => {
  const namespaceKeypair = await makeKeypair();
  const authorKeypair = await makeKeypair();

  const namespacePubkeyBytes = await exportKey(namespaceKeypair.publicKey);
  const authorPubkeyBytes = await exportKey(authorKeypair.publicKey);

  const pathBytes = new Uint8Array([3, 3, 3, 3]).buffer;
  const hashBytes = new Uint8Array([4, 4, 4, 4]).buffer;

  const entry: Entry = {
    identifier: {
      namespace: namespacePubkeyBytes,
      author: authorPubkeyBytes,
      timestamp: BigInt(1679467892424),
      path: pathBytes,
    },
    record: {
      length: BigInt(256),
      hash: hashBytes,
    },
  };

  const signed = await signEntry<CryptoKeyPair>({
    entry,
    namespaceKeypair: namespaceKeypair,
    authorKeypair: authorKeypair,
    sign: (keypair, entryEncoded) => {
      return window.crypto.subtle.sign(
        {
          name: "ECDSA",
          hash: { name: "SHA-256" },
        },
        keypair.privateKey,
        entryEncoded,
      );
    },
  });

  assertEquals(entry, signed.entry);

  const verified = await verifyEntry<CryptoKey>({
    namespacePubkey: namespaceKeypair.publicKey,
    authorPubkey: authorKeypair.publicKey,
    signedEntry: signed,
    verify: (
      publicKey,
      signature,
      encodedEntry,
    ) => {
      return window.crypto.subtle.verify(
        {
          name: "ECDSA",
          hash: { name: "SHA-256" },
        },
        publicKey,
        signature,
        encodedEntry,
      );
    },
  });

  assert(verified);

  // Swap the author / namespace keypairs
  const failedVerification = await verifyEntry<CryptoKey>({
    namespacePubkey: authorKeypair.publicKey,
    authorPubkey: namespaceKeypair.publicKey,
    signedEntry: signed,
    verify: (
      publicKey,
      signature,
      encodedEntry,
    ) => {
      return window.crypto.subtle.verify(
        {
          name: "ECDSA",
          hash: { name: "SHA-256" },
        },
        publicKey,
        signature,
        encodedEntry,
      );
    },
  });

  assert(failedVerification === false);
});
