import { signEntry, verifyEntry } from "./sign_verify.ts";
import { Entry, SignedEntry } from "../types.ts";
import {
  assert,
  assertEquals,
} from "https://deno.land/std@0.177.0/testing/asserts.ts";

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
      path: pathBytes,
    },
    record: {
      timestamp: BigInt(1679467892424),
      length: BigInt(256),
      hash: hashBytes,
    },
  };

  const signed = await signEntry<CryptoKeyPair>({
    entry,
    namespaceKeypair: namespaceKeypair,
    authorKeypair: authorKeypair,
    sign: (keypair, entryEncoded) => {
      return crypto.subtle.sign(
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

  const verified = await verifyEntry({
    signedEntry: signed,
    verify: async (
      publicKey,
      signature,
      encodedEntry,
    ) => {
      const cryptoKey = await importPublicKey(publicKey);

      return crypto.subtle.verify(
        {
          name: "ECDSA",
          hash: { name: "SHA-256" },
        },
        cryptoKey,
        signature,
        encodedEntry,
      );
    },
  });

  assert(verified);

  const badSigned: SignedEntry = {
    entry: signed.entry,
    authorSignature: new Uint8Array([1, 2, 3, 4]).buffer,
    namespaceSignature: new Uint8Array([5, 6, 7, 8]).buffer,
  };

  const failedVerification = await verifyEntry({
    signedEntry: badSigned,
    verify: async (
      publicKey,
      signature,
      encodedEntry,
    ) => {
      const cryptoKey = await importPublicKey(publicKey);

      return crypto.subtle.verify(
        {
          name: "ECDSA",
          hash: { name: "SHA-256" },
        },
        cryptoKey,
        signature,
        encodedEntry,
      );
    },
  });

  assert(failedVerification === false);
});

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

function importPublicKey(raw: ArrayBuffer) {
  return crypto.subtle.importKey(
    "raw",
    raw,
    {
      name: "ECDSA",
      namedCurve: "P-256",
    },
    true,
    ["verify"],
  );
}

function exportKey(key: CryptoKey) {
  return window.crypto.subtle.exportKey("raw", key);
}
