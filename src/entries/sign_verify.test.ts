import { signEntry, verifyEntry } from "./sign_verify.ts";
import { Entry, SignedEntry } from "./types.ts";
import {
  assert,
  assertEquals,
} from "https://deno.land/std@0.188.0/testing/asserts.ts";

Deno.test("Signs and verifies", async () => {
  const namespaceKeypair = await makeKeypair();
  const authorKeypair = await makeKeypair();

  const namespacePubkeyBytes = await exportKey(namespaceKeypair.publicKey);
  const authorPubkeyBytes = await exportKey(authorKeypair.publicKey);

  const pathBytes = new Uint8Array([3, 3, 3, 3]);
  const hashBytes = new Uint8Array([4, 4, 4, 4]);

  const entry: Entry = {
    identifier: {
      namespace: new Uint8Array(namespacePubkeyBytes),
      author: new Uint8Array(authorPubkeyBytes),
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
    sign: async (keypair, entryEncoded) => {
      const res = await crypto.subtle.sign(
        {
          name: "ECDSA",
          hash: { name: "SHA-256" },
        },
        keypair.privateKey,
        entryEncoded,
      );

      return new Uint8Array(res);
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
    authorSignature: new Uint8Array([1, 2, 3, 4]),
    namespaceSignature: new Uint8Array([5, 6, 7, 8]),
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
