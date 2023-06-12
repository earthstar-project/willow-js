import { Replica } from "../src/replica/replica.ts";
import { crypto } from "https://deno.land/std@0.188.0/crypto/crypto.ts";

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

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

const namespacePair = await makeKeypair();
const authorPair = await makeKeypair();
const author2Pair = await makeKeypair();

const replica = new Replica<CryptoKeyPair>({
  namespace: new Uint8Array(await exportKey(namespacePair.publicKey)),
  format: {
    // Ignore, not being used yet.
    hashLength: 32,
    pubkeyLength: 65,
    signatureLength: 64,
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
    hash: async (bytes: Uint8Array | ReadableStream<Uint8Array>) => {
      return new Uint8Array(await crypto.subtle.digest("SHA-256", bytes));
    },
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
    async pubkeyBytesFromPair(pair) {
      const arrayBuffer = await exportKey(pair.publicKey);

      return new Uint8Array(arrayBuffer);
    },
  },
});

// Two entries at the same path by different authors
// Both will be inserted!
await replica.set(namespacePair, authorPair, {
  path: textEncoder.encode("pathA"),
  payload: textEncoder.encode("I'm here!"),
});

await replica.set(namespacePair, author2Pair, {
  path: textEncoder.encode("pathA"),
  payload: textEncoder.encode("Me too!"),
});

// Two entries at a different path, but by the same author.
// Only the second one will remain!
await replica.set(namespacePair, authorPair, {
  path: textEncoder.encode("pathB"),
  payload: textEncoder.encode("I want to win..."),
});

await replica.set(namespacePair, authorPair, {
  path: textEncoder.encode("pathB"),
  payload: textEncoder.encode("I win!"),
});

console.group("All entries");

for await (
  const [signed, payload] of replica.query({
    order: "path",
  })
) {
  const pathName = textDecoder.decode(signed.entry.identifier.path);

  console.group(`${pathName}`);
  console.log(
    `Author: ${signed.entry.identifier.author.slice(0, 4)} (etc. etc.)`,
  );
  console.log(`Timestamp: ${signed.entry.record.timestamp}`);
  console.log(
    `Namespace sig: ${signed.namespaceSignature.slice(0, 4)}... etc.`,
  );
  console.log(`Author sig: ${signed.authorSignature.slice(0, 4)}... etc.`);
  console.log(
    `Payload: ${
      payload ? textDecoder.decode(await payload.bytes()) : "Not in possession"
    }`,
  );
  console.groupEnd();
}

console.groupEnd();
