import { Replica } from "../src/replica/replica.ts";
import { crypto } from "$std/crypto/mod.ts";
import { equals as bytesEquals } from "$std/bytes/equals.ts";
import { ProtocolParameters } from "../src/replica/types.ts";
import { getPersistedDrivers } from "../src/replica/util.ts";
import { compareBytes } from "../src/util/bytes.ts";
import { encodeEntry } from "../src/entries/encode_decode.ts";

async function makeKeypair() {
  const { publicKey, privateKey } = await crypto.subtle.generateKey(
    {
      name: "ECDSA",
      namedCurve: "P-256",
    },
    true,
    ["sign", "verify"],
  );

  return {
    subspace: new Uint8Array(
      await window.crypto.subtle.exportKey("raw", publicKey),
    ),
    privateKey,
  };
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

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

const authorPair = await makeKeypair();
const author2Pair = await makeKeypair();

const protocolParameters: ProtocolParameters<
  Uint8Array,
  Uint8Array,
  ArrayBuffer,
  CryptoKey,
  ArrayBuffer
> = {
  namespaceScheme: {
    encode: (v) => v,
    decode: (v) => v,
    encodedLength: (v) => v.byteLength,
    isEqual: bytesEquals,
  },
  subspaceScheme: {
    encode: (v) => v,
    decode: (v) => v.subarray(0, 65),
    encodedLength: () => 65,
    isEqual: bytesEquals,
  },
  pathLengthEncoding: {
    encode(length) {
      return new Uint8Array([length]);
    },
    decode(bytes) {
      return bytes[0];
    },
    encodedLength() {
      return 1;
    },
  },
  payloadScheme: {
    encode(hash) {
      return new Uint8Array(hash);
    },
    decode(bytes) {
      return bytes.subarray(0, 32);
    },
    encodedLength() {
      return 32;
    },
    async fromBytes(bytes) {
      return new Uint8Array(await crypto.subtle.digest("SHA-256", bytes));
    },
    order(a, b) {
      return compareBytes(new Uint8Array(a), new Uint8Array(b)) as 1 | 0 | -1;
    },
  },
  authorisationScheme: {
    async authorise(entry, secretKey) {
      const encodedEntry = encodeEntry(entry, {
        namespacePublicKeyEncoding: {
          encode: (v) => v,
          decode: (v) => v,
          encodedLength: (v) => v.byteLength,
        },
        subspacePublicKeyEncoding: {
          encode: (v) => v,
          decode: (v) => v,
          encodedLength: (v) => v.byteLength,
        },
        pathEncoding: {
          encode(path) {
            const bytes = new Uint8Array(1 + path.byteLength);
            bytes[0] = path.byteLength;

            bytes.set(path, 1);
            return bytes;
          },
          decode(bytes) {
            const length = bytes[0];
            return bytes.subarray(1, 1 + length);
          },
          encodedLength(path) {
            return 1 + path.byteLength;
          },
        },
        payloadDigestEncoding: {
          encode(hash) {
            return new Uint8Array(hash);
          },
          decode(bytes) {
            return bytes.buffer;
          },
          encodedLength(hash) {
            return hash.byteLength;
          },
        },
      });

      const res = await crypto.subtle.sign(
        {
          name: "ECDSA",
          hash: { name: "SHA-256" },
        },
        secretKey,
        encodedEntry,
      );

      return new Uint8Array(res);
    },
    async isAuthorised(entry, token) {
      const cryptoKey = await importPublicKey(entry.identifier.subspace);

      const encodedEntry = encodeEntry(entry, {
        namespacePublicKeyEncoding: {
          encode: (v) => v,
          decode: (v) => v,
          encodedLength: (v) => v.byteLength,
        },
        subspacePublicKeyEncoding: {
          encode: (v) => v,
          decode: (v) => v,
          encodedLength: (v) => v.byteLength,
        },
        pathEncoding: {
          encode(path) {
            const bytes = new Uint8Array(1 + path.byteLength);
            bytes[0] = path.byteLength;

            bytes.set(path, 1);
            return bytes;
          },
          decode(bytes) {
            const length = bytes[0];
            return bytes.subarray(1, 1 + length);
          },
          encodedLength(path) {
            return 1 + path.byteLength;
          },
        },
        payloadDigestEncoding: {
          encode(hash) {
            return new Uint8Array(hash);
          },
          decode(bytes) {
            return bytes.buffer;
          },
          encodedLength(hash) {
            return hash.byteLength;
          },
        },
      });

      return crypto.subtle.verify(
        {
          name: "ECDSA",
          hash: { name: "SHA-256" },
        },
        cryptoKey,
        token,
        encodedEntry,
      );
    },
    tokenEncoding: {
      encode: (ab) => new Uint8Array(ab),
      decode: (bytes) => bytes.buffer,
      encodedLength: (ab) => ab.byteLength,
    },
  },
};

const drivers = await getPersistedDrivers(
  "./debug/replica_test",
  protocolParameters,
);

const replica = new Replica<
  Uint8Array,
  Uint8Array,
  ArrayBuffer,
  CryptoKey,
  ArrayBuffer
>({
  namespace: new Uint8Array(new Uint8Array([137])),
  protocolParameters,
  //...drivers,
});

// Won't be inserted
await replica.set({
  path: textEncoder.encode("unauthorised"),
  payload: textEncoder.encode("I should really not be here!"),
  subspace: authorPair.subspace,
}, author2Pair.privateKey);

// Two entries at the same path by different authors
// Both will be inserted!
await replica.set({
  path: textEncoder.encode("pathA"),
  payload: textEncoder.encode("I'm here!"),
  subspace: authorPair.subspace,
}, authorPair.privateKey);

await replica.set({
  path: textEncoder.encode("pathA"),
  payload: textEncoder.encode("Me too!"),
  subspace: author2Pair.subspace,
}, author2Pair.privateKey);

// Two entries at another path, but by the same author.
// Only the second one will remain, it being later!
await replica.set({
  path: textEncoder.encode("pathB"),
  payload: textEncoder.encode("I want to win... and shouldn't be here."),
  subspace: authorPair.subspace,
}, authorPair.privateKey);

await replica.set({
  path: textEncoder.encode("pathB"),
  payload: textEncoder.encode("I win!"),
  subspace: authorPair.subspace,
}, authorPair.privateKey);

// The first and second will be removed!

await replica.set({
  path: textEncoder.encode("prefixed"),
  payload: textEncoder.encode("I am not newest... and shouldn't be here."),
  subspace: authorPair.subspace,
}, authorPair.privateKey);

await replica.set({
  path: textEncoder.encode("prefixed2"),
  payload: textEncoder.encode(
    "I am not newest either... and shouldn't be here.",
  ),
  subspace: authorPair.subspace,
}, authorPair.privateKey);

await replica.set({
  path: textEncoder.encode("prefix"),
  payload: textEncoder.encode("I'm the newest, and a prefix of the others!"),
  subspace: authorPair.subspace,
}, authorPair.privateKey);

// The second one won't be inserted!

await replica.set({
  path: textEncoder.encode("willbe"),
  payload: textEncoder.encode(
    "I am still the newest prefix!",
  ),
  timestamp: BigInt((Date.now() + 10) * 1000),
  subspace: authorPair.subspace,
}, authorPair.privateKey);

await replica.set({
  path: textEncoder.encode("willbeprefixed"),
  payload: textEncoder.encode("I shouldn't be here..."),
  subspace: authorPair.subspace,
}, authorPair.privateKey);

console.group("All entries");

for await (
  const [entry, payload, authToken] of replica.query({
    order: "path",
  })
) {
  const pathName = textDecoder.decode(entry.identifier.path);

  console.group(`${pathName}`);
  console.log(
    `Subspace: ${entry.identifier.subspace.slice(0, 4)} (etc. etc.)`,
  );
  console.log(`Timestamp: ${entry.record.timestamp}`);

  console.log(`Auth token: ${new Uint8Array(authToken).slice(0, 4)}... etc.`);
  console.log(
    `Payload: ${
      payload ? textDecoder.decode(await payload.bytes()) : "Not in possession"
    }`,
  );
  console.groupEnd();
}

console.groupEnd();
