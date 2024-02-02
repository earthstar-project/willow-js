import {
  encodeEntry,
  equalsBytes,
  orderBytes,
  PathScheme,
  successorBytesFixedWidth,
} from "../../deps.ts";
import { crypto } from "https://deno.land/std@0.188.0/crypto/crypto.ts";
import {
  AuthorisationScheme,
  FingerprintScheme,
  NamespaceScheme,
  PayloadScheme,
  SubspaceScheme,
} from "../replica/types.ts";
import { importPublicKey } from "./crypto.ts";

export const testSchemeNamespace: NamespaceScheme<Uint8Array> = {
  encode: (v) => v,
  decode: (v) => v,
  encodedLength: (v) => v.byteLength,
  isEqual: equalsBytes,
};

export const testSchemeSubspace: SubspaceScheme<Uint8Array> = {
  encode: (v) => v,
  decode: (v) => v.subarray(0, 65),
  encodedLength: () => 65,
  minimalSubspaceKey: new Uint8Array(65),
  order: orderBytes,
  successor: successorBytesFixedWidth,
};

export const testSchemePath: PathScheme = {
  maxPathLength: 8,
  maxComponentCount: 4,
  maxComponentLength: 3,
};

export const testSchemePayload: PayloadScheme<ArrayBuffer> = {
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
    return orderBytes(new Uint8Array(a), new Uint8Array(b)) as
      | 1
      | 0
      | -1;
  },
};

export const testSchemeFingerprint: FingerprintScheme<
  Uint8Array,
  Uint8Array,
  Uint8Array,
  Uint8Array
> = {
  neutral: new Uint8Array(32),
  async fingerprintSingleton(entry) {
    const encodedEntry = encodeEntry({
      namespaceScheme: testSchemeNamespace,
      subspaceScheme: testSchemeSubspace,
      pathScheme: testSchemePath,
      payloadScheme: testSchemePayload,
    }, entry);

    return new Uint8Array(await crypto.subtle.digest("SHA-256", encodedEntry));
  },
  fingerprintCombine(a, b) {
    const bytes = new Uint8Array(32);

    for (let i = 0; i < 32; i++) {
      bytes.set([a[i] ^ b[i]], i);
    }

    return bytes;
  },
};

export const testSchemeAuthorisation: AuthorisationScheme<
  Uint8Array,
  Uint8Array,
  ArrayBuffer,
  CryptoKey,
  ArrayBuffer
> = {
  async authorise(entry, secretKey) {
    const encodedEntry = encodeEntry({
      namespaceScheme: testSchemeNamespace,
      subspaceScheme: testSchemeSubspace,
      pathScheme: testSchemePath,
      payloadScheme: testSchemePayload,
    }, entry);

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
  async isAuthorisedWrite(entry, token) {
    const cryptoKey = await importPublicKey(entry.subspaceId);

    const encodedEntry = encodeEntry({
      namespaceScheme: testSchemeNamespace,
      subspaceScheme: testSchemeSubspace,
      pathScheme: testSchemePath,
      payloadScheme: testSchemePayload,
    }, entry);

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
};
