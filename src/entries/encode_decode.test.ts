import { concat } from "$std/bytes/concat.ts";
import { EncodingScheme } from "../replica/types.ts";
import { decodeEntry, encodeEntry } from "./encode_decode.ts";
import { Entry } from "./types.ts";
import { assertEquals } from "https://deno.land/std@0.188.0/testing/asserts.ts";

Deno.test("Encodes and decodes", () => {
  const entry: Entry<number, number, number> = {
    identifier: {
      namespace: 1,
      subspace: 2,
      path: new Uint8Array([3, 3, 3, 3]),
    },
    record: {
      timestamp: BigInt(1679467892424),
      length: BigInt(256),
      hash: 4,
    },
  };

  const encoded = encodeEntry(entry, {
    namespacePublicKeyEncoding: namespaceEncoding,
    subspacePublicKeyEncoding: subspaceEncoding,
    pathEncoding,
    payloadDigestEncoding,
  });

  const decoded = decodeEntry(encoded, {
    namespacePublicKeyEncoding: namespaceEncoding,
    subspacePublicKeyEncoding: subspaceEncoding,
    pathEncoding,
    payloadDigestEncoding,
  });

  assertEquals(decoded, entry);
});

const namespaceEncoding: EncodingScheme<number> = {
  encode(namespace) {
    return new Uint8Array([namespace]);
  },
  decode(encoded) {
    return encoded[0];
  },
  encodedLength: () => 1,
};

const subspaceEncoding: EncodingScheme<number> = {
  encode(namespace) {
    return new Uint8Array([0, namespace]);
  },
  decode(encoded) {
    return encoded[1];
  },
  encodedLength: () => 2,
};

const pathEncoding: EncodingScheme<Uint8Array> = {
  encode(value) {
    return concat(new Uint8Array([value.byteLength]), value);
  },
  decode(encoded) {
    const length = encoded[0];

    return encoded.subarray(1, 1 + length);
  },
  encodedLength(value) {
    return value.byteLength + 1;
  },
};

const payloadDigestEncoding: EncodingScheme<number> = {
  encode(value) {
    return new Uint8Array([0, 0, 0, value]);
  },
  decode(encoded) {
    return encoded[3];
  },
  encodedLength: () => 4,
};
