import {
  NamespaceScheme,
  PathLengthScheme,
  PayloadScheme,
  SubspaceScheme,
} from "../replica/types.ts";
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
    namespaceScheme: namespaceScheme,
    subspaceScheme: subspaceScheme,
    pathLengthScheme: pathLengthScheme,
    payloadScheme: payloadScheme,
  });

  const decoded = decodeEntry(encoded, {
    namespaceScheme: namespaceScheme,
    subspaceScheme: subspaceScheme,
    pathLengthScheme: pathLengthScheme,
    payloadScheme: payloadScheme,
  });

  assertEquals(decoded, entry);
});

const namespaceScheme: NamespaceScheme<number> = {
  encode(namespace) {
    return new Uint8Array([namespace]);
  },
  decode(encoded) {
    return encoded[0];
  },
  encodedLength: () => 1,
  isEqual: (a, b) => a === b,
};

const subspaceScheme: SubspaceScheme<number> = {
  encode(namespace) {
    return new Uint8Array([0, namespace]);
  },
  decode(encoded) {
    return encoded[1];
  },
  encodedLength: () => 2,
  isEqual: (a, b) => a === b,
  minimalSubspaceKey: 0,
  order: (a, b) => {
    if (a < b) return -1;
    else if (a > b) return 1;

    return 0;
  },
  successor: (a) => a + 1,
};

const pathLengthScheme: PathLengthScheme = {
  encode(length) {
    return new Uint8Array([length]);
  },
  decode(encoded) {
    return encoded[0];
  },
  encodedLength() {
    return 1;
  },
  maxLength: 4,
};

const payloadScheme: PayloadScheme<number> = {
  encode(value) {
    return new Uint8Array([0, 0, 0, value]);
  },
  decode(encoded) {
    return encoded[3];
  },
  encodedLength: () => 4,
  fromBytes: () => Promise.resolve(1),
  order: (a, b) => {
    if (a < b) return -1;
    else if (a > b) return 1;

    return 0;
  },
};
