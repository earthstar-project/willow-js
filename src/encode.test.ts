import { decodeEntry, encodeEntry } from "./encode.ts";
import { Entry } from "./types.ts";

import { assertEquals } from "https://deno.land/std@0.177.0/testing/asserts.ts";

Deno.test("Encodes and decodes", () => {
  const namespaceBytes = new Uint8Array([1, 1, 1, 1]);
  const authorBytes = new Uint8Array([2, 2, 2, 2]);
  const pathBytes = new Uint8Array([3, 3, 3, 3]);
  const hashBytes = new Uint8Array([4, 4, 4, 4]);

  const entry: Entry = {
    identifier: {
      namespace: namespaceBytes,
      author: authorBytes,
      timestamp: BigInt(1679467892424),
      path: pathBytes,
    },
    record: {
      length: BigInt(256),
      hash: hashBytes,
    },
  };

  const encoded = encodeEntry(entry);

  const decoded = decodeEntry(encoded, { digestLength: 4, pubKeyLength: 4 });

  assertEquals(decoded, entry);
});
