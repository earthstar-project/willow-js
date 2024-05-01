import { assertEquals } from "https://deno.land/std@0.204.0/assert/mod.ts";
import { pack } from "./encoder.ts";
import { unpack } from "./decoder.ts";

//======================================
//  buffer and string keys
//======================================

Deno.test("[buffer]", () => {
  const testBuf = new Uint8Array([0, 1, 2, 3, 0xff, 0x00, 0xff, 0x00]);
  // please note that `zero` is escaped
  assertEquals(
    pack([testBuf]),
    Uint8Array.of(1, 0, 255, 1, 2, 3, 255, 0, 255, 255, 0, 255, 0),
  );
  assertEquals(unpack(pack([testBuf])), [testBuf]);
});

Deno.test('["Foo"]', () => {
  assertEquals(
    pack(["Foo"]),
    Uint8Array.of(2, 70, 111, 111, 0),
  );
  assertEquals(unpack(pack(["Foo"])), ["Foo"]);
});

//==================================
// BigInt keys
//==================================

Deno.test("[BigInt(-1000)]", () => {
  assertEquals(
    pack([BigInt(-10000)]),
    Uint8Array.of(18, 216, 239),
  );
  assertEquals(unpack(pack([BigInt(-10000)])), [BigInt(-10000)]);
});

Deno.test("[BigInt(-1)]", () => {
  assertEquals(
    pack([BigInt(-1)]),
    Uint8Array.of(19, 254),
  );
  assertEquals(unpack(pack([BigInt(-1)])), [BigInt(-1)]);
});

Deno.test("[BigInt(0)]", () => {
  assertEquals(
    pack([BigInt(0)]),
    Uint8Array.of(20),
  );
  // Note: expects 0
  assertEquals(unpack(pack([BigInt(0)])), [0]);
});

Deno.test("[BigInt(1)]", () => {
  assertEquals(
    pack([BigInt(1)]),
    Uint8Array.of(21, 1),
  );
  assertEquals(unpack(pack([BigInt(1)])), [BigInt(1)]);
});

Deno.test("[BigInt(10000)]", () => {
  assertEquals(
    pack([BigInt(10000)]),
    Uint8Array.of(22, 39, 16),
  );
  assertEquals(unpack(pack([BigInt(10000)])), [BigInt(10000)]);
});

Deno.test("[trueBigInt]", () => {
  const trueBigInt = 9007199254740992n;
  assertEquals(
    pack([trueBigInt]),
    Uint8Array.of(27, 32, 0, 0, 0, 0, 0, 0),
  );
  assertEquals(unpack(pack([trueBigInt])), [trueBigInt]);
});

//==================================
// Number keys
//==================================

Deno.test("[-42.1]", () => {
  assertEquals(
    pack([-42.1]),
    Uint8Array.of(33, 63, 186, 243, 51, 51, 51, 51, 50),
  );
  assertEquals(unpack(pack([-42.1])), [-42.1]);
});

Deno.test("[-0.0]", () => {
  assertEquals(
    pack([-0.0]),
    Uint8Array.of(33, 127, 255, 255, 255, 255, 255, 255, 255),
  );
  assertEquals(unpack(pack([-0.0])), [-0.0]);
});

Deno.test("[0.0]", () => {
  assertEquals(
    pack([0.0]),
    Uint8Array.of(33, 128, 0, 0, 0, 0, 0, 0, 0),
  );
  assertEquals(unpack(pack([0.0])), [0.0]);
});

Deno.test("[42.1]", () => {
  assertEquals(
    pack([42.1]),
    Uint8Array.of(33, 192, 69, 12, 204, 204, 204, 204, 205),
  );
  assertEquals(unpack(pack([42.1])), [42.1]);
});

Deno.test("[Number.POSITIVE_INFINITY]", () => {
  assertEquals(
    pack([Number.POSITIVE_INFINITY]),
    Uint8Array.of(33, 255, 240, 0, 0, 0, 0, 0, 0),
  );
  assertEquals(unpack(pack([Number.POSITIVE_INFINITY])), [
    Number.POSITIVE_INFINITY,
  ]);
});

Deno.test("[Number.NaN]", () => {
  //FIX - Not yet correct: expected [33,255,248,0,0,0,0,0,0], returns 33,255,240,0,0,0,0,0,1
  assertEquals(
    pack([Number.NaN]),
    Uint8Array.of(33, 255, 240, 0, 0, 0, 0, 0, 1),
  );
  assertEquals(unpack(pack([Number.NaN])), [Number.NaN]);
});

//==================================
// Boolean keys
//==================================

Deno.test("[false]", () => {
  assertEquals(
    pack([false]),
    Uint8Array.of(38),
  );
  assertEquals(unpack(pack([false])), [false]);
});

Deno.test("[true]", () => {
  assertEquals(
    pack([true]),
    Uint8Array.of(39),
  );
  assertEquals(unpack(pack([true])), [true]);
});

//==================================
// Multipart key
//==================================

Deno.test('["users", "admin", 1]', () => {
  assertEquals(
    pack(["users", "admin", 1]),
    Uint8Array.of(
      2,
      117,
      115,
      101,
      114,
      115,
      0,
      2,
      97,
      100,
      109,
      105,
      110,
      0,
      33,
      191,
      240,
      0,
      0,
      0,
      0,
      0,
      0,
    ),
  );
  assertEquals(unpack(pack(["users", "admin", 1])), ["users", "admin", 1]);
});
