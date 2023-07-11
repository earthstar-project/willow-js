import { assertEquals } from "https://deno.land/std@0.158.0/testing/asserts.ts";
import { compareBytes } from "./bytes.ts";

type TestVector = [number[], number[], number];

const testVectors: TestVector[] = [
  // Equal
  [[0], [0], 0],
  [[0, 1], [0, 1], 0],
  // Greater than
  [[1], [0], 1],
  [[1], [0, 1], 1],
  [[1, 1], [0, 1], 1],
  [[1, 1], [1], 1],
  // Less than
  [[0], [1], -1],
  [[0], [1, 2], -1],
  [[0, 1, 2], [1, 2, 3], -1],
  [[0, 1, 2], [1], -1],
];

Deno.test("compareBytes", () => {
  for (const vector of testVectors) {
    const a = new Uint8Array(vector[0]);
    const b = new Uint8Array(vector[1]);
    const res = compareBytes(a, b);

    assertEquals(
      res,
      vector[2],
      `[${a}] <> [${b}] should be ${vector[2]}, but is ${res}`,
    );
  }
});
