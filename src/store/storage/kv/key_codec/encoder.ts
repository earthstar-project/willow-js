import type { KeyPart } from "./types.ts";
import { encodeBigInt, isBigInt } from "./bigIntCodec.ts";
import { encodeDouble } from "./doubleCodec.ts";
import { Accumulator } from "./accumulator.ts";
import * as TYPE from "./types.ts";

//===========================================
//  Encode a multipart key to a byte array
//===========================================

/**
 * Internal method. Please use pack() below
 */
const encodeKey = (accumulator: Accumulator, item: KeyPart) => {
  if (item === undefined) {
    throw new TypeError("Packed element cannot be undefined");
  } else if (item === null) accumulator.appendByte(TYPE.NULL);
  else if (item === false) accumulator.appendByte(TYPE.FALSE);
  else if (item === true) accumulator.appendByte(TYPE.TRUE);
  else if (item.constructor === Uint8Array || typeof item === "string") {
    let itemBuf: Uint8Array;
    if (typeof item === "string") {
      itemBuf = new TextEncoder().encode(item);
      accumulator.appendByte(TYPE.STRING);
    } else {
      itemBuf = item;
      accumulator.appendByte(TYPE.BYTES);
    }

    for (let i = 0; i < itemBuf.length; i++) {
      const val = itemBuf[i];
      accumulator.appendByte(val);
      if (val === 0) { // escape zeros
        accumulator.appendByte(0xff);
      }
    }
    accumulator.appendByte(0);
  } else if (Array.isArray(item)) {
    // Embedded child tuple.
    throw new Error("Nested Tuples are not supported!");
  } else if (typeof item === "number") {
    // Encode as a double precision float.
    encodeDouble(accumulator, item);
  } else if (isBigInt(item)) {
    // Encode as a BigInt
    encodeBigInt(accumulator, item as bigint);
  } else {
    throw new TypeError("Packed items must be an array!");
  }
};

function packRawKey(part: KeyPart[]): Uint8Array {
  if (
    part === undefined ||
    Array.isArray(part) &&
      part.length === 0
  ) return new Uint8Array(0);

  if (!Array.isArray(part)) {
    throw new TypeError("pack must be called with an array");
  }

  const accumulator = new Accumulator();

  for (let i = 0; i < part.length; i++) {
    encodeKey(accumulator, part[i]);
  }

  // finally we extract and return our trimmed buffer
  return accumulator.extract();
}

/**
 * pack() -- the main entry point
 * Encode the specified item or array of items into a buffer.
 * `pack(x)` is equivalent to `pack([x])` if x is not itself an array.
 * Packing single items this way is also slightly more efficient.
 */
export function pack(parts: KeyPart | KeyPart[]): Uint8Array {
  // converts a single part to an array
  if (!Array.isArray(parts)) parts = [parts];
  const packedKey = packRawKey(parts);
  return packedKey;
}
