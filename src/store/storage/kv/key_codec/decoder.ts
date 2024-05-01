import { KeyPart, TYPE_CODE } from "./types.ts";
import { decodeBigInt } from "./bigIntCodec.ts";
import { decodeDouble } from "./doubleCodec.ts";
import { Accumulator } from "./accumulator.ts";

import {
  BYTES,
  DOUBLE,
  ESCAPE,
  FALSE,
  NEGINTSTART,
  POSINTEND,
  STRING,
  TRUE,
} from "./types.ts";

//===========================================
//  Decode a byte array to a multipart key
//===========================================

/**
 *  Internal method. Please use unpack() below.
 */
function decodeKey(
  buf: Uint8Array,
  pos: { p: number },
): KeyPart {
  const code = buf[pos.p++] as TYPE_CODE;
  let { p } = pos;

  /** unpacks by type */
  switch (code) {
    case FALSE:
      return false;
    case TRUE:
      return true;
    case BYTES: {
      const accumulator = new Accumulator();
      for (; p < buf.length; p++) {
        const byte = buf[p];
        if (byte === 0) {
          if (p + 1 >= buf.length || buf[p + 1] !== ESCAPE) {
            break;
          } else {
            p++; // skip ESCAPE
          }
        }
        accumulator.appendByte(byte);
      }
      pos.p = p + 1; // eats our trailing null byte
      return accumulator.extract();
    }

    case STRING: {
      const accumulator = new Accumulator();
      for (; p < buf.length; p++) {
        const byte = buf[p];
        if (byte === 0) {
          if (p + 1 >= buf.length || buf[p + 1] !== ESCAPE) {
            break;
          } else {
            p++; // skip ESCAPE
          }
        }
        accumulator.appendByte(byte);
      }
      pos.p = p + 1; // eats our trailing null byte
      const decoder = new TextDecoder("utf-8");
      return decoder.decode(accumulator.extract().buffer);
    }

    case DOUBLE: {
      const numBuf = new Uint8Array(8);
      for (let i = 0; i < 8; i++) {
        numBuf[i] = buf[p + i];
      }
      pos.p += 8;
      return decodeDouble(numBuf);
    }

    // could be BigInt
    default: {
      if (code >= NEGINTSTART || code <= POSINTEND) {
        return decodeBigInt(buf, pos, code);

        // if we got here, it must be an unknown type
      } else {
        throw new TypeError(
          `Invalid KvKey data: code ${code} ('${buf}' at ${pos})`,
        );
      }
    }
  }
}

/**
 * Unpack a buffer containing a kvKey back to
 * its original array elements.
 * This is the inverse of `pack()`, so unpack(pack(x)) == x.
 * @param buf The buffer containing the data to be decoded.
 */
export function unpack(buf: Uint8Array) {
  const pos = { p: 0 };
  const key: KeyPart[] = [];
  while (pos.p < buf.byteLength) {
    key.push(decodeKey(buf, pos));
  }
  return key;
}
