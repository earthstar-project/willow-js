import type { Accumulator } from "./accumulator.ts";
import { DOUBLE } from "./types.ts";
const ENCODING = true;
const DECODING = false;

/** encode double */
export function encodeDouble(accumulator: Accumulator, num: number) {
  accumulator.appendByte(DOUBLE);
  const buf = new Uint8Array(8);
  writeDoubleBE(buf, num, 0);
  accumulator.appendBuffer(adjustFloat(buf, ENCODING));
}
/**
 * decode double
 */
export function decodeDouble(buf: Uint8Array) {
  adjustFloat(buf, DECODING);
  return readDoubleBE(buf, 0);
}

function writeDoubleBE(
  buf: Uint8Array,
  value: number,
  offset: number,
) {
  value = +value;
  offset = offset >>> 0;
  ieeeWrite(buf, value, offset);
}

const nBytes = 8;

/**
 * ieee754. BSD-3-Clause License. Feross Aboukhadijeh <https://feross.org/opensource>
 */
function ieeeWrite(
  buffer: Uint8Array,
  value: number,
  offset: number,
) {
  let mLen = 52;
  let e;
  let m;
  let c;
  let eLen = (nBytes * 8) - mLen - 1;
  const eMax = (1 << eLen) - 1;
  const eBias = eMax >> 1;
  const rt = 0;
  let i = nBytes - 1;
  const d = -1;
  const s = value < 0 || (value === 0 && 1 / value < 0) ? 1 : 0;

  value = Math.abs(value);

  if (isNaN(value) || value === Infinity) {
    m = isNaN(value) ? 1 : 0;
    e = eMax;
  } else {
    e = Math.floor(Math.log(value) / Math.LN2);
    if (value * (c = Math.pow(2, -e)) < 1) {
      e--;
      c *= 2;
    }
    if (e + eBias >= 1) {
      value += rt / c;
    } else {
      value += rt * Math.pow(2, 1 - eBias);
    }
    if (value * c >= 2) {
      e++;
      c /= 2;
    }

    if (e + eBias >= eMax) {
      m = 0;
      e = eMax;
    } else if (e + eBias >= 1) {
      m = ((value * c) - 1) * Math.pow(2, mLen);
      e = e + eBias;
    } else {
      m = value * Math.pow(2, eBias - 1) * Math.pow(2, mLen);
      e = 0;
    }
  }

  for (
    ;
    mLen >= 8;
    buffer[offset + i] = m & 0xff, i += d, m /= 256, mLen -= 8
  ) { /* empty */ }

  e = (e << mLen) | m;
  eLen += mLen;
  for (
    ;
    eLen > 0;
    buffer[offset + i] = e & 0xff, i += d, e /= 256, eLen -= 8
  ) { /* empty */ }

  buffer[offset + i - d] |= s * 128;
}

/**
 * ieee754. BSD-3-Clause License. Feross Aboukhadijeh <https://feross.org/opensource>
 */
function ieeeRead(
  buffer: Uint8Array,
  offset: number,
  isLE: boolean,
) {
  const mLen = 52;
  let e;
  let m;
  const eLen = (nBytes * 8) - mLen - 1;
  const eMax = (1 << eLen) - 1;
  const eBias = eMax >> 1;
  let nBits = -7;
  let i = isLE ? (nBytes - 1) : 0;
  const d = isLE ? -1 : 1;
  let s = buffer[offset + i];

  i += d;

  e = s & ((1 << (-nBits)) - 1);
  s >>= -nBits;
  nBits += eLen;
  for (
    ;
    nBits > 0;
    e = (e * 256) + buffer[offset + i], i += d, nBits -= 8
  ) { /* empty */ }

  m = e & ((1 << (-nBits)) - 1);
  e >>= -nBits;
  nBits += mLen;
  for (
    ;
    nBits > 0;
    m = (m * 256) + buffer[offset + i], i += d, nBits -= 8
  ) { /* empty */ }

  if (e === 0) {
    e = 1 - eBias;
  } else if (e === eMax) {
    return m ? NaN : ((s ? -1 : 1) * Infinity);
  } else {
    m = m + Math.pow(2, mLen);
    e = e - eBias;
  }
  return (s ? -1 : 1) * m * Math.pow(2, e - mLen);
}

function readDoubleBE(buf: Uint8Array, offset: number) {
  offset = offset >>> 0;
  return ieeeRead(buf, offset, false);
}

/**
 * adjust float data
 */
export function adjustFloat(data: Uint8Array, isEncode: boolean) {
  if (
    (isEncode && (data[0] & 0x80) === 0x80) ||
    (!isEncode && (data[0] & 0x80) === 0x00)
  ) {
    for (let i = 0; i < data.length; i++) {
      data[i] = ~data[i];
    }
  } else {
    data[0] ^= 0x80;
  }
  return data;
}
