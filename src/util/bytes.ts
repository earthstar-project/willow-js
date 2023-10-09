export function compareBytes(a: Uint8Array, b: Uint8Array): number {
  const shorter = a.byteLength < b.byteLength ? a : b;

  for (let i = 0; i < shorter.byteLength; i++) {
    const aByte = a[i];
    const bByte = b[i];

    if (aByte === bByte) {
      continue;
    }

    if (aByte < bByte) {
      return -1;
    }

    if (aByte > bByte) {
      return 1;
    }
  }

  if (a.byteLength < b.byteLength) {
    return -1;
  } else if (a.byteLength > b.byteLength) {
    return 1;
  }

  return 0;
}

export function bigintToBytes(bigint: bigint): Uint8Array {
  const bytes = new Uint8Array(8);
  const view = new DataView(bytes.buffer);

  view.setBigUint64(0, bigint);

  return bytes;
}

export function incrementLastByte(bytes: Uint8Array) {
  const last = bytes[bytes.byteLength - 1];

  if (last === 255) {
    const newBytes = new Uint8Array(bytes.byteLength + 1);

    newBytes.set(bytes, 0);
    newBytes.set([0], bytes.byteLength);

    return newBytes;
  } else {
    const newBytes = new Uint8Array(bytes);

    newBytes.set([last + 1], bytes.byteLength - 1);

    return newBytes;
  }
}
