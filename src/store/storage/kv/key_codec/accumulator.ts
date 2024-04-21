/** A resizable buffer */
let acc = new Uint8Array(1 << 14); // 16384

/** CodecBuffer class */
export class Accumulator {
  /** The next available byte (tail-pointer) */
  nextByte = 0;

  /** extract the encoded bytes
   * @returns - a trimmed encoded buffer
   */
  extract() {
    return acc.slice(0, this.nextByte);
  }

  /** check fit - expand accumulator as required */
  requires(bytesRequired: number) {
    if (acc.length < this.nextByte + bytesRequired) {
      let newAmt = acc.length;
      while (newAmt < this.nextByte + bytesRequired) newAmt *= 2;
      const newStorage = new Uint8Array(newAmt);
      newStorage.set(acc, 0);
      acc = newStorage;
      console.log("Increased accumulator capacity to - ", acc.byteLength);
    }
  }

  /** add a byte to the accumulator */
  appendByte(val: number) {
    this.requires(1);
    acc[this.nextByte++] = val;
  }

  /** add a buffer to the accumulator */
  appendBuffer(buf: Uint8Array) {
    const len = buf.byteLength;
    this.requires(len);
    acc.set(buf, this.nextByte);
    this.nextByte += len;
  }
}
