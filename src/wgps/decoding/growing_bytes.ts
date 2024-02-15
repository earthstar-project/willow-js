import { concat, Deferred, deferred } from "../../../deps.ts";

/** An array of growing bytes which can be awaited upon and pruned. */
export class GrowingBytes {
  array = new Uint8Array();

  private deferredUntilLength: [number, Deferred<Uint8Array>] | null = null;

  constructor(incoming: AsyncIterable<Uint8Array>) {
    (async () => {
      for await (const chunk of incoming) {
        this.array = concat(this.array, chunk);

        if (
          this.deferredUntilLength &&
          this.array.byteLength >= this.deferredUntilLength[0]
        ) {
          this.deferredUntilLength[1].resolve(this.array);
          this.deferredUntilLength = null;
        }
      }
    })();
  }

  /** Wait for the underyling Uint8Array to grow relative to the given length, regardless of the current length. */
  nextRelative(length: number): Promise<Uint8Array> {
    return this.next(length, true);
  }

  /** Wait for the underyling Uint8Array to grow to at least the absolute given length. */
  nextAbsolute(length: number): Promise<Uint8Array> {
    if (this.array.byteLength >= length) {
      return Promise.resolve(this.array);
    }

    return this.next(length, false);
  }

  private next(ofLength: number, relative: boolean): Promise<Uint8Array> {
    const target = relative ? this.array.byteLength + ofLength : ofLength;

    if (
      this.deferredUntilLength &&
      this.deferredUntilLength[0] === target
    ) {
      return this.deferredUntilLength[1];
    }

    const deferredPromise = deferred<Uint8Array>();

    this.deferredUntilLength = [
      target,
      deferredPromise,
    ];

    return deferredPromise;
  }

  /** Prunes the array by the given bytelength. */
  prune(length: number) {
    this.array = this.array.slice(length);
  }
}
