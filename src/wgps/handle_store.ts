import { Deferred, deferred } from "../../deps.ts";
import { ValidationError } from "../errors.ts";

/** A mapping of handles to data */
export class HandleStore<ValueType> {
  private leastUnassignedHandle = BigInt(0);

  /** A map of handles (numeric IDs) to a triple made up of:
   * - The bound data
   * - Whether we've asked to free that data (and in doing so committing to no longer using it)
   * - The number of unprocessed messages which refer to this handle. */
  private map = new Map<bigint, [ValueType, boolean, number]>();

  private eventuallyMap = new Map<bigint, Deferred<ValueType>>();

  /** Indicates whether this a store of handles we have bound, or a store of handles bound by another peer. */
  // private isOurs: boolean;

  get(handle: bigint): ValueType | undefined {
    const result = this.map.get(handle);

    if (!result) {
      return;
    }

    const [info] = result;

    return info;
  }

  getEventually(handle: bigint): Promise<ValueType> {
    const result = this.map.get(handle);

    if (result) {
      const [info] = result;

      return Promise.resolve(info);
    }

    const existingPromise = this.eventuallyMap.get(handle);

    if (existingPromise) {
      return existingPromise;
    }

    const newPromise = deferred<ValueType>();

    this.eventuallyMap.set(handle, newPromise);

    return newPromise;
  }

  /** Bind some data to a handle. */
  bind(data: ValueType) {
    const handle = this.leastUnassignedHandle;

    const eventuallyPromise = this.eventuallyMap.get(handle);

    if (eventuallyPromise) {
      eventuallyPromise.resolve(data);
    }

    this.map.set(handle, [data, false, 0]);

    this.leastUnassignedHandle += BigInt(1);

    return handle;
  }

  /** Update the data at some binding */
  update(handle: bigint, data: ValueType) {
    const result = this.map.get(handle);

    if (!result) {
      return new ValidationError(
        "Asked to a update a handle we don't have any record of",
      );
    }

    const [, askedToFree, referenceCount] = result;

    if (askedToFree) {
      return new ValidationError(
        "Tried to a update a handle we have already freed",
      );
    }

    this.map.set(handle, [data, askedToFree, referenceCount]);
  }

  canUse(handle: bigint) {
    const result = this.map.get(handle);

    if (!result) {
      return false;
    }

    const [, askedToFree] = result;

    return !askedToFree;
  }

  markForFreeing(handle: bigint) {
    const result = this.map.get(handle);

    if (!result) {
      return new ValidationError(
        "Asked to a free a handle we don't have any record of",
      );
    }

    const [info, , referenceCount] = result;

    if (referenceCount === 0) {
      this.map.delete(handle);
    } else {
      this.map.set(handle, [info, true, referenceCount]);
    }
  }

  incrementHandleReference(handle: bigint) {
    const result = this.map.get(handle);

    if (!result) {
      return new ValidationError(
        "Asked to a increment the reference counter for a handle we don't have any record of",
      );
    }

    const [info, markedForFreeing, referenceCount] = result;

    this.map.set(handle, [info, markedForFreeing, referenceCount + 1]);
  }

  decrementHandleReference(handle: bigint) {
    const result = this.map.get(handle);

    if (!result) {
      return new ValidationError(
        "Asked to decrement the reference counter for a handle we don't have any record of",
      );
    }

    const [info, markedForFreeing, referenceCount] = result;

    const nextCount = Math.max(referenceCount - 1, 0);

    if (markedForFreeing && nextCount === 0) {
      this.map.delete(handle);
    } else {
      this.map.set(handle, [info, markedForFreeing, referenceCount - 1]);
    }
  }

  *[Symbol.iterator](): Iterator<[bigint, ValueType]> {
    for (const [handle, [info]] of this.map) {
      yield [handle, info];
    }
  }
}
