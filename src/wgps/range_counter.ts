import { WillowError } from "../errors.ts";
import { COVERS_NONE } from "./types.ts";

export class RangeCounter {
  private count = 0n;
  private inProgress = new Set<bigint>();

  getNext() {
    this.inProgress.add(this.count);
    return this.count++;
  }

  done(covers: bigint | typeof COVERS_NONE) {
    if (covers !== COVERS_NONE) {
      if (!this.inProgress.delete(covers)) {
        throw new WillowError("Answered range without request");
      }
    }
  }

  info() {
    return {
      remaining: BigInt(this.inProgress.size),
      all: this.count,
    };
  }
}
