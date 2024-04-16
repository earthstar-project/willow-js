type Resolver = (mutexId: string) => void;

export type MutexId = string;

/**
 * A mutex lock for coordination across async functions
 * This is based off of https://github.com/ide/await-lock/blob/master/src/AwaitLock.ts
 */
export default class Mutex {
  private acquired: boolean = false;
  private waitingResolvers: Resolver[] = [];
  private currentMutexId: MutexId | null = null;

  /**
   * Acquires the mutex, waiting if necessary for it to become free if it is already locked. The
   * returned promise is fulfilled once the lock is acquired.
   *
   * After acquiring the lock, you **must** call `release` when you are done with it.
   * @returns {MutexId} the v4 UUID of this lock
   */
  acquire(): Promise<MutexId> {
    if (!this.acquired) {
      this.acquired = true;
      this.currentMutexId = crypto.randomUUID();
      return Promise.resolve(this.currentMutexId);
    }

    return new Promise((resolve) => {
      this.waitingResolvers.push(resolve);
    });
  }

  /**
   * Releases the lock and gives it to the next waiting acquirer, if there is one. Each acquirer
   * may release the lock exactly once. If the UUID passed in doesn't match the current lock holder
   * this will throw an error.
   * @param {MutexId} mutexId the UUID returned from the aquire function call
   */
  release(mutexId: MutexId): void {
    if (!this.acquired) {
      throw new Error(`Cannot release an unacquired lock`);
    }
    if (mutexId !== this.currentMutexId) {
      throw new Error(`Release ID doesn't match current lock ID`);
    }

    if (this.waitingResolvers.length > 0) {
      const resolve = this.waitingResolvers.shift()!;
      this.currentMutexId = crypto.randomUUID();
      resolve(this.currentMutexId);
    } else {
      this.acquired = false;
      this.currentMutexId = null;
    }
  }
}
