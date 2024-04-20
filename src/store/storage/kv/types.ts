export type KeyPart =
  | Uint8Array
  | string
  | number
  | boolean;

export type KvKey = KeyPart[];

/**
 * The ordering on KeyParts. Uint8Arrays are less than strings are less than numbers are less than booleans. Amongst themselves, Uint8Arrays are compared lexicographically, strings use the js `<` and `>` operators, numbers use the js `<` and `>` operators, and booleans use the js `<` and `>` operators.
 * 
 * @returns A negative number if a is less than b, zero if a equals b, a positive number if a is greater than b.
 */
export function compareTwoKeyParts(a: KeyPart, b: KeyPart): number {
  if (a instanceof Uint8Array) {
    if (b instanceof Uint8Array) {
      return compareUint8Arrays(a, b);
    } else {
      return -1;
    }
  } else if (typeof a === "string") {
    if (b instanceof Uint8Array) {
      return 1;
    } else if (typeof b === "string") {
      if (a < b) {
        return -1;
      } else if (a > b) {
        return 1;
      } else {
        return 0;
      }
    } else {
      return -1;
    }
  } else if (typeof a === "number") {
    if (typeof b === "boolean") {
      return -1;
    } else if (typeof b === "number") {
      if (a < b) {
        return -1;
      } else if (a > b) {
        return 1;
      } else {
        return 0;
      }
    } else {
      return 1;
    }
  } else {
    // a is a boolean
    if (typeof b === "boolean") {
      if (a < b) {
        return -1;
      } else if (a > b) {
        return 1;
      } else {
        return 0;
      }
    } else {
      return 1;
    }
  }
}

/**
 * The ordering on keys. Lexicographically compares the KeyParts according to {@linkcode compareTwoKeyParts}.
 * 
 * @returns A negative number if a is less than b, zero if a equals b, a positive number if a is greater than b.
 */
export function compareKeys(a: KvKey, b: KvKey): number {
  for (let i = 0; i < Math.min(a.length, b.length); i++) {
    if (compareTwoKeyParts(a[i], b[i]) < 0) {
      return -1;
    } else if (compareTwoKeyParts(a[i], b[i]) > 0) {
      return 1;
    }
  }

  if (a.length < b.length) {
    return -1;
  } else if (a.length > b.length) {
    return 1;
  } else {
    return 0;
  }
}

/**
 * Returns whether the first argment is a prefix of the second argument.
 */
export function isFirstKeyPrefixOfSecondKey(a: KvKey, b: KvKey): boolean {
  if (a.length > b.length) {
    return false;
  } else {
    for (let i = 0; i < a.length; i++) {
      if (compareTwoKeyParts(a[i], b[i]) !== 0) {
        return false;
      }
    }

    return true;
  }
}

export type KvBatch = {
  set: <Value>(key: KvKey, value: Value) => void;
  delete: (key: KvKey) => void;
  commit: () => Promise<void>;
};

export interface KvDriver {
  get<Value>(key: KvKey): Promise<Value | undefined>;
  set<Value>(key: KvKey, value: Value): Promise<void>;
  /**
   * Return `true` if something was deleted.
   */
  delete(key: KvKey): Promise<boolean>;
  /**
   * Matches an entry whose key is exactly `prefix` (unlike Deno.Kv).
   * `limit` must not be zero (like Deno.Kv).
   */
  list<Value>(
    selector: { start?: KvKey; end?: KvKey; prefix?: KvKey },
    opts?: {
      reverse?: boolean;
      limit?: number;
      batchSize?: number;
    },
  ): AsyncIterable<{ key: KvKey; value: Value }>;
  /**
   * If prefix is specified, then start and end must be prefixed by it, otherwise unspecified behavior.
   */
  clear(opts?: { prefix?: KvKey; start?: KvKey; end?: KvKey }): Promise<void>;
  batch(): KvBatch;
}

function compareUint8Arrays(a: Uint8Array, b: Uint8Array): number {
  for (let i = 0; i < Math.min(a.length, b.length); i++) {
    if (a[i] < b[i]) {
      return -1;
    } else if (a[i] > b[i]) {
      return 1;
    }
  }

  if (a.length < b.length) {
    return -1;
  } else if (a.length > b.length) {
    return 1;
  } else {
    return 0;
  }
}
