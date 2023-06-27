/** Generic top-level error class that other Willow errors inherit from. */
export class WillowError extends Error {
  constructor(message?: string) {
    super(message || "");
    this.name = "WillowError";
  }
}

/** Validation failed on an entry, keypair, etc. */
export class ValidationError extends WillowError {
  constructor(message?: string) {
    super(message || "Validation error");
    this.name = "ValidationError";
  }
}

/** Check if any value is a subclass of WillowError (return true) or not (return false) */
export function isErr<T>(x: T | Error): x is WillowError {
  return x instanceof WillowError;
}

/** Check if any value is a subclass of WillowError (return false) or not (return true) */
export function notErr<T>(x: T | Error): x is T {
  return !(x instanceof WillowError);
}
