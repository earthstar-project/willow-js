/** Generic top-level error class that other Earthstar errors inherit from. */
export class WillowError extends Error {
  constructor(message?: string) {
    super(message || "");
    this.name = "WillowError";
  }
}

/** Validation failed on a document, share address, author address, etc. */
export class ValidationError extends WillowError {
  constructor(message?: string) {
    super(message || "Validation error");
    this.name = "ValidationError";
  }
}

/** Check if any value is a subclass of EarthstarError (return true) or not (return false) */
export function isErr<T>(x: T | Error): x is WillowError {
  return x instanceof WillowError;
}

/** Check if any value is a subclass of EarthstarError (return false) or not (return true) */
export function notErr<T>(x: T | Error): x is T {
  return !(x instanceof WillowError);
}
