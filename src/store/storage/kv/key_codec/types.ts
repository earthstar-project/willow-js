// deno-lint-ignore-file ban-types

// codec type codes
export const NULL = 0; // NUL - null-terminator = 0
export const BYTES = 1; // BYTES = 1
export const STRING = 2; // STX - Start of String = 2
export const NEGINTSTART = 0x0b; // Negative Integer Start = 11
export const INTZERO = 0x14; // Integer = 20
export const POSINTEND = 0x1d; // Positive Integer End = 29
export const DOUBLE = 0x21; // Double = 33
export const FALSE = 0x26; // Boolean False = 38
export const TRUE = 0x27; // Boolean True = 39
export const ESCAPE = 0xFF; // Escape = 255

/**
 * valid type codes
 */
export type TYPE_CODE =
  | 0 // NUL - null-terminator = 0
  | 1 // BYTES = 1
  | 2 // STX - Start of String = 2
  | 0x0b // Negative Integer Start = 11
  | 0x14 // Integer = 20
  | 0x1d // Positive Integer End = 29
  | 0x21 // Double = 33
  | 0x26 // Boolean False = 38
  | 0x27 // Boolean True = 39
  | 0xFF; // Escape = 255

/**
 * Supported tuple item types
 */
export type KeyPart =
  | Uint8Array
  | string
  | number
  | BigInt
  | boolean;
