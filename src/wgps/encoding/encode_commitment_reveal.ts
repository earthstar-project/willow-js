import { concat } from "../../../deps.ts";
import { MsgCommitmentReveal } from "../types.ts";

export function encodeCommitmentReveal(msg: MsgCommitmentReveal): Uint8Array {
  return concat(
    new Uint8Array([0]),
    msg.nonce,
  );
}
