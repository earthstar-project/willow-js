import { MSG_COMMITMENT_REVEAL, MsgCommitmentReveal } from "../types.ts";
import { GrowingBytes } from "./growing_bytes.ts";

export async function decodeCommitmentReveal(
  bytes: GrowingBytes,
  challengeLength: number,
): Promise<MsgCommitmentReveal> {
  const commitmentBytes = await bytes.nextAbsolute(1 + challengeLength);

  bytes.prune(1 + challengeLength);

  return {
    kind: MSG_COMMITMENT_REVEAL,
    nonce: commitmentBytes.subarray(1, 1 + challengeLength),
  };
}
