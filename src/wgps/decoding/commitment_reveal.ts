import { GrowingBytes } from "../../../deps.ts";
import { MsgCommitmentReveal, MsgKind } from "../types.ts";

export async function decodeCommitmentReveal(
  bytes: GrowingBytes,
  challengeLength: number,
): Promise<MsgCommitmentReveal> {
  const commitmentBytes = await bytes.nextAbsolute(1 + challengeLength);

  bytes.prune(1 + challengeLength);

  return {
    kind: MsgKind.CommitmentReveal,
    nonce: commitmentBytes.subarray(1, 1 + challengeLength),
  };
}
