import { decodeCompactWidth, GrowingBytes } from "../../../deps.ts";
import {
  MSG_SETUP_BIND_READ_CAPABILITY,
  MsgSetupBindReadCapability,
} from "../types.ts";
import { compactWidthFromEndOfByte } from "./util.ts";

export async function decodeSetupBindReadCapability<
  ReadCapabilityPartial,
  SyncSignature,
>(
  bytes: GrowingBytes,
  decodeReadCapPartial: (bytes: GrowingBytes) => Promise<ReadCapabilityPartial>,
  decodeSignature: (bytes: GrowingBytes) => Promise<SyncSignature>,
): Promise<MsgSetupBindReadCapability<ReadCapabilityPartial, SyncSignature>> {
  await bytes.nextAbsolute(1);

  const compactWidth = compactWidthFromEndOfByte(bytes.array[0]);

  const handle = decodeCompactWidth(bytes.array.subarray(1, 1 + compactWidth));

  bytes.prune(1 + compactWidth);

  const capability = await decodeReadCapPartial(bytes);
  const signature = await decodeSignature(bytes);

  return {
    kind: MSG_SETUP_BIND_READ_CAPABILITY,
    handle: BigInt(handle),
    capability,
    signature,
  };
}
