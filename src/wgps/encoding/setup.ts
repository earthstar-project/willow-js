import { compactWidth, concat, encodeCompactWidth } from "../../../deps.ts";
import { MsgSetupBindReadCapability } from "../types.ts";
import { compactWidthOr } from "./util.ts";

export function encodeSetupBindReadCapability<
  ReadCapabilityPartial,
  SyncSignature,
>(
  msg: MsgSetupBindReadCapability<ReadCapabilityPartial, SyncSignature>,
  encodeReadCapability: (cap: ReadCapabilityPartial) => Uint8Array,
  encodeSignature: (sig: SyncSignature) => Uint8Array,
) {
  const handleWidth = compactWidth(msg.handle);

  const header = compactWidthOr(0x20, handleWidth);

  return concat(
    new Uint8Array([header]),
    encodeCompactWidth(msg.handle),
    encodeReadCapability(msg.capability),
    encodeSignature(msg.signature),
  );
}
