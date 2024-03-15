import { decodeCompactWidth, GrowingBytes } from "../../../deps.ts";
import { WillowError } from "../../errors.ts";
import {
  HandleType,
  LogicalChannel,
  MSG_CONTROL_ABSOLVE,
  MSG_CONTROL_ANNOUNCE_DROPPING,
  MSG_CONTROL_APOLOGISE,
  MSG_CONTROL_FREE,
  MSG_CONTROL_ISSUE_GUARANTEE,
  MSG_CONTROL_PLEAD,
  MsgControlAbsolve,
  MsgControlAnnounceDropping,
  MsgControlApologise,
  MsgControlFree,
  MsgControlIssueGuarantee,
  MsgControlPlead,
} from "../types.ts";
import { compactWidthFromEndOfByte } from "./util.ts";

export function decodeChannelFromBeginningOfByte(byte: number): LogicalChannel {
  if ((byte & 0x60) === 0x60) {
    return LogicalChannel.CapabilityChannel;
  } else if ((byte & 0x40) === 0x40) {
    return LogicalChannel.IntersectionChannel;
  }

  throw new WillowError("Couldn't decode logical channel");
}

export function decodeChannelFromEndOfByte(byte: number): LogicalChannel {
  if ((byte & 0x3) === 0x3) {
    return LogicalChannel.CapabilityChannel;
  } else if ((byte & 0x2) === 0x2) {
    return LogicalChannel.IntersectionChannel;
  }

  throw new WillowError("Couldn't decode logical channel");
}

export function decodeHandleTypeFromBeginningOfByte(byte: number): HandleType {
  if ((byte & 0x20) === 0x20) {
    return HandleType.CapabilityHandle;
  } else if ((byte & 0x0) === 0x0) {
    return HandleType.IntersectionHandle;
  }

  throw new WillowError("Couldn't decode handle type");
}

export async function decodeControlIssueGuarantee(
  bytes: GrowingBytes,
): Promise<MsgControlIssueGuarantee> {
  await bytes.nextAbsolute(1);

  // We know we have a byte.
  const compactWidth = compactWidthFromEndOfByte(bytes.array[0]);

  // Wait for another byte and decode the channel from it
  await bytes.nextAbsolute(2);

  const channel = decodeChannelFromBeginningOfByte(bytes.array[1]);

  // Wait for the number of bytes compact width told us to expect.

  await bytes.nextAbsolute(2 + compactWidth);

  const amount = decodeCompactWidth(bytes.array.subarray(2, 2 + compactWidth));

  bytes.prune(2 + compactWidth);

  return {
    kind: MSG_CONTROL_ISSUE_GUARANTEE,
    channel,
    amount: BigInt(amount),
  };
}

export async function decodeControlAbsolve(
  bytes: GrowingBytes,
): Promise<MsgControlAbsolve> {
  await bytes.nextAbsolute(1);

  const compactWidth = compactWidthFromEndOfByte(bytes.array[0]);

  await bytes.nextAbsolute(2);

  const channel = decodeChannelFromBeginningOfByte(bytes.array[1]);

  await bytes.nextAbsolute(2 + compactWidth);

  const amount = decodeCompactWidth(bytes.array.subarray(2, 2 + compactWidth));

  bytes.prune(2 + compactWidth);

  return {
    kind: MSG_CONTROL_ABSOLVE,
    channel,
    amount: BigInt(amount),
  };
}

export async function decodeControlPlead(
  bytes: GrowingBytes,
): Promise<MsgControlPlead> {
  await bytes.nextAbsolute(1);

  const compactWidth = compactWidthFromEndOfByte(bytes.array[0]);

  await bytes.nextAbsolute(2);

  const channel = decodeChannelFromBeginningOfByte(bytes.array[1]);

  await bytes.nextAbsolute(2 + compactWidth);

  const target = decodeCompactWidth(bytes.array.subarray(2, 2 + compactWidth));

  bytes.prune(2 + compactWidth);

  return {
    kind: MSG_CONTROL_PLEAD,
    channel,
    target: BigInt(target),
  };
}

export async function decodeControlAnnounceDropping(
  bytes: GrowingBytes,
): Promise<MsgControlAnnounceDropping> {
  await bytes.nextAbsolute(1);

  const channel = decodeChannelFromEndOfByte(bytes.array[0]);

  bytes.prune(1);

  return {
    kind: MSG_CONTROL_ANNOUNCE_DROPPING,
    channel,
  };
}

export async function decodeControlApologise(
  bytes: GrowingBytes,
): Promise<MsgControlApologise> {
  await bytes.nextAbsolute(1);

  const channel = decodeChannelFromEndOfByte(bytes.array[0]);

  bytes.prune(1);

  return {
    kind: MSG_CONTROL_APOLOGISE,
    channel,
  };
}

export async function decodeControlFree(
  bytes: GrowingBytes,
): Promise<MsgControlFree> {
  await bytes.nextAbsolute(1);

  const compactWidth = compactWidthFromEndOfByte(bytes.array[0]);

  await bytes.nextAbsolute(2);

  const handleType = decodeHandleTypeFromBeginningOfByte(bytes.array[1]);

  const mine = (bytes.array[1] & 0x10) === 0x10;

  await bytes.nextAbsolute(2 + compactWidth);

  const handle = decodeCompactWidth(bytes.array.subarray(2, 2 + compactWidth));

  bytes.prune(2 + compactWidth);

  return {
    kind: MSG_CONTROL_FREE,
    handleType,
    handle: BigInt(handle),
    mine,
  };
}
