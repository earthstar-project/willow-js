import { decodeCompactWidth, type GrowingBytes } from "@earthstar/willow-utils";
import {
  HandleType,
  LogicalChannel,
  type MsgControlAbsolve,
  type MsgControlAnnounceDropping,
  type MsgControlApologise,
  type MsgControlFree,
  type MsgControlIssueGuarantee,
  type MsgControlPlead,
  type MsgControlLimitSending,
  type MsgControlLimitReceiving,
  MsgKind,
} from "../types.ts";
import { compactWidthFromEndOfByte } from "./util.ts";

export function decodeChannelFromBeginningOfByte(byte: number): LogicalChannel {
  if ((byte & 0xc0) === 0xc0) {
    return LogicalChannel.StaticTokenChannel;
  } else if ((byte & 0xa0) === 0xa0) {
    return LogicalChannel.PayloadRequestChannel;
  } else if ((byte & 0x80) === 0x80) {
    return LogicalChannel.AreaOfInterestChannel;
  } else if ((byte & 0x60) === 0x60) {
    return LogicalChannel.CapabilityChannel;
  } else if ((byte & 0x40) === 0x40) {
    return LogicalChannel.IntersectionChannel;
  } else if ((byte & 0x20) === 0x20) {
    return LogicalChannel.DataChannel;
  } else {
    return LogicalChannel.ReconciliationChannel;
  }
}

export function decodeChannelFromEndOfByte(byte: number): LogicalChannel {
  if ((byte & 0x6) === 0x6) {
    return LogicalChannel.StaticTokenChannel;
  } else if ((byte & 0x5) === 0x5) {
    return LogicalChannel.PayloadRequestChannel;
  } else if ((byte & 0x4) === 0x4) {
    return LogicalChannel.AreaOfInterestChannel;
  } else if ((byte & 0x3) === 0x3) {
    return LogicalChannel.CapabilityChannel;
  } else if ((byte & 0x2) === 0x2) {
    return LogicalChannel.IntersectionChannel;
  } else if ((byte & 0x1) === 0x1) {
    return LogicalChannel.DataChannel;
  } else {
    return LogicalChannel.ReconciliationChannel;
  }
}

export function decodeHandleTypeFromBeginningOfByte(byte: number): HandleType {
  if ((byte & 0x80) === 0x80) {
    return HandleType.StaticTokenHandle;
  } else if ((byte & 0x60) === 0x60) {
    return HandleType.PayloadRequestHandle;
  } else if ((byte & 0x40) === 0x40) {
    return HandleType.AreaOfInterestHandle;
  } else if ((byte & 0x20) === 0x20) {
    return HandleType.CapabilityHandle;
  } else {
    return HandleType.IntersectionHandle;
  }
}

export async function decodeControlIssueGuarantee(
  bytes: GrowingBytes,
): Promise<MsgControlIssueGuarantee> {
  await bytes.nextAbsolute(2);
  const compactWidth = compactWidthFromEndOfByte(bytes.array[1]);
  const channel = decodeChannelFromEndOfByte(bytes.array[1] >> 3);

  // Wait for the number of bytes compact width told us to expect.

  await bytes.nextAbsolute(2 + compactWidth);

  const amount = decodeCompactWidth(bytes.array.subarray(2, 2 + compactWidth));

  bytes.prune(2 + compactWidth);

  return {
    kind: MsgKind.ControlIssueGuarantee,
    channel,
    amount: BigInt(amount),
  };
}

export async function decodeControlAbsolve(
  bytes: GrowingBytes,
): Promise<MsgControlAbsolve> {
  await bytes.nextAbsolute(2);
  const compactWidth = compactWidthFromEndOfByte(bytes.array[1]);
  const channel = decodeChannelFromEndOfByte(bytes.array[1] >> 3);

  await bytes.nextAbsolute(2 + compactWidth);

  const amount = decodeCompactWidth(bytes.array.subarray(2, 2 + compactWidth));

  bytes.prune(2 + compactWidth);

  return {
    kind: MsgKind.ControlAbsolve,
    channel,
    amount: BigInt(amount),
  };
}

export async function decodeControlPlead(
  bytes: GrowingBytes,
): Promise<MsgControlPlead> {
  await bytes.nextAbsolute(2);
  const compactWidth = compactWidthFromEndOfByte(bytes.array[1]);
  const channel = decodeChannelFromEndOfByte(bytes.array[1] >> 3);

  await bytes.nextAbsolute(2 + compactWidth);

  const target = decodeCompactWidth(bytes.array.subarray(2, 2 + compactWidth));

  bytes.prune(2 + compactWidth);

  return {
    kind: MsgKind.ControlPlead,
    channel,
    target: BigInt(target),
  };
}

export async function decodeControlLimitSending(
  bytes: GrowingBytes,
): Promise<MsgControlLimitSending> {
  await bytes.nextAbsolute(2);
  const compactWidth = compactWidthFromEndOfByte(bytes.array[1]);
  const channel = decodeChannelFromEndOfByte(bytes.array[1] >> 3);

  await bytes.nextAbsolute(2 + compactWidth);

  const bound = decodeCompactWidth(bytes.array.subarray(2, 2 + compactWidth));

  bytes.prune(2 + compactWidth);

  return {
    kind: MsgKind.ControlLimitSending,
    channel,
    bound: BigInt(bound),
  };
}

export async function decodeControlLimitReceiving(
  bytes: GrowingBytes,
): Promise<MsgControlLimitReceiving> {
  await bytes.nextAbsolute(2);
  const compactWidth = compactWidthFromEndOfByte(bytes.array[1]);
  const channel = decodeChannelFromEndOfByte(bytes.array[1] >> 3);

  await bytes.nextAbsolute(2 + compactWidth);

  const bound = decodeCompactWidth(bytes.array.subarray(2, 2 + compactWidth));

  bytes.prune(2 + compactWidth);

  return {
    kind: MsgKind.ControlLimitReceiving,
    channel,
    bound: BigInt(bound),
  };
}

export async function decodeControlAnnounceDropping(
  bytes: GrowingBytes,
): Promise<MsgControlAnnounceDropping> {
  await bytes.nextAbsolute(1);

  const channel = decodeChannelFromEndOfByte(bytes.array[0]);

  bytes.prune(1);

  return {
    kind: MsgKind.ControlAnnounceDropping,
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
    kind: MsgKind.ControlApologise,
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
    kind: MsgKind.ControlFree,
    handleType,
    handle: BigInt(handle),
    mine,
  };
}
