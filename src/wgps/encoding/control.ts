import { concat } from "@std/bytes";
import { compactWidth, encodeCompactWidth } from "@earthstar/willow-utils";
import {
  HandleType,
  LogicalChannel,
  MsgControlLimitReceiving,
  MsgControlLimitSending,
  type MsgControlAbsolve,
  type MsgControlAnnounceDropping,
  type MsgControlApologise,
  type MsgControlFree,
  type MsgControlIssueGuarantee,
  type MsgControlPlead,
} from "../types.ts";

export function channelMaskStart(
  mask: number,
  channel: LogicalChannel,
): number {
  switch (channel) {
    case LogicalChannel.ReconciliationChannel:
      return mask;
    case LogicalChannel.DataChannel:
      return mask | 0x20;
    case LogicalChannel.IntersectionChannel:
      return mask | 0x40;
    case LogicalChannel.CapabilityChannel:
      return mask | 0x60;
    case LogicalChannel.AreaOfInterestChannel:
      return mask | 0x80;
    case LogicalChannel.PayloadRequestChannel:
      return mask | 0xa0;
    case LogicalChannel.StaticTokenChannel:
      return mask | 0xc0;
  }
}

export function channelMaskEnd(mask: number, channel: LogicalChannel): number {
  switch (channel) {
    case LogicalChannel.ReconciliationChannel:
      return mask;
    case LogicalChannel.DataChannel:
      return mask | 0x1;
    case LogicalChannel.IntersectionChannel:
      return mask | 0x2;
    case LogicalChannel.CapabilityChannel:
      return mask | 0x3;
    case LogicalChannel.AreaOfInterestChannel:
      return mask | 0x4;
    case LogicalChannel.PayloadRequestChannel:
      return mask | 0x5;
    case LogicalChannel.StaticTokenChannel:
      return mask | 0x6;
  }
}

export function handleMask(mask: number, handleType: HandleType): number {
  switch (handleType) {
    case HandleType.IntersectionHandle:
      return mask;
    case HandleType.CapabilityHandle:
      return mask | 0x20;
    case HandleType.AreaOfInterestHandle:
      return mask | 0x40;
    case HandleType.PayloadRequestHandle:
      return mask | 0x60;
    case HandleType.StaticTokenHandle:
      return mask | 0x80;
  }
}

export function encodeControlIssueGuarantee(
  msg: MsgControlIssueGuarantee,
) {
  const amountWidth = compactWidth(msg.amount);

  const header = 0x80;

  const amountMask = amountWidth === 1
    ? 0x80
    : amountWidth === 2
    ? 0x81
    : amountWidth === 4
    ? 0x82
    : 0x83;

  const channelMask = channelMaskEnd(0, msg.channel) << 3;

  return concat(
    [
      new Uint8Array([header, amountMask | channelMask]),
      encodeCompactWidth(msg.amount),
    ],
  );
}

export function encodeControlAbsolve(
  msg: MsgControlAbsolve,
) {
  const amountWidth = compactWidth(msg.amount);

  const header = 0x82;

  const amountMask = amountWidth === 1
    ? 0x80
    : amountWidth === 2
    ? 0x81
    : amountWidth === 4
    ? 0x82
    : 0x83;

  const channelMask = channelMaskEnd(0, msg.channel) << 3;

  return concat(
    [
      new Uint8Array([header, amountMask | channelMask]),
      encodeCompactWidth(msg.amount),
    ],
  );
}

export function encodeControlPlead(
  msg: MsgControlPlead,
) {
  const targetWidth = compactWidth(msg.target);

  const header = 0x84;

  const targetMask = targetWidth === 1
    ? 0x80
    : targetWidth === 2
    ? 0x81
    : targetWidth === 4
    ? 0x82
    : 0x83;

  const channelMask = channelMaskEnd(0, msg.channel) << 3;

  return concat(
    [
      new Uint8Array([header, targetMask | channelMask]),
      encodeCompactWidth(msg.target),
    ],
  );
}

export function encodeControlLimitSending(
  msg: MsgControlLimitSending,
) {
  const boundWidth = compactWidth(msg.bound);

  const header = 0x86;

  const boundMask = boundWidth === 1
    ? 0x80
    : boundWidth === 2
    ? 0x81
    : boundWidth === 4
    ? 0x82
    : 0x83;

  const channelMask = channelMaskEnd(0, msg.channel) << 3;

  return concat(
    [
      new Uint8Array([header, boundMask | channelMask]),
      encodeCompactWidth(msg.bound),
    ],
  );
}

export function encodeControlLimitReceiving(
  msg: MsgControlLimitReceiving,
) {
  const boundWidth = compactWidth(msg.bound);

  const header = 0x87;

  const boundMask = boundWidth === 1
    ? 0x80
    : boundWidth === 2
    ? 0x81
    : boundWidth === 4
    ? 0x82
    : 0x83;

  const channelMask = channelMaskEnd(0, msg.channel) << 3;

  return concat(
    [
      new Uint8Array([header, boundMask | channelMask]),
      encodeCompactWidth(msg.bound),
    ],
  );
}

export function encodeControlAnnounceDropping(
  msg: MsgControlAnnounceDropping,
) {
  return new Uint8Array([channelMaskEnd(0x90, msg.channel)]);
}

export function encodeControlApologise(
  msg: MsgControlApologise,
) {
  return new Uint8Array([channelMaskEnd(0x98, msg.channel)]);
}

export function encodeControlFree(
  msg: MsgControlFree,
) {
  const handleWidth = compactWidth(msg.handle);

  const header = handleWidth === 1
    ? 0x8c
    : handleWidth === 2
    ? 0x8d
    : handleWidth === 4
    ? 0x8e
    : 0x8f;

  const handleTypeByte = handleMask(0, msg.handleType) | (msg.mine ? 0x10 : 0);

  return concat(
    [new Uint8Array([header, handleTypeByte]), encodeCompactWidth(msg.handle)],
  );
}
