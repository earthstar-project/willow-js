import { concat } from "@std/bytes";
import { compactWidth, encodeCompactWidth } from "@earthstar/willow-utils";
import {
  HandleType,
  LogicalChannel,
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

  const header = amountWidth === 1
    ? 0x80
    : amountWidth === 2
    ? 0x81
    : amountWidth === 4
    ? 0x82
    : 0x83;

  return concat(
    [
      new Uint8Array([header, channelMaskStart(0, msg.channel)]),
      encodeCompactWidth(msg.amount),
    ],
  );
}

export function encodeControlAbsolve(
  msg: MsgControlAbsolve,
) {
  const amountWidth = compactWidth(msg.amount);

  const header = amountWidth === 1
    ? 0x84
    : amountWidth === 2
    ? 0x85
    : amountWidth === 4
    ? 0x86
    : 0x87;

  return concat(
    [
      new Uint8Array([header, channelMaskStart(0, msg.channel)]),
      encodeCompactWidth(msg.amount),
    ],
  );
}

export function encodeControlPlead(
  msg: MsgControlPlead,
) {
  const targetWidth = compactWidth(msg.target);

  const header = targetWidth === 1
    ? 0x88
    : targetWidth === 2
    ? 0x89
    : targetWidth === 4
    ? 0x8a
    : 0x8b;

  return concat(
    [
      new Uint8Array([header, channelMaskStart(0, msg.channel)]),
      encodeCompactWidth(msg.target),
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
