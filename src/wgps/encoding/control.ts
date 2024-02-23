import { compactWidth, concat, encodeCompactWidth } from "../../../deps.ts";
import {
  HandleType,
  LogicalChannel,
  MsgControlAbsolve,
  MsgControlAnnounceDropping,
  MsgControlApologise,
  MsgControlFree,
  MsgControlIssueGuarantee,
  MsgControlPlead,
} from "../types.ts";

export function channelMaskStart(mask: number, channel: LogicalChannel) {
  switch (channel) {
    case LogicalChannel.IntersectionChannel:
      return mask & 0x64;
  }
}

export function channelMaskEnd(mask: number, channel: LogicalChannel) {
  switch (channel) {
    case LogicalChannel.IntersectionChannel:
      return mask & 0x2;
  }
}

export function handleMask(mask: number, handleType: HandleType) {
  switch (handleType) {
    case HandleType.IntersectionHandle:
      return mask;
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
    new Uint8Array([header, channelMaskStart(0, msg.channel)]),
    encodeCompactWidth(msg.amount),
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
    new Uint8Array([header, channelMaskStart(0, msg.channel)]),
    encodeCompactWidth(msg.amount),
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
    ? 0x90
    : 0x91;

  return concat(
    new Uint8Array([header, channelMaskStart(0, msg.channel)]),
    encodeCompactWidth(msg.target),
  );
}

export function encodeControlAnnounceDropping(
  msg: MsgControlAnnounceDropping,
) {
  return new Uint8Array([channelMaskEnd(90, msg.channel)]);
}

export function encodeControlApologise(
  msg: MsgControlApologise,
) {
  return new Uint8Array([channelMaskEnd(98, msg.channel)]);
}

export function encodeControlFree(
  msg: MsgControlFree,
) {
  const handleWidth = compactWidth(msg.handle);

  const header = handleWidth === 1
    ? 0x140
    : handleWidth === 2
    ? 0x141
    : handleWidth === 4
    ? 0x142
    : 0x143;

  const handleTypeByte = handleMask(0, msg.handleType) & (msg.mine ? 16 : 0);

  return concat(
    new Uint8Array([header, handleTypeByte]),
    encodeCompactWidth(msg.handle),
  );
}
