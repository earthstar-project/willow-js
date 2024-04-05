import {
  compactWidth,
  concat,
  encodeCompactWidth,
  encodeEntryRelativeEntry,
  Entry,
  PathScheme,
  TotalOrder,
} from "../../../deps.ts";
import {
  MsgDataBindPayloadRequest,
  MsgDataSendEntry,
  MsgDataSendPayload,
  MsgDataSetEagerness,
} from "../types.ts";
import { compactWidthOr } from "./util.ts";

export function encodeDataSendEntry<
  DynamicToken,
  NamespaceId,
  SubspaceId,
  PayloadDigest,
>(
  msg: MsgDataSendEntry<
    DynamicToken,
    NamespaceId,
    SubspaceId,
    PayloadDigest
  >,
  opts: {
    encodeDynamicToken: (token: DynamicToken) => Uint8Array;
    currentlySentEntry: Entry<NamespaceId, SubspaceId, PayloadDigest>;
    isEqualNamespace: (a: NamespaceId, b: NamespaceId) => boolean;
    orderSubspace: TotalOrder<SubspaceId>;
    encodeNamespaceId: (namespace: NamespaceId) => Uint8Array;
    encodeSubspaceId: (subspace: SubspaceId) => Uint8Array;
    encodePayloadDigest: (digest: PayloadDigest) => Uint8Array;
    pathScheme: PathScheme;
  },
) {
  const messageTypeMask = 0x60;
  const compactWidthStaticTokenFlag = compactWidthOr(
    0,
    compactWidth(msg.staticTokenHandle),
  );

  const firstByte = messageTypeMask | compactWidthStaticTokenFlag;

  const encodeOffsetFlag =
    msg.offset !== 0n && msg.offset !== msg.entry.payloadLength ? 0x80 : 0x0;
  const compactWidthOffsetFlag = msg.offset === 0n
    ? 0x0
    : msg.offset === msg.entry.payloadLength
    ? 0x20
    : compactWidthOr(0, compactWidth(msg.offset)) <<
      5;
  // This is always flagged to true
  const encodedRelativeToCurrent = 0x10;

  // which means this is always 0x0
  const compactWidthSenderHandle = 0x0;
  // and this is always 0x0
  const compactWidthReceiverHandle = 0x0;

  const secondByte = encodeOffsetFlag | compactWidthOffsetFlag |
    encodedRelativeToCurrent | compactWidthSenderHandle |
    compactWidthReceiverHandle;

  const encodedStaticToken = encodeCompactWidth(msg.staticTokenHandle);

  const encodedDynamicToken = opts.encodeDynamicToken(msg.dynamicToken);

  const encodedOffset =
    msg.offset === 0n || msg.offset === msg.entry.payloadLength
      ? new Uint8Array()
      : encodeCompactWidth(msg.offset);

  // We skip encoding the sender and receiver handles because this implementation doesn't do that yet.

  const encodedEntry = encodeEntryRelativeEntry(
    {
      encodeNamespace: opts.encodeNamespaceId,
      encodePayloadDigest: opts.encodePayloadDigest,
      encodeSubspace: opts.encodeSubspaceId,
      isEqualNamespace: opts.isEqualNamespace,
      orderSubspace: opts.orderSubspace,
      pathScheme: opts.pathScheme,
    },
    msg.entry,
    opts.currentlySentEntry,
  );

  return concat(
    new Uint8Array([firstByte, secondByte]),
    encodedStaticToken,
    encodedDynamicToken,
    encodedOffset,
    encodedEntry,
  );
}

export function encodeDataSendPayload(msg: MsgDataSendPayload): Uint8Array {
  const messageKindFlag = 0x64;
  const header = compactWidthOr(messageKindFlag, compactWidth(msg.amount));

  const encodedAmount = encodeCompactWidth(msg.amount);

  return concat(
    new Uint8Array([header]),
    encodedAmount,
    msg.bytes,
  );
}

export function encodeDataSetEagerness(msg: MsgDataSetEagerness): Uint8Array {
  const messageKind = 0x68;
  const eagernessFlag = msg.isEager ? 0x2 : 0x0;

  const firstByte = messageKind | eagernessFlag;

  let secondByte = 0x0;

  secondByte = compactWidthOr(secondByte, compactWidth(msg.senderHandle)) << 2;
  secondByte = compactWidthOr(secondByte, compactWidth(msg.receiverHandle)) <<
    4;

  return concat(
    new Uint8Array([firstByte, secondByte]),
    encodeCompactWidth(msg.senderHandle),
    encodeCompactWidth(msg.receiverHandle),
  );
}

export function encodeDataBindPayloadRequest<
  NamespaceId,
  SubspaceId,
  PayloadDigest,
>(
  msg: MsgDataBindPayloadRequest<NamespaceId, SubspaceId, PayloadDigest>,
  opts: {
    currentlySentEntry: Entry<NamespaceId, SubspaceId, PayloadDigest>;
    isEqualNamespace: (a: NamespaceId, b: NamespaceId) => boolean;
    orderSubspace: TotalOrder<SubspaceId>;
    encodeNamespaceId: (namespace: NamespaceId) => Uint8Array;
    encodeSubspaceId: (subspace: SubspaceId) => Uint8Array;
    encodePayloadDigest: (digest: PayloadDigest) => Uint8Array;
    pathScheme: PathScheme;
  },
): Uint8Array {
  const messageKindBits = 0x6c;
  const firstByte = compactWidthOr(
    messageKindBits,
    compactWidth(msg.capability),
  );

  const encodeOffsetFlag = msg.offset !== 0n ? 0x80 : 0x0;
  const compactWidthOffsetBits = encodeOffsetFlag === 0x0
    ? 0x0
    : compactWidthOr(0x0, compactWidth(msg.offset)) << 5;
  const encodedRelativeFlag = 0x10;
  // Don't encode sender and receiver handle widths.

  const secondByte = encodeOffsetFlag | compactWidthOffsetBits |
    encodedRelativeFlag;

  const encodedCapability = encodeCompactWidth(msg.capability);

  const encodedOffset = encodeOffsetFlag === 0x0
    ? new Uint8Array()
    : encodeCompactWidth(msg.offset);

  // We skip encoding the sender and receiver handles because we don't do that here.

  const encodedEntry = encodeEntryRelativeEntry(
    {
      encodeNamespace: opts.encodeNamespaceId,
      encodePayloadDigest: opts.encodePayloadDigest,
      encodeSubspace: opts.encodeSubspaceId,
      isEqualNamespace: opts.isEqualNamespace,
      orderSubspace: opts.orderSubspace,
      pathScheme: opts.pathScheme,
    },
    msg.entry,
    opts.currentlySentEntry,
  );

  return concat(
    new Uint8Array([firstByte, secondByte]),
    encodedCapability,
    encodedOffset,
    encodedEntry,
  );
}
