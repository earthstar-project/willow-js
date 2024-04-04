import {
  compactWidth,
  concat,
  encodeCompactWidth,
  encodeEntryRelativeEntry,
  Entry,
  PathScheme,
  TotalOrder,
} from "../../../deps.ts";
import { MsgDataSendEntry, MsgDataSendPayload } from "../types.ts";
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
