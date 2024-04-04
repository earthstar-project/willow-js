import {
  Area,
  decodeCompactWidth,
  decodeStreamEntryInNamespaceArea,
  decodeStreamEntryRelativeEntry,
  Entry,
  GrowingBytes,
  PathScheme,
} from "../../../deps.ts";
import { MSG_DATA_SEND_ENTRY, MsgDataSendEntry } from "../types.ts";
import { compactWidthFromEndOfByte } from "./util.ts";

export async function decodeDataSendEntry<
  DynamicToken,
  NamespaceId,
  SubspaceId,
  PayloadDigest,
>(
  bytes: GrowingBytes,
  opts: {
    decodeNamespaceId: (bytes: GrowingBytes) => Promise<NamespaceId>;
    decodeSubspaceId: (bytes: GrowingBytes) => Promise<SubspaceId>;
    decodeDynamicToken: (bytes: GrowingBytes) => Promise<DynamicToken>;
    decodePayloadDigest: (bytes: GrowingBytes) => Promise<PayloadDigest>;
    pathScheme: PathScheme;
    currentlyReceivedEntry: Entry<NamespaceId, SubspaceId, PayloadDigest>;
    aoiHandlesToArea: (
      senderHandle: bigint,
      receiverHandle: bigint,
    ) => Area<SubspaceId>;
    aoiHandlesToNamespace: (
      senderHandle: bigint,
      receiverHandle: bigint,
    ) => NamespaceId;
  },
): Promise<
  MsgDataSendEntry<
    DynamicToken,
    NamespaceId,
    SubspaceId,
    PayloadDigest
  >
> {
  await bytes.nextAbsolute(2);

  const [firstByte, secondByte] = bytes.array;

  const staticTokenCompactWidth = compactWidthFromEndOfByte(firstByte);

  const isOffsetEncoded = (secondByte & 0x80) === 0x80;

  const isOffsetPayloadLengthOrZero = !isOffsetEncoded &&
    (secondByte & 0x20) === 0x20;

  const offsetCompactWidth = isOffsetEncoded
    ? compactWidthFromEndOfByte(secondByte & 0x60 >> 5)
    : 0;

  const isEntryEncodedRelative = (secondByte & 0x10) === 0x10;

  const senderHandleCompactWidth = isEntryEncodedRelative
    ? 0
    : compactWidthFromEndOfByte(secondByte & 0xc >> 2);
  const receiverHandleCompactWidth = isEntryEncodedRelative
    ? 0
    : compactWidthFromEndOfByte(secondByte);

  bytes.prune(2);

  await bytes.nextAbsolute(staticTokenCompactWidth);

  const staticTokenHandle = BigInt(decodeCompactWidth(
    bytes.array.subarray(0, staticTokenCompactWidth),
  ));

  bytes.prune(staticTokenCompactWidth);

  const dynamicToken = await opts.decodeDynamicToken(bytes);

  let offset: bigint;

  if (isOffsetEncoded) {
    await bytes.nextAbsolute(offsetCompactWidth);

    offset = BigInt(
      decodeCompactWidth(bytes.array.subarray(0, offsetCompactWidth)),
    );

    bytes.prune(offsetCompactWidth);
  } else {
    // This is just to keep Typescript happy, we'll assign it properly after decoding the entry.
    offset = 0n;
  }

  let entry: Entry<NamespaceId, SubspaceId, PayloadDigest>;

  if (isEntryEncodedRelative) {
    entry = await decodeStreamEntryRelativeEntry(
      {
        decodeStreamNamespace: opts.decodeNamespaceId,
        decodeStreamSubspace: opts.decodeSubspaceId,
        decodeStreamPayloadDigest: opts.decodePayloadDigest,
        pathScheme: opts.pathScheme,
      },
      bytes,
      opts.currentlyReceivedEntry,
    );
  } else if (
    !isEntryEncodedRelative && senderHandleCompactWidth > 0 &&
    receiverHandleCompactWidth > 0
  ) {
    await bytes.nextAbsolute(
      senderHandleCompactWidth + receiverHandleCompactWidth,
    );

    const senderHandle = BigInt(
      decodeCompactWidth(bytes.array.subarray(0, senderHandleCompactWidth)),
    );
    const receiverHandle = BigInt(
      decodeCompactWidth(
        bytes.array.subarray(
          senderHandleCompactWidth,
          senderHandleCompactWidth + receiverHandleCompactWidth,
        ),
      ),
    );

    bytes.prune(senderHandleCompactWidth + receiverHandleCompactWidth);

    entry = await decodeStreamEntryInNamespaceArea(
      {
        decodeStreamPayloadDigest: opts.decodePayloadDigest,
        decodeStreamSubspace: opts.decodeSubspaceId,
        pathScheme: opts.pathScheme,
      },
      bytes,
      opts.aoiHandlesToArea(senderHandle, receiverHandle),
      opts.aoiHandlesToNamespace(senderHandle, receiverHandle),
    );
  } else {
    throw new Error(
      "Could not decode entry encoded relative to area when no handles are provided",
    );
  }

  if (!isOffsetEncoded) {
    offset = isOffsetPayloadLengthOrZero ? entry.payloadLength : 0n;
  }

  return {
    kind: MSG_DATA_SEND_ENTRY,
    entry,
    dynamicToken,
    offset,
    staticTokenHandle,
  };
}
