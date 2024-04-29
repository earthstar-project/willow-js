import {
  Area,
  decodeCompactWidth,
  decodeStreamEntryInNamespaceArea,
  decodeStreamEntryRelativeEntry,
  Entry,
  GrowingBytes,
  PathScheme,
} from "../../../deps.ts";
import {
  MsgDataBindPayloadRequest,
  MsgDataReplyPayload,
  MsgDataSendEntry,
  MsgDataSendPayload,
  MsgDataSetMetadata,
  MsgKind,
} from "../types.ts";
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
    kind: MsgKind.DataSendEntry,
    entry,
    dynamicToken,
    offset,
    staticTokenHandle,
  };
}

export async function decodeDataSendPayload(
  bytes: GrowingBytes,
): Promise<MsgDataSendPayload> {
  await bytes.nextAbsolute(1);

  const [header] = bytes.array;

  const compactWidthAmount = compactWidthFromEndOfByte(header);

  await bytes.nextAbsolute(1 + compactWidthAmount);

  const amount = Number(
    decodeCompactWidth(bytes.array.subarray(1, 1 + compactWidthAmount)),
  );

  await bytes.nextAbsolute(1 + compactWidthAmount + amount);

  const msgBytes = bytes.array.slice(
    1 + compactWidthAmount,
    1 + compactWidthAmount + amount,
  );

  bytes.prune(1 + compactWidthAmount + amount);

  return {
    kind: MsgKind.DataSendPayload,
    amount: BigInt(amount),
    bytes: msgBytes,
  };
}

export async function decodeDataSetEagerness(
  bytes: GrowingBytes,
): Promise<MsgDataSetMetadata> {
  await bytes.nextAbsolute(2);

  const [firstByte, secondByte] = bytes.array;

  const isEager = (firstByte & 0x2) === 0x2;

  const compactWidthSenderHandle = compactWidthFromEndOfByte(secondByte >> 6);

  const compactWidthReceiverHandle = compactWidthFromEndOfByte(secondByte >> 4);

  await bytes.nextAbsolute(
    2 + compactWidthSenderHandle + compactWidthReceiverHandle,
  );

  const senderHandle = BigInt(
    decodeCompactWidth(bytes.array.subarray(2, 2 + compactWidthSenderHandle)),
  );

  const receiverHandle = BigInt(
    decodeCompactWidth(
      bytes.array.subarray(
        2 + compactWidthSenderHandle,
        2 + compactWidthSenderHandle + compactWidthReceiverHandle,
      ),
    ),
  );

  bytes.prune(2 + compactWidthSenderHandle + compactWidthReceiverHandle);

  return {
    kind: MsgKind.DataSetMetadata,
    isEager,
    receiverHandle,
    senderHandle,
  };
}

export async function decodeDataBindPayloadRequest<
  NamespaceId,
  SubspaceId,
  PayloadDigest,
>(
  bytes: GrowingBytes,
  opts: {
    decodeNamespaceId: (bytes: GrowingBytes) => Promise<NamespaceId>;
    decodeSubspaceId: (bytes: GrowingBytes) => Promise<SubspaceId>;
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
): Promise<MsgDataBindPayloadRequest<NamespaceId, SubspaceId, PayloadDigest>> {
  await bytes.nextAbsolute(2);

  const [firstByte, secondByte] = bytes.array;

  const compactWidthCapability = compactWidthFromEndOfByte(firstByte);

  const isOffsetEncoded = (secondByte & 0x80) === 0x80;

  const compactWidthOffset = isOffsetEncoded
    ? compactWidthFromEndOfByte((secondByte & 0x60) >> 5)
    : 0;

  const isEncodedRelativeToCurrEntry = (secondByte & 0x10) === 0x10;

  const compactWidthSenderHandle = isEncodedRelativeToCurrEntry
    ? 0
    : compactWidthFromEndOfByte(secondByte >> 2);

  const compactWidthReceiverHandle = isEncodedRelativeToCurrEntry
    ? 0
    : compactWidthFromEndOfByte(secondByte);

  await bytes.nextAbsolute(2 + compactWidthCapability);

  const capability = BigInt(decodeCompactWidth(
    bytes.array.subarray(2, 2 + compactWidthCapability),
  ));

  let offset: bigint;

  if (isOffsetEncoded) {
    await bytes.nextAbsolute(2 + compactWidthCapability + compactWidthOffset);

    offset = BigInt(
      decodeCompactWidth(
        bytes.array.subarray(
          2 + compactWidthCapability,
          2 + compactWidthCapability + compactWidthOffset,
        ),
      ),
    );

    bytes.prune(2 + compactWidthCapability + compactWidthOffset);
  } else {
    offset = 0n;

    bytes.prune(2 + compactWidthCapability);
  }

  let entry: Entry<NamespaceId, SubspaceId, PayloadDigest>;

  if (isEncodedRelativeToCurrEntry) {
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
    !isEncodedRelativeToCurrEntry && compactWidthSenderHandle > 0 &&
    compactWidthReceiverHandle > 0
  ) {
    await bytes.nextAbsolute(
      compactWidthSenderHandle + compactWidthReceiverHandle,
    );

    const senderHandle = BigInt(
      decodeCompactWidth(bytes.array.subarray(0, compactWidthSenderHandle)),
    );

    const receiverHandle = BigInt(
      decodeCompactWidth(
        bytes.array.subarray(
          compactWidthSenderHandle,
          compactWidthSenderHandle + compactWidthReceiverHandle,
        ),
      ),
    );

    entry = await decodeStreamEntryInNamespaceArea(
      {
        decodeStreamSubspace: opts.decodeSubspaceId,
        decodeStreamPayloadDigest: opts.decodePayloadDigest,
        pathScheme: opts.pathScheme,
      },
      bytes,
      opts.aoiHandlesToArea(senderHandle, receiverHandle),
      opts.aoiHandlesToNamespace(senderHandle, receiverHandle),
    );
  } else {
    throw new Error();
  }

  return {
    kind: MsgKind.DataBindPayloadRequest,
    capability,
    entry,
    offset,
  };
}

export async function decodeDataReplyPayload(
  bytes: GrowingBytes,
): Promise<MsgDataReplyPayload> {
  await bytes.nextAbsolute(1);

  const compactWidthHandle = compactWidthFromEndOfByte(bytes.array[0]);

  await bytes.nextAbsolute(1 + compactWidthHandle);

  const handle = BigInt(
    decodeCompactWidth(bytes.array.subarray(1, 1 + compactWidthHandle)),
  );

  bytes.prune(1 + compactWidthHandle);

  return {
    kind: MsgKind.DataReplyPayload,
    handle,
  };
}
