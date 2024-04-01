import {
  decodeCompactWidth,
  decodeStreamEntryRelativeEntry,
  decodeStreamEntryRelativeRange3d,
  decodeStreamRange3dRelative,
  Entry,
  GrowingBytes,
  PathScheme,
  Range3d,
} from "../../../deps.ts";
import {
  MSG_RECONCILIATION_ANNOUNCE_ENTRIES,
  MSG_RECONCILIATION_SEND_ENTRY,
  MSG_RECONCILIATION_SEND_FINGERPRINT,
  MsgReconciliationAnnounceEntries,
  MsgReconciliationSendEntry,
  MsgReconciliationSendFingerprint,
  ReconciliationPrivy,
} from "../types.ts";

export async function decodeReconciliationSendFingerprint<
  Fingerprint,
  NamespaceId,
  SubspaceId,
  PayloadDigest,
>(
  bytes: GrowingBytes,
  opts: {
    neutralFingerprint: Fingerprint;
    decodeFingerprint: (bytes: GrowingBytes) => Promise<Fingerprint>;
    decodeSubspaceId: (bytes: GrowingBytes) => Promise<SubspaceId>;
    pathScheme: PathScheme;
    getPrivy: () => ReconciliationPrivy<NamespaceId, SubspaceId, PayloadDigest>;
    aoiHandlesToRange3d: (
      senderAoiHandle: bigint,
      receiverAoiHandle: bigint,
    ) => Promise<Range3d<SubspaceId>>;
  },
): Promise<
  MsgReconciliationSendFingerprint<SubspaceId, Fingerprint>
> {
  const privy = opts.getPrivy();

  await bytes.nextAbsolute(1);

  const [firstByte] = bytes.array;

  const isFingeprintNeutral = (firstByte & 0x8) === 0x8;

  const encodedRelativeToPrevRange = (firstByte & 0x4) === 0x4;

  const isSenderPrevSender = (firstByte & 0x2) === 0x2;
  const isReceiverPrevReceiver = (firstByte & 0x1) === 0x1;

  let senderHandle: bigint;
  let receiverHandle: bigint;

  if (!isSenderPrevSender && !isReceiverPrevReceiver) {
    await bytes.nextAbsolute(2);
    const [, second] = bytes.array;

    const senderCompactWidth = 2 ** (second >> 6);
    const receiverCompactWidth = 2 ** ((second >> 4) & 0x3);

    await bytes.nextAbsolute(2 + senderCompactWidth + receiverCompactWidth);

    senderHandle = BigInt(
      decodeCompactWidth(bytes.array.subarray(2, 2 + senderCompactWidth)),
    );
    receiverHandle = BigInt(
      decodeCompactWidth(
        bytes.array.subarray(
          2 + senderCompactWidth,
          2 + senderCompactWidth + receiverCompactWidth,
        ),
      ),
    );

    bytes.prune(2 + senderCompactWidth + receiverCompactWidth);
  } else if (!isSenderPrevSender && isReceiverPrevReceiver) {
    await bytes.nextAbsolute(2);
    const [, second] = bytes.array;

    const senderCompactWidth = 2 ** (second >> 6);

    await bytes.nextAbsolute(2 + senderCompactWidth);

    senderHandle = BigInt(
      decodeCompactWidth(bytes.array.subarray(2, 2 + senderCompactWidth)),
    );
    receiverHandle = privy.prevReceiverHandle;

    bytes.prune(2 + senderCompactWidth);
  } else if (isSenderPrevSender && !isReceiverPrevReceiver) {
    await bytes.nextAbsolute(2);
    const [, second] = bytes.array;

    const receiverCompactWidth = 2 ** ((second >> 4) & 0x3);

    await bytes.nextAbsolute(2 + receiverCompactWidth);

    senderHandle = privy.prevSenderHandle;
    receiverHandle = BigInt(
      decodeCompactWidth(
        bytes.array.subarray(
          2,
          2 + receiverCompactWidth,
        ),
      ),
    );

    bytes.prune(2 + receiverCompactWidth);
  } else {
    senderHandle = privy.prevSenderHandle;
    receiverHandle = privy.prevReceiverHandle;

    bytes.prune(1);
  }

  let fingerprint: Fingerprint;

  if (isFingeprintNeutral) {
    fingerprint = opts.neutralFingerprint;
  } else {
    fingerprint = await opts.decodeFingerprint(bytes);
  }

  const outer = encodedRelativeToPrevRange
    ? privy.prevRange
    : await opts.aoiHandlesToRange3d(senderHandle, receiverHandle);

  const range = await decodeStreamRange3dRelative(
    {
      pathScheme: opts.pathScheme,
      decodeStreamSubspaceId: opts.decodeSubspaceId,
    },
    bytes,
    outer,
  );

  return {
    kind: MSG_RECONCILIATION_SEND_FINGERPRINT,
    fingerprint,
    range,
    receiverHandle,
    senderHandle,
  };
}

export async function decodeReconciliationAnnounceEntries<
  NamespaceId,
  SubspaceId,
  PayloadDigest,
>(
  bytes: GrowingBytes,
  opts: {
    decodeSubspaceId: (bytes: GrowingBytes) => Promise<SubspaceId>;
    pathScheme: PathScheme;
    getPrivy: () => ReconciliationPrivy<NamespaceId, SubspaceId, PayloadDigest>;
    aoiHandlesToRange3d: (
      senderAoiHandle: bigint,
      receiverAoiHandle: bigint,
    ) => Promise<Range3d<SubspaceId>>;
  },
): Promise<MsgReconciliationAnnounceEntries<SubspaceId>> {
  const privy = opts.getPrivy();

  await bytes.nextAbsolute(2);

  const [firstByte, secondByte] = bytes.array;

  const wantResponse = (firstByte & 0x8) === 0x8;

  const encodedRelativeToPrevRange = (firstByte & 0x4) === 0x4;

  const isSenderPrevSender = (firstByte & 0x2) === 0x2;
  const isReceiverPrevReceiver = (firstByte & 0x1) === 0x1;

  const countWidth = 2 ** ((secondByte & 0xc) >> 2);

  const willSort = (secondByte & 0x2) === 0x2;

  let senderHandle: bigint;
  let receiverHandle: bigint;

  if (!isSenderPrevSender && !isReceiverPrevReceiver) {
    const senderCompactWidth = 2 ** (secondByte >> 6);
    const receiverCompactWidth = 2 ** ((secondByte >> 4) & 0x3);

    await bytes.nextAbsolute(2 + senderCompactWidth + receiverCompactWidth);

    senderHandle = BigInt(
      decodeCompactWidth(bytes.array.subarray(2, 2 + senderCompactWidth)),
    );
    receiverHandle = BigInt(
      decodeCompactWidth(
        bytes.array.subarray(
          2 + senderCompactWidth,
          2 + senderCompactWidth + receiverCompactWidth,
        ),
      ),
    );

    bytes.prune(2 + senderCompactWidth + receiverCompactWidth);
  } else if (!isSenderPrevSender && isReceiverPrevReceiver) {
    const senderCompactWidth = 2 ** (secondByte >> 6);

    await bytes.nextAbsolute(2 + senderCompactWidth);

    senderHandle = BigInt(
      decodeCompactWidth(bytes.array.subarray(2, 2 + senderCompactWidth)),
    );
    receiverHandle = privy.prevReceiverHandle;

    bytes.prune(2 + senderCompactWidth);
  } else if (isSenderPrevSender && !isReceiverPrevReceiver) {
    const receiverCompactWidth = 2 ** ((secondByte >> 4) & 0x3);

    await bytes.nextAbsolute(2 + receiverCompactWidth);

    senderHandle = privy.prevSenderHandle;
    receiverHandle = BigInt(
      decodeCompactWidth(
        bytes.array.subarray(
          2,
          2 + receiverCompactWidth,
        ),
      ),
    );

    bytes.prune(2 + receiverCompactWidth);
  } else {
    senderHandle = privy.prevSenderHandle;
    receiverHandle = privy.prevReceiverHandle;

    bytes.prune(2);
  }

  await bytes.nextAbsolute(countWidth);

  const count = BigInt(decodeCompactWidth(bytes.array.subarray(0, countWidth)));

  bytes.prune(countWidth);

  const outer = encodedRelativeToPrevRange
    ? privy.prevRange
    : await opts.aoiHandlesToRange3d(senderHandle, receiverHandle);

  const range = await decodeStreamRange3dRelative(
    {
      pathScheme: opts.pathScheme,
      decodeStreamSubspaceId: opts.decodeSubspaceId,
    },
    bytes,
    outer,
  );

  return {
    kind: MSG_RECONCILIATION_ANNOUNCE_ENTRIES,
    range,
    count,
    receiverHandle,
    senderHandle,
    wantResponse,
    willSort,
  };
}

export async function decodeReconciliationSendEntry<
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
    getPrivy: () => ReconciliationPrivy<NamespaceId, SubspaceId, PayloadDigest>;
  },
): Promise<
  MsgReconciliationSendEntry<
    DynamicToken,
    NamespaceId,
    SubspaceId,
    PayloadDigest
  >
> {
  const privy = opts.getPrivy();

  await bytes.nextAbsolute(1);

  const [header] = bytes.array;

  const isPrevStaticToken = (header & 0x8) === 0x8;
  const isEncodedRelativeToPrev = (header & 0x4) === 0x4;
  const compactWidthAvailable = 2 ** (header & 0x3);

  let staticTokenHandle: bigint;

  if (isPrevStaticToken) {
    staticTokenHandle = privy.prevStaticTokenHandle;
    bytes.prune(1);
  } else {
    await bytes.nextAbsolute(2);

    const [, staticTokenSizeByte] = bytes.array;

    let compactWidth = 0;

    if ((staticTokenSizeByte & 0xff) === 0xff) {
      compactWidth = 8;
    } else if ((staticTokenSizeByte & 0xbf) === 0xbf) {
      compactWidth = 4;
    } else if ((staticTokenSizeByte & 0x7f) === 0x7f) {
      compactWidth = 2;
    } else if ((staticTokenSizeByte & 0x3f) === 0x3f) {
      compactWidth = 1;
    }

    await bytes.nextAbsolute(2 + compactWidth);

    staticTokenHandle = staticTokenSizeByte < 63
      ? BigInt(staticTokenSizeByte)
      : BigInt(
        decodeCompactWidth(bytes.array.subarray(2, 2 + compactWidth)),
      );

    bytes.prune(2 + compactWidth);
  }

  await bytes.nextAbsolute(compactWidthAvailable);

  const available = BigInt(decodeCompactWidth(
    bytes.array.subarray(0, compactWidthAvailable),
  ));

  bytes.prune(compactWidthAvailable);

  const dynamicToken = await opts.decodeDynamicToken(bytes);

  let entry: Entry<NamespaceId, SubspaceId, PayloadDigest>;

  if (isEncodedRelativeToPrev) {
    entry = await decodeStreamEntryRelativeEntry(
      {
        decodeStreamNamespace: opts.decodeNamespaceId,
        decodeStreamPayloadDigest: opts.decodePayloadDigest,
        decodeStreamSubspace: opts.decodeSubspaceId,
        pathScheme: opts.pathScheme,
      },
      bytes,
      privy.prevEntry,
    );
  } else {
    entry = await decodeStreamEntryRelativeRange3d(
      {
        decodeStreamSubspace: opts.decodeSubspaceId,
        pathScheme: opts.pathScheme,
        decodeStreamPayloadDigest: opts.decodePayloadDigest,
      },
      bytes,
      privy.announced.range,
      privy.announced.namespace,
    );
  }

  return {
    kind: MSG_RECONCILIATION_SEND_ENTRY,
    dynamicToken,
    entry: {
      available,
      entry,
    },
    staticTokenHandle,
  };
}
