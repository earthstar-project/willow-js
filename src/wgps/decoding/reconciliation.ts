import {
  decodeCompactWidth,
  decodeStreamEntryRelativeEntry,
  decodeStreamEntryRelativeRange3d,
  decodeStreamRange3dRelative,
  type Entry,
  type GrowingBytes,
  type PathScheme,
  type Range3d,
} from "@earthstar/willow-utils";
import {
  COVERS_NONE,
  MsgKind,
  type MsgReconciliationAnnounceEntries,
  type MsgReconciliationSendEntry,
  type MsgReconciliationSendFingerprint,
  type MsgReconciliationSendPayload,
  type MsgReconciliationTerminatePayload,
  type ReconciliationPrivy,
} from "../types.ts";
import { compactWidthFromEndOfByte } from "./util.ts";

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

  await bytes.nextAbsolute(2);

  const [firstByte, secondByte] = bytes.array;

  const isFingeprintNeutral = (firstByte & 0x8) === 0x8;

  const encodedRelativeToPrevRange = (firstByte & 0x4) === 0x4;

  const isSenderPrevSender = (firstByte & 0x2) === 0x2;
  const isReceiverPrevReceiver = (firstByte & 0x1) === 0x1;

  const senderCompactWidth = 2 ** (secondByte >> 6);
  const receiverCompactWidth = 2 ** ((secondByte >> 4) & 0x3);

  const coversNotNone = (secondByte & 0x8) === 0x8;
  const coversCompactWidth = 2 ** (secondByte & 0x3);

  let covers: bigint | typeof COVERS_NONE;
  let senderHandle: bigint;
  let receiverHandle: bigint;

  bytes.prune(2);

  if (coversNotNone) {
    await bytes.nextAbsolute(coversCompactWidth);

    covers = BigInt(
      decodeCompactWidth(bytes.array.subarray(0, coversCompactWidth)),
    );

    bytes.prune(coversCompactWidth);
  } else {
    covers = COVERS_NONE;
  }

  if (!isSenderPrevSender) {
    await bytes.nextAbsolute(senderCompactWidth);

    senderHandle = BigInt(
      decodeCompactWidth(bytes.array.subarray(0, senderCompactWidth)),
    );

    bytes.prune(senderCompactWidth);
  } else {
    senderHandle = privy.prevSenderHandle;
  }

  if (!isReceiverPrevReceiver) {
    await bytes.nextAbsolute(receiverCompactWidth);

    receiverHandle = BigInt(
      decodeCompactWidth(bytes.array.subarray(0, receiverCompactWidth)),
    );

    bytes.prune(receiverCompactWidth);
  } else {
    receiverHandle = privy.prevReceiverHandle;
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
    kind: MsgKind.ReconciliationSendFingerprint,
    fingerprint,
    range,
    receiverHandle,
    senderHandle,
    covers,
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

  const isEmpty = (secondByte & 0x4) === 0x4;

  const willSort = (secondByte & 0x2) === 0x2;

  const coversNotNone = (secondByte & 0x1) === 0x1;

  let covers: bigint | typeof COVERS_NONE;

  bytes.prune(2);

  if (coversNotNone) {
    await bytes.nextAbsolute(1);

    const [coversLength] = bytes.array;

    if ((coversLength & 0xfc) == 0xfc) {
      const coversCompactWidth = 2 ** (coversLength & 0x3);

      await bytes.nextAbsolute(coversCompactWidth);

      covers = BigInt(
        decodeCompactWidth(bytes.array.slice(1, 1 + coversCompactWidth)),
      );

      bytes.prune(1 + coversCompactWidth);
    } else {
      covers = BigInt(coversLength);
      bytes.prune(1);
    }
  } else {
    covers = COVERS_NONE;
  }

  let senderHandle: bigint;
  let receiverHandle: bigint;

  if (!isSenderPrevSender) {
    const senderCompactWidth = 2 ** (secondByte >> 6);
    await bytes.nextAbsolute(senderCompactWidth);

    senderHandle = BigInt(
      decodeCompactWidth(bytes.array.subarray(0, senderCompactWidth)),
    );

    bytes.prune(senderCompactWidth);
  } else {
    senderHandle = privy.prevSenderHandle;
  }

  if (!isReceiverPrevReceiver) {
    const receiverCompactWidth = 2 ** ((secondByte >> 4) & 0x3);
    await bytes.nextAbsolute(receiverCompactWidth);

    receiverHandle = BigInt(
      decodeCompactWidth(bytes.array.subarray(0, receiverCompactWidth)),
    );

    bytes.prune(receiverCompactWidth);
  } else {
    receiverHandle = privy.prevReceiverHandle;
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
    kind: MsgKind.ReconciliationAnnounceEntries,
    range,
    isEmpty,
    receiverHandle,
    senderHandle,
    wantResponse,
    willSort,
    covers,
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
    kind: MsgKind.ReconciliationSendEntry,
    dynamicToken,
    entry: {
      available,
      entry,
    },
    staticTokenHandle,
  };
}

export async function decodeReconciliationSendPayload(
  bytes: GrowingBytes,
): Promise<MsgReconciliationSendPayload> {
  await bytes.nextAbsolute(1);

  const amountCompactWidth = compactWidthFromEndOfByte(bytes.array[0]);

  await bytes.nextAbsolute(1 + amountCompactWidth);

  const amount = decodeCompactWidth(
    bytes.array.subarray(1, 1 + amountCompactWidth),
  );

  bytes.prune(1 + amountCompactWidth);

  await bytes.nextAbsolute(Number(amount));

  const messageBytes = bytes.array.slice(0, Number(amount));

  bytes.prune(Number(amount));

  return {
    kind: MsgKind.ReconciliationSendPayload,
    amount: BigInt(amount),
    bytes: messageBytes,
  };
}

export async function decodeReconciliationTerminatePayload(
  bytes: GrowingBytes,
): Promise<MsgReconciliationTerminatePayload> {
  await bytes.nextAbsolute(1);

  const isFinal = (bytes.array[0] & 0x4) === 0x4;

  bytes.prune(1);

  return {
      kind: MsgKind.ReconciliationTerminatePayload,
      isFinal,
  };
}
