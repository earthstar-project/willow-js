import {
  decodeCompactWidth,
  decodeStreamRange3dRelative,
  GrowingBytes,
  PathScheme,
} from "../../../deps.ts";
import {
  MSG_RECONCILIATION_ANNOUNCE_ENTRIES,
  MSG_RECONCILIATION_SEND_FINGERPRINT,
  MsgReconciliationAnnounceEntries,
  MsgReconciliationSendFingerprint,
  ReconciliationPrivy,
} from "../types.ts";

export async function decodeReconciliationSendFingerprint<
  SubspaceId,
  Fingerprint,
>(
  bytes: GrowingBytes,
  opts: {
    neutralFingerprint: Fingerprint;
    decodeFingerprint: (bytes: GrowingBytes) => Promise<Fingerprint>;
    decodeSubspaceId: (bytes: GrowingBytes) => Promise<SubspaceId>;
    pathScheme: PathScheme;
    getPrivy: () => ReconciliationPrivy<SubspaceId>;
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
    receiverHandle = privy.previousReceiverHandle;

    bytes.prune(2 + senderCompactWidth);
  } else if (isSenderPrevSender && !isReceiverPrevReceiver) {
    await bytes.nextAbsolute(2);
    const [, second] = bytes.array;

    const receiverCompactWidth = 2 ** ((second >> 4) & 0x3);

    await bytes.nextAbsolute(2 + receiverCompactWidth);

    senderHandle = privy.previousSenderHandle;
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
    senderHandle = privy.previousSenderHandle;
    receiverHandle = privy.previousReceiverHandle;

    bytes.prune(1);
  }

  let fingerprint: Fingerprint;

  if (isFingeprintNeutral) {
    fingerprint = opts.neutralFingerprint;
  } else {
    fingerprint = await opts.decodeFingerprint(bytes);
  }

  const outer = encodedRelativeToPrevRange
    ? privy.previousRange
    : privy.aoiHandlesToRange3d(receiverHandle, senderHandle);

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

export async function decodeReconciliationAnnounceEntries<SubspaceId>(
  bytes: GrowingBytes,
  opts: {
    decodeSubspaceId: (bytes: GrowingBytes) => Promise<SubspaceId>;
    pathScheme: PathScheme;
    getPrivy: () => ReconciliationPrivy<SubspaceId>;
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
    receiverHandle = privy.previousReceiverHandle;

    bytes.prune(2 + senderCompactWidth);
  } else if (isSenderPrevSender && !isReceiverPrevReceiver) {
    const receiverCompactWidth = 2 ** ((secondByte >> 4) & 0x3);

    await bytes.nextAbsolute(2 + receiverCompactWidth);

    senderHandle = privy.previousSenderHandle;
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
    senderHandle = privy.previousSenderHandle;
    receiverHandle = privy.previousReceiverHandle;

    bytes.prune(2);
  }

  await bytes.nextAbsolute(countWidth);

  const count = BigInt(decodeCompactWidth(bytes.array.subarray(0, countWidth)));

  bytes.prune(countWidth);

  const outer = encodedRelativeToPrevRange
    ? privy.previousRange
    : privy.aoiHandlesToRange3d(receiverHandle, senderHandle);

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
