import {
  decodeCompactWidth,
  decodeStreamRange3dRelative,
  GrowingBytes,
  PathScheme,
} from "../../../deps.ts";
import {
  MSG_RECONCILIATION_SEND_FINGERPRINT,
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

  const [first] = bytes.array;

  const isFingeprintNeutral = (first & 0x8) === 0x8;

  // We ignore bit 5 as we always encode relative to the previous range.

  const isSenderPrevSender = (first & 0x2) === 0x2;
  const isReceiverPrevReceiver = (first & 0x1) === 0x1;

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

  const range = await decodeStreamRange3dRelative(
    {
      pathScheme: opts.pathScheme,
      decodeStreamSubspaceId: opts.decodeSubspaceId,
    },
    bytes,
    privy.previousRange,
  );

  return {
    kind: MSG_RECONCILIATION_SEND_FINGERPRINT,
    fingerprint,
    range,
    receiverHandle,
    senderHandle,
  };
}
