import {
  compactWidth,
  concat,
  encodeCompactWidth,
  encodeRange3dRelative,
  PathScheme,
  TotalOrder,
} from "../../../deps.ts";
import {
  MsgReconciliationAnnounceEntries,
  MsgReconciliationSendFingerprint,
  ReconciliationPrivy,
} from "../types.ts";
import { compactWidthOr } from "./util.ts";

export function encodeReconciliationSendFingerprint<SubspaceId, Fingerprint>(
  msg: MsgReconciliationSendFingerprint<SubspaceId, Fingerprint>,
  opts: {
    orderSubspace: TotalOrder<SubspaceId>;
    encodeSubspaceId: (subspace: SubspaceId) => Uint8Array;
    pathScheme: PathScheme;
    isFingerprintNeutral: (fp: Fingerprint) => boolean;
    encodeFingerprint: (fp: Fingerprint) => Uint8Array;
    privy: ReconciliationPrivy<SubspaceId>;
  },
): Uint8Array {
  // header relies on prev_range, prev_sender_handle, prev_receiver_handle
  const messageTypeMask = 0x40;

  const neutralMask = opts.isFingerprintNeutral(msg.fingerprint) ? 0x8 : 0x0;

  const encodedRelativeToPrevRange = 0x4;

  const senderHandleIsSame =
    msg.senderHandle === opts.privy.previousSenderHandle;
  const receiverHandleIsSame =
    msg.receiverHandle === opts.privy.previousReceiverHandle;

  const usingPrevSenderHandleMask = senderHandleIsSame ? 0x2 : 0x0;

  const usingPrevReceiverHandleMask = receiverHandleIsSame ? 0x1 : 0x0;

  const headerByte = messageTypeMask | neutralMask |
    encodedRelativeToPrevRange | usingPrevSenderHandleMask |
    usingPrevReceiverHandleMask;

  let handleLengthByte;

  const compactWidthSender = compactWidth(msg.senderHandle);
  const compactWidthReceiver = compactWidth(msg.receiverHandle);

  if (!senderHandleIsSame && !receiverHandleIsSame) {
    const unshifted = compactWidthOr(0x0, compactWidthSender);
    const shifted2 = unshifted << 2;
    const unshifted2 = compactWidthOr(shifted2, compactWidthReceiver);
    const lengths = unshifted2 << 4;

    handleLengthByte = new Uint8Array([lengths]);
  } else if (!senderHandleIsSame && receiverHandleIsSame) {
    const unshifted = compactWidthOr(0x0, compactWidthSender);
    const lengths = unshifted << 6;

    handleLengthByte = new Uint8Array([lengths]);
  } else if (senderHandleIsSame && !receiverHandleIsSame) {
    const unshifted = compactWidthOr(0x0, compactWidthReceiver);
    const lengths = unshifted << 4;

    handleLengthByte = new Uint8Array([lengths]);
  } else {
    handleLengthByte = new Uint8Array();
  }

  const encodedSenderHandle = senderHandleIsSame
    ? new Uint8Array()
    : encodeCompactWidth(msg.senderHandle);
  const encodedReceiverHandle = receiverHandleIsSame
    ? new Uint8Array()
    : encodeCompactWidth(msg.receiverHandle);

  const encodedFingerprint = opts.isFingerprintNeutral(msg.fingerprint)
    ? new Uint8Array()
    : opts.encodeFingerprint(msg.fingerprint);

  const encodedRelativeRange = encodeRange3dRelative(
    {
      encodeSubspaceId: opts.encodeSubspaceId,
      orderSubspace: opts.orderSubspace,
      pathScheme: opts.pathScheme,
    },
    msg.range,
    opts.privy.previousRange,
  );

  return concat(
    new Uint8Array([messageTypeMask | headerByte]),
    handleLengthByte,
    encodedSenderHandle,
    encodedReceiverHandle,
    encodedFingerprint,
    encodedRelativeRange,
  );
}

export function encodeReconciliationAnnounceEntries<SubspaceId>(
  msg: MsgReconciliationAnnounceEntries<SubspaceId>,
  opts: {
    privy: ReconciliationPrivy<SubspaceId>;
    orderSubspace: TotalOrder<SubspaceId>;
    encodeSubspaceId: (subspace: SubspaceId) => Uint8Array;
    pathScheme: PathScheme;
  },
): Uint8Array {
  // First byte

  const messageTypeMask = 0x50;

  const wantResponseBit = msg.wantResponse ? 0x8 : 0x0;

  const encodedRelativebit = 0x4;

  const senderHandleIsSame =
    msg.senderHandle === opts.privy.previousSenderHandle;
  const receiverHandleIsSame =
    msg.receiverHandle === opts.privy.previousReceiverHandle;

  const usingPrevSenderHandleMask = senderHandleIsSame ? 0x2 : 0x0;

  const usingPrevReceiverHandleMask = receiverHandleIsSame ? 0x1 : 0x0;

  const firstByte = messageTypeMask | wantResponseBit | encodedRelativebit |
    usingPrevSenderHandleMask | usingPrevReceiverHandleMask;

  // Second byte

  const compactWidthSender = compactWidth(msg.senderHandle);
  const compactWidthReceiver = compactWidth(msg.receiverHandle);

  let senderReceiverWidthFlags;

  if (!senderHandleIsSame && !receiverHandleIsSame) {
    const unshifted = compactWidthOr(0x0, compactWidthSender);
    const shifted2 = unshifted << 2;
    const unshifted2 = compactWidthOr(shifted2, compactWidthReceiver);
    senderReceiverWidthFlags = unshifted2 << 4;
  } else if (!senderHandleIsSame && receiverHandleIsSame) {
    const unshifted = compactWidthOr(0x0, compactWidthSender);
    senderReceiverWidthFlags = unshifted << 6;
  } else if (senderHandleIsSame && !receiverHandleIsSame) {
    const unshifted = compactWidthOr(0x0, compactWidthReceiver);
    senderReceiverWidthFlags = unshifted << 4;
  } else {
    senderReceiverWidthFlags = 0x0;
  }

  const countCompactWidth = compactWidth(msg.count);

  const countCompactWidthFlags = compactWidthOr(0, countCompactWidth) << 2;

  const willSortFlag = msg.willSort ? 0x2 : 0x0;

  const secondByte = senderReceiverWidthFlags | countCompactWidthFlags |
    willSortFlag;

  const encodedSenderHandle = !senderHandleIsSame
    ? encodeCompactWidth(msg.senderHandle)
    : new Uint8Array();

  const encodedReceiverHandle = !receiverHandleIsSame
    ? encodeCompactWidth(msg.receiverHandle)
    : new Uint8Array();

  const encodedCount = encodeCompactWidth(msg.count);

  const encodedRelativeRange = encodeRange3dRelative(
    {
      encodeSubspaceId: opts.encodeSubspaceId,
      orderSubspace: opts.orderSubspace,
      pathScheme: opts.pathScheme,
    },
    msg.range,
    opts.privy.previousRange,
  );

  return concat(
    new Uint8Array([firstByte, secondByte]),
    encodedSenderHandle,
    encodedReceiverHandle,
    encodedCount,
    encodedRelativeRange,
  );
}
