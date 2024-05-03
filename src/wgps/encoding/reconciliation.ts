import {
  compactWidth,
  concat,
  encodeCompactWidth,
  encodeEntryRelativeEntry,
  encodeRange3dRelative,
  PathScheme,
  TotalOrder,
} from "../../../deps.ts";
import {
  COVERS_NONE,
  MsgReconciliationAnnounceEntries,
  MsgReconciliationSendEntry,
  MsgReconciliationSendFingerprint,
  MsgReconciliationSendPayload,
  ReconciliationPrivy,
} from "../types.ts";
import { compactWidthOr } from "./util.ts";

export function encodeReconciliationSendFingerprint<
  Fingerprint,
  NamespaceId,
  SubspaceId,
  PayloadDigest,
>(
  msg: MsgReconciliationSendFingerprint<SubspaceId, Fingerprint>,
  opts: {
    orderSubspace: TotalOrder<SubspaceId>;
    encodeSubspaceId: (subspace: SubspaceId) => Uint8Array;
    pathScheme: PathScheme;
    isFingerprintNeutral: (fp: Fingerprint) => boolean;
    encodeFingerprint: (fp: Fingerprint) => Uint8Array;
    privy: ReconciliationPrivy<NamespaceId, SubspaceId, PayloadDigest>;
  },
): Uint8Array {
  // header relies on prev_range, prev_sender_handle, prev_receiver_handle
  const messageTypeMask = 0x40;

  const neutralMask = opts.isFingerprintNeutral(msg.fingerprint) ? 0x8 : 0x0;

  const encodedRelativeToPrevRange = 0x4;

  const senderHandleIsSame = msg.senderHandle === opts.privy.prevSenderHandle;
  const receiverHandleIsSame =
    msg.receiverHandle === opts.privy.prevReceiverHandle;

  const usingPrevSenderHandleMask = senderHandleIsSame ? 0x2 : 0x0;

  const usingPrevReceiverHandleMask = receiverHandleIsSame ? 0x1 : 0x0;

  const headerByte = messageTypeMask | neutralMask |
    encodedRelativeToPrevRange | usingPrevSenderHandleMask |
    usingPrevReceiverHandleMask;

  let handleLengthNumber = 0x0;

  const compactWidthSender = compactWidth(msg.senderHandle);
  const compactWidthReceiver = compactWidth(msg.receiverHandle);

  if (!senderHandleIsSame) {
    const unshifted = compactWidthOr(0x0, compactWidthSender);
    const shifted = unshifted << 6;
    handleLengthNumber = handleLengthNumber | shifted;
  }

  if (!receiverHandleIsSame) {
    const unshifted = compactWidthOr(0x0, compactWidthReceiver);
    const shifted = unshifted << 4;
    handleLengthNumber = handleLengthNumber | shifted;
  }

  if (msg.covers !== COVERS_NONE) {
    handleLengthNumber = handleLengthNumber | 0x8;
    handleLengthNumber = compactWidthOr(
      handleLengthNumber,
      compactWidth(msg.covers),
    );
  }

  const handleLengthByte = new Uint8Array([handleLengthNumber]);

  const encodedCovers = msg.covers === COVERS_NONE
    ? new Uint8Array()
    : encodeCompactWidth(msg.covers);

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
    opts.privy.prevRange,
  );

  return concat(
    new Uint8Array([messageTypeMask | headerByte]),
    handleLengthByte,
    encodedCovers,
    encodedSenderHandle,
    encodedReceiverHandle,
    encodedFingerprint,
    encodedRelativeRange,
  );
}

export function encodeReconciliationAnnounceEntries<
  NamespaceId,
  SubspaceId,
  PayloadDigest,
>(
  msg: MsgReconciliationAnnounceEntries<SubspaceId>,
  opts: {
    privy: ReconciliationPrivy<NamespaceId, SubspaceId, PayloadDigest>;
    orderSubspace: TotalOrder<SubspaceId>;
    encodeSubspaceId: (subspace: SubspaceId) => Uint8Array;
    pathScheme: PathScheme;
  },
): Uint8Array {
  // First byte

  const messageTypeMask = 0x50;

  const wantResponseBit = msg.wantResponse ? 0x8 : 0x0;

  const encodedRelativebit = 0x4;

  const senderHandleIsSame = msg.senderHandle === opts.privy.prevSenderHandle;
  const receiverHandleIsSame =
    msg.receiverHandle === opts.privy.prevReceiverHandle;

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
    opts.privy.prevRange,
  );

  return concat(
    new Uint8Array([firstByte, secondByte]),
    encodedSenderHandle,
    encodedReceiverHandle,
    encodedCount,
    encodedRelativeRange,
  );
}

export function encodeReconciliationSendEntry<
  DynamicToken,
  NamespaceId,
  SubspaceId,
  PayloadDigest,
>(
  msg: MsgReconciliationSendEntry<
    DynamicToken,
    NamespaceId,
    SubspaceId,
    PayloadDigest
  >,
  opts: {
    privy: ReconciliationPrivy<NamespaceId, SubspaceId, PayloadDigest>;
    isEqualNamespace: (a: NamespaceId, b: NamespaceId) => boolean;
    orderSubspace: TotalOrder<SubspaceId>;
    encodeNamespaceId: (namespace: NamespaceId) => Uint8Array;
    encodeSubspaceId: (subspace: SubspaceId) => Uint8Array;
    encodePayloadDigest: (digest: PayloadDigest) => Uint8Array;
    encodeDynamicToken: (token: DynamicToken) => Uint8Array;
    pathScheme: PathScheme;
  },
): Uint8Array {
  const messageTypeMask = 0x50;

  const isPrevTokenEqual =
    msg.staticTokenHandle === opts.privy.prevStaticTokenHandle;

  const isPrevStaticTokenFlag = isPrevTokenEqual ? 0x8 : 0x0;

  const isEncodedRelativeToPrevEntryFlag = 0x4;

  const compactWidthAvailableFlag = compactWidthOr(
    0,
    compactWidth(msg.entry.available),
  );

  const header = messageTypeMask | isPrevStaticTokenFlag |
    isEncodedRelativeToPrevEntryFlag | compactWidthAvailableFlag;

  let encodedStaticTokenWidth: Uint8Array;

  const compactWidthStaticToken = compactWidth(msg.staticTokenHandle);

  if (isPrevTokenEqual) {
    encodedStaticTokenWidth = new Uint8Array();
  } else if (msg.staticTokenHandle < 63n) {
    encodedStaticTokenWidth = new Uint8Array([Number(msg.staticTokenHandle)]);
  } else if (compactWidthStaticToken === 1) {
    encodedStaticTokenWidth = new Uint8Array([0x3f]);
  } else if (compactWidthStaticToken === 2) {
    encodedStaticTokenWidth = new Uint8Array([0x7f]);
  } else if (compactWidthStaticToken === 4) {
    encodedStaticTokenWidth = new Uint8Array([0xbf]);
  } else {
    encodedStaticTokenWidth = new Uint8Array([0xff]);
  }

  let encodedStaticToken: Uint8Array;

  if (!isPrevTokenEqual && msg.staticTokenHandle > 63n) {
    encodedStaticToken = encodeCompactWidth(msg.staticTokenHandle);
  } else {
    encodedStaticToken = new Uint8Array();
  }

  const encodedAvailable = encodeCompactWidth(msg.entry.available);

  const encodedDynamicToken = opts.encodeDynamicToken(msg.dynamicToken);

  const encodedRelativeEntry = encodeEntryRelativeEntry(
    {
      encodeNamespace: opts.encodeNamespaceId,
      encodeSubspace: opts.encodeSubspaceId,
      isEqualNamespace: opts.isEqualNamespace,
      orderSubspace: opts.orderSubspace,
      encodePayloadDigest: opts.encodePayloadDigest,
      pathScheme: opts.pathScheme,
    },
    msg.entry.entry,
    opts.privy.prevEntry,
  );

  return concat(
    new Uint8Array([header]),
    encodedStaticTokenWidth,
    encodedStaticToken,
    encodedAvailable,
    encodedDynamicToken,
    encodedRelativeEntry,
  );
}

export function encodeReconciliationSendPayload(
  msg: MsgReconciliationSendPayload,
): Uint8Array {
  const header = compactWidthOr(0x50, compactWidth(msg.amount));
  const amountEncoded = encodeCompactWidth(msg.amount);

  return concat(new Uint8Array([header]), amountEncoded, msg.bytes);
}

export function encodeReconciliationTerminatePayload(): Uint8Array {
  return new Uint8Array([0x58]);
}
