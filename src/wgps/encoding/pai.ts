import { compactWidth, concat, encodeCompactWidth } from "../../../deps.ts";
import {
  MsgCommitmentReveal,
  MsgPaiBindFragment,
  MsgPaiReplyFragment,
  MsgPaiReplySubspaceCapability,
  MsgPaiRequestSubspaceCapability,
} from "../types.ts";

export function encodeCommitmentReveal(msg: MsgCommitmentReveal): Uint8Array {
  return concat(
    new Uint8Array([0]),
    msg.nonce,
  );
}

export function encodePaiBindFragment<PsiGroup>(
  msg: MsgPaiBindFragment<PsiGroup>,
  encodeGroupMember: (group: PsiGroup) => Uint8Array,
): Uint8Array {
  return concat(
    new Uint8Array([msg.isSecondary ? 6 : 4]),
    encodeGroupMember(msg.groupMember),
  );
}

export function encodePaiReplyFragment<PsiGroup>(
  msg: MsgPaiReplyFragment<PsiGroup>,
  encodeGroupMember: (group: PsiGroup) => Uint8Array,
): Uint8Array {
  const handleWidth = compactWidth(msg.handle);

  const header = handleWidth === 1
    ? 0x8
    : handleWidth === 2
    ? 0x9
    : handleWidth === 4
    ? 0xa
    : 0xb;

  return concat(
    new Uint8Array([header]),
    encodeCompactWidth(msg.handle),
    encodeGroupMember(msg.groupMember),
  );
}

export function encodePaiRequestSubspaceCapability(
  msg: MsgPaiRequestSubspaceCapability,
) {
  const handleWidth = compactWidth(msg.handle);

  const header = handleWidth === 1
    ? 0xc
    : handleWidth === 2
    ? 0xd
    : handleWidth === 4
    ? 0xe
    : 0xf;

  return concat(
    new Uint8Array([header]),
    encodeCompactWidth(msg.handle),
  );
}

export function encodePaiReplySubspaceCapability<
  SubspaceCapability,
  SyncSubspaceSignature,
>(
  msg: MsgPaiReplySubspaceCapability<SubspaceCapability, SyncSubspaceSignature>,
  encodeSubspaceCapability: (cap: SubspaceCapability) => Uint8Array,
  encodeSyncSubspaceSignature: (sig: SyncSubspaceSignature) => Uint8Array,
): Uint8Array {
  const handleWidth = compactWidth(msg.handle);

  const header = handleWidth === 1
    ? 0x10
    : handleWidth === 2
    ? 0x11
    : handleWidth === 4
    ? 0x12
    : 0x13;

  return concat(
    new Uint8Array([header]),
    encodeCompactWidth(msg.handle),
    encodeSubspaceCapability(msg.capability),
    encodeSyncSubspaceSignature(msg.signature),
  );
}
