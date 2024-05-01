import {
  decodeCompactWidth,
  GrowingBytes,
  StreamDecoder,
} from "../../../deps.ts";
import {
  MsgKind,
  MsgPaiBindFragment,
  MsgPaiReplyFragment,
  MsgPaiReplySubspaceCapability,
  MsgPaiRequestSubspaceCapability,
} from "../types.ts";
import { compactWidthFromEndOfByte } from "./util.ts";

export async function decodePaiBindFragment<PsiGroup>(
  bytes: GrowingBytes,
  groupDecoder: StreamDecoder<PsiGroup>,
): Promise<MsgPaiBindFragment<PsiGroup>> {
  await bytes.nextAbsolute(1);

  const isSecondary = bytes.array[0] === 0x6;

  bytes.prune(1);

  const groupMember = await groupDecoder(bytes);

  return {
    kind: MsgKind.PaiBindFragment,
    isSecondary,
    groupMember,
  };
}

export async function decodePaiReplyFragment<PsiGroup>(
  bytes: GrowingBytes,
  groupDecoder: StreamDecoder<PsiGroup>,
): Promise<MsgPaiReplyFragment<PsiGroup>> {
  await bytes.nextAbsolute(1);

  const compactWidth = compactWidthFromEndOfByte(bytes.array[0]);

  const handle = decodeCompactWidth(bytes.array.subarray(1, 1 + compactWidth));

  bytes.prune(1 + compactWidth);

  const groupMember = await groupDecoder(bytes);

  return {
    kind: MsgKind.PaiReplyFragment,
    groupMember,
    handle: BigInt(handle),
  };
}

export async function decodePaiRequestSubspaceCapability(
  bytes: GrowingBytes,
): Promise<MsgPaiRequestSubspaceCapability> {
  await bytes.nextAbsolute(1);

  const compactWidth = compactWidthFromEndOfByte(bytes.array[0]);

  const handle = decodeCompactWidth(bytes.array.subarray(1, 1 + compactWidth));

  bytes.prune(1 + compactWidth);

  return {
    kind: MsgKind.PaiRequestSubspaceCapability,
    handle: BigInt(handle),
  };
}

export async function decodePaiReplySubspaceCapability<
  SubspaceCapability,
  SyncSubspaceSignature,
>(
  bytes: GrowingBytes,
  decodeCap: StreamDecoder<SubspaceCapability>,
  decodeSig: StreamDecoder<SyncSubspaceSignature>,
): Promise<
  MsgPaiReplySubspaceCapability<
    SubspaceCapability,
    SyncSubspaceSignature
  >
> {
  await bytes.nextAbsolute(1);

  const compactWidth = compactWidthFromEndOfByte(bytes.array[0]);

  const handle = decodeCompactWidth(bytes.array.subarray(1, 1 + compactWidth));

  bytes.prune(1 + compactWidth);

  const capability = await decodeCap(bytes);
  const signature = await decodeSig(bytes);

  return {
    kind: MsgKind.PaiReplySubspaceCapability,
    handle: BigInt(handle),
    capability,
    signature,
  };
}
