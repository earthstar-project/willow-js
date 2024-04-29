import {
  Area,
  decodeCompactWidth,
  decodeStreamAreaInArea,
  EncodingScheme,
  GrowingBytes,
  PathScheme,
} from "../../../deps.ts";
import {
  MsgKind,
  MsgSetupBindAreaOfInterest,
  MsgSetupBindReadCapability,
  MsgSetupBindStaticToken,
  ReadCapEncodingScheme,
  ReadCapPrivy,
} from "../types.ts";
import { compactWidthFromEndOfByte } from "./util.ts";

export async function decodeSetupBindReadCapability<
  ReadCapability,
  SyncSignature,
  NamespaceId,
  SubspaceId,
>(
  bytes: GrowingBytes,
  readCapScheme: ReadCapEncodingScheme<
    ReadCapability,
    NamespaceId,
    SubspaceId
  >,
  getPrivy: (handle: bigint) => ReadCapPrivy<NamespaceId, SubspaceId>,
  decodeSignature: (bytes: GrowingBytes) => Promise<SyncSignature>,
): Promise<MsgSetupBindReadCapability<ReadCapability, SyncSignature>> {
  await bytes.nextAbsolute(1);

  const compactWidth = compactWidthFromEndOfByte(bytes.array[0]);

  await bytes.nextAbsolute(1 + compactWidth);

  const handle = decodeCompactWidth(bytes.array.subarray(1, 1 + compactWidth));

  bytes.prune(1 + compactWidth);

  const privy = getPrivy(BigInt(handle));

  const capability = await readCapScheme.decodeStream(bytes, privy);
  const signature = await decodeSignature(bytes);

  return {
    kind: MsgKind.SetupBindReadCapability,
    handle: BigInt(handle),
    capability,
    signature,
  };
}

export async function decodeSetupBindAreaOfInterest<SubspaceId>(
  bytes: GrowingBytes,
  getPrivy: (handle: bigint) => Promise<Area<SubspaceId>>,
  decodeStreamSubspace: EncodingScheme<SubspaceId>["decodeStream"],
  pathScheme: PathScheme,
): Promise<MsgSetupBindAreaOfInterest<SubspaceId>> {
  await bytes.nextAbsolute(1);

  const hasALimit = (0x4 & bytes.array[0]) == 0x4;

  const compactWidth = compactWidthFromEndOfByte(bytes.array[0]);

  await bytes.nextAbsolute(1 + compactWidth);

  const authHandle = decodeCompactWidth(
    bytes.array.subarray(1, 1 + compactWidth),
  );

  bytes.prune(1 + compactWidth);

  const outer = await getPrivy(BigInt(authHandle));

  const area = await decodeStreamAreaInArea(
    {
      decodeStreamSubspace,
      pathScheme,
    },
    bytes,
    outer,
  );

  if (!hasALimit) {
    return {
      kind: MsgKind.SetupBindAreaOfInterest,
      areaOfInterest: {
        area: area,
        maxCount: 0,
        maxSize: BigInt(0),
      },
      authorisation: BigInt(authHandle),
    };
  }

  await bytes.nextAbsolute(1);

  const maxes = bytes.array[0];

  const compactWidthCount = compactWidthFromEndOfByte(maxes >> 6);
  const compactWidthSize = compactWidthFromEndOfByte(maxes >> 4);

  await bytes.nextAbsolute(1 + compactWidthCount + compactWidthSize);

  const maxCount = decodeCompactWidth(
    bytes.array.subarray(1, 1 + compactWidthCount),
  );

  const maxSize = decodeCompactWidth(
    bytes.array.subarray(
      1 + compactWidthCount,
      1 + compactWidthCount + compactWidthSize,
    ),
  );

  bytes.prune(1 + compactWidthCount + compactWidthSize);

  return {
    kind: MsgKind.SetupBindAreaOfInterest,
    areaOfInterest: {
      area: area,
      maxCount: Number(maxCount),
      maxSize: BigInt(maxSize),
    },
    authorisation: BigInt(authHandle),
  };
}

export async function decodeSetupBindStaticToken<StaticToken>(
  bytes: GrowingBytes,
  decodeStaticToken: (bytes: GrowingBytes) => Promise<StaticToken>,
): Promise<MsgSetupBindStaticToken<StaticToken>> {
  await bytes.nextAbsolute(1);

  bytes.prune(1);

  const staticToken = await decodeStaticToken(bytes);

  return {
    kind: MsgKind.SetupBindStaticToken,
    staticToken,
  };
}
