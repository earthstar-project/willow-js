import {
  Area,
  compactWidth,
  concat,
  encodeAreaInArea,
  encodeCompactWidth,
  PathScheme,
  TotalOrder,
} from "../../../deps.ts";
import {
  MsgSetupBindAreaOfInterest,
  MsgSetupBindReadCapability,
  MsgSetupBindStaticToken,
  ReadCapEncodingScheme,
  ReadCapPrivy,
} from "../types.ts";
import { compactWidthOr } from "./util.ts";

export function encodeSetupBindReadCapability<
  ReadCapability,
  SyncSignature,
  NamespaceId,
  SubspaceId,
>(
  msg: MsgSetupBindReadCapability<ReadCapability, SyncSignature>,
  encodeReadCapability: ReadCapEncodingScheme<
    ReadCapability,
    NamespaceId,
    SubspaceId
  >["encode"],
  encodeSignature: (sig: SyncSignature) => Uint8Array,
  privy: ReadCapPrivy<NamespaceId, SubspaceId>,
): Uint8Array {
  const handleWidth = compactWidth(msg.handle);

  const header = compactWidthOr(0x20, handleWidth);

  return concat(
    new Uint8Array([header]),
    encodeCompactWidth(msg.handle),
    encodeReadCapability(msg.capability, privy),
    encodeSignature(msg.signature),
  );
}

export function encodeSetupBindAreaOfInterest<SubspaceId>(
  msg: MsgSetupBindAreaOfInterest<SubspaceId>,
  opts: {
    outer: Area<SubspaceId>;
    pathScheme: PathScheme;
    encodeSubspace: (subspace: SubspaceId) => Uint8Array;
    orderSubspace: TotalOrder<SubspaceId>;
  },
): Uint8Array {
  // 0x28 masked with compact width of authorisation handle
  // AND masked with 1 at 5th bit if either max count or max size is

  const header = compactWidthOr(0x28, compactWidth(msg.authorisation)) |
    ((msg.areaOfInterest.maxCount !== 0 ||
        msg.areaOfInterest.maxSize !== BigInt(0))
      ? 0x4
      : 0x0);

  const authHandle = encodeCompactWidth(msg.authorisation);

  const areaInArea = encodeAreaInArea(
    {
      pathScheme: opts.pathScheme,
      encodeSubspace: opts.encodeSubspace,
      orderSubspace: opts.orderSubspace,
    },
    msg.areaOfInterest.area,
    opts.outer,
  );

  if (
    msg.areaOfInterest.maxCount === 0 &&
    msg.areaOfInterest.maxSize === BigInt(0)
  ) {
    return concat(new Uint8Array([header]), authHandle, areaInArea);
  }

  const maxCountMask = compactWidthOr(
    0,
    compactWidth(msg.areaOfInterest.maxCount),
  );

  const shifted = maxCountMask << 2;

  const maxSizeMask = compactWidthOr(
    shifted,
    compactWidth(msg.areaOfInterest.maxSize),
  );

  const lengthBytes = maxSizeMask << 4;

  return concat(
    new Uint8Array([header]),
    authHandle,
    areaInArea,
    new Uint8Array([lengthBytes]),
    encodeCompactWidth(msg.areaOfInterest.maxCount),
    encodeCompactWidth(msg.areaOfInterest.maxSize),
  );
}

export function encodeSetupBindStaticToken<StaticToken>(
  msg: MsgSetupBindStaticToken<StaticToken>,
  encodeStaticToken: (token: StaticToken) => Uint8Array,
) {
  return concat(
    new Uint8Array([0x30]),
    encodeStaticToken(msg.staticToken),
  );
}
