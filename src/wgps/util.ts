import { ReadAuthorisation } from "./types.ts";

export function isSubspaceReadAuthorisation<
  ReadCapability,
  SubspaceReadCapability,
  SyncSignature,
  SyncSubspaceSignature,
>(
  authorisation: ReadAuthorisation<
    ReadCapability,
    SubspaceReadCapability,
    SyncSignature,
    SyncSubspaceSignature
  >,
): authorisation is {
  capability: ReadCapability;
  subspaceCapability: SubspaceReadCapability;
  signature: SyncSignature;
  subspaceSignature: SyncSubspaceSignature;
} {
  if ("subspaceCapability" in authorisation) {
    return true;
  }

  return false;
}
