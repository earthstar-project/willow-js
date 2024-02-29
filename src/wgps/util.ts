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

export function onAsyncIterate<ValueType>(
  iterator: AsyncIterable<ValueType>,
  callback: (value: ValueType) => void | Promise<void>,
) {
  (async () => {
    for await (const value of iterator) {
      await callback(value);
    }
  })();
}
