import { encodeEntry } from "./encode_decode.ts";
import { Entry, SignedEntry } from "./types.ts";

export async function signEntry<KeyType>(
  opts: {
    entry: Entry;
    namespaceKeypair: KeyType;
    authorKeypair: KeyType;
    sign: (
      key: KeyType,
      encodedEntry: ArrayBuffer,
    ) => Promise<ArrayBuffer>;
  },
): Promise<SignedEntry> {
  const encodedEntry = encodeEntry(opts.entry);
  const namespaceSignature = await opts.sign(
    opts.namespaceKeypair,
    encodedEntry,
  );

  const authorSignature = await opts.sign(
    opts.authorKeypair,
    encodedEntry,
  );

  return {
    entry: opts.entry,
    namespaceSignature,
    authorSignature,
  };
}

export async function verifyEntry(
  opts: {
    signedEntry: SignedEntry;
    verify: (
      publicKey: ArrayBuffer,
      signature: ArrayBuffer,
      signed: ArrayBuffer,
    ) => Promise<boolean>;
  },
): Promise<boolean> {
  const signedBytes = encodeEntry(opts.signedEntry.entry);

  const namespaceVerified = await opts.verify(
    opts.signedEntry.entry.identifier.namespace,
    opts.signedEntry.namespaceSignature,
    signedBytes,
  );

  const authorVerified = await opts.verify(
    opts.signedEntry.entry.identifier.author,
    opts.signedEntry.authorSignature,
    signedBytes,
  );

  return namespaceVerified && authorVerified;
}
