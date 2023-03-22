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

export async function verifyEntry<PubKeyType>(
  opts: {
    signedEntry: SignedEntry;
    namespacePubkey: PubKeyType;
    authorPubkey: PubKeyType;
    verify: (
      publicKey: PubKeyType,
      signature: ArrayBuffer,
      signed: ArrayBuffer,
    ) => Promise<boolean>;
  },
): Promise<boolean> {
  const signedBytes = encodeEntry(opts.signedEntry.entry);

  const namespaceVerified = await opts.verify(
    opts.namespacePubkey,
    opts.signedEntry.namespaceSignature,
    signedBytes,
  );

  const authorVerified = await opts.verify(
    opts.authorPubkey,
    opts.signedEntry.authorSignature,
    signedBytes,
  );

  return namespaceVerified && authorVerified;
}
