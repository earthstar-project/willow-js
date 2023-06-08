import { encodeEntry } from "../encode_decode.ts";
import { Entry, SignedEntry } from "../types.ts";
import { SignFn, VerifyFn } from "./types.ts";

export async function signEntry<KeypairType>(
  opts: {
    entry: Entry;
    namespaceKeypair: KeypairType;
    authorKeypair: KeypairType;
    sign: SignFn<KeypairType>;
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
    verify: VerifyFn;
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
