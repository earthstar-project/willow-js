import { AuthorKeypair, NamespaceKeypair, SignedEntry } from "./types.ts";

function generateAuthorKeypair(shortname: string): AuthorKeypair;

function generateNamespaceKeypair(shortname: string): NamespaceKeypair;

function generateEntry(
  opts: {
    author: AuthorKeypair;
    namespace: NamespaceKeypair;
    path: Uint8Array;
    data: Uint8Array;
    timestamp?: Uint8Array;
  },
): SignedEntry;

function encodeEntry(
  signedEntry: SignedEntry,
): Uint8Array;

function decodeEntry(
  encodedEntry: Uint8Array,
): SignedEntry;

// Not going to even think about storing entries or merging them right now.
