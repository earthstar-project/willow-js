import { GrowingBytes } from "../../../deps.ts";
import { SyncEncodings, SyncMessage, Transport } from "../types.ts";
import { decodeCommitmentReveal } from "./commitment_reveal.ts";
import {
  decodeControlAbsolve,
  decodeControlAnnounceDropping,
  decodeControlApologise,
  decodeControlFree,
  decodeControlIssueGuarantee,
  decodeControlPlead,
} from "./control.ts";
import {
  decodePaiBindFragment,
  decodePaiReplyFragment,
  decodePaiReplySubspaceCapability,
  decodePaiRequestSubspaceCapability,
} from "./pai.ts";

export type DecodeMessagesOpts<
  PsiGroup,
  SubspaceCapability,
  SyncSubspaceSignature,
> = {
  transport: Transport;
  challengeLength: number;
  encodings: SyncEncodings<PsiGroup, SubspaceCapability, SyncSubspaceSignature>;
};

export async function* decodeMessages<
  PsiGroup,
  SubspaceCapability,
  SyncSubspaceSignature,
>(
  opts: DecodeMessagesOpts<PsiGroup, SubspaceCapability, SyncSubspaceSignature>,
): AsyncIterable<
  SyncMessage<PsiGroup, SubspaceCapability, SyncSubspaceSignature>
> {
  const bytes = new GrowingBytes(opts.transport);

  // TODO: Not while true, but while transport is open.
  while (true) {
    await bytes.nextAbsolute(1);

    // Find out the type of decoder to use by bitmasking the first byte of the message.
    const [firstByte] = bytes.array;

    if (firstByte === 0) {
      yield await decodeCommitmentReveal(
        bytes,
        opts.challengeLength,
      );
    } else if ((firstByte & 0x98) === 0x98) {
      // Control apologise
      yield await decodeControlApologise(bytes);
    } else if ((firstByte & 0x90) === 0x90) {
      // Control announce dropping
      yield await decodeControlAnnounceDropping(bytes);
    } else if ((firstByte & 0x8c) === 0x8c) {
      // Control free
      yield await decodeControlFree(bytes);
    } else if ((firstByte & 0x88) === 0x88) {
      // Control plead
      yield await decodeControlPlead(bytes);
    } else if ((firstByte & 0x84) === 0x84) {
      // Control Absolve
      yield await decodeControlAbsolve(bytes);
    } else if ((firstByte & 0x80) === 0x80) {
      // Control Issue Guarantee.
      yield await decodeControlIssueGuarantee(bytes);
    } else if ((firstByte & 0x10) === 0x10) {
      // PAI Reply Subspace Capability
      yield await decodePaiReplySubspaceCapability(
        bytes,
        opts.encodings.subspaceCapability.decodeStream,
        opts.encodings.syncSubspaceSignature.decodeStream,
      );
    } else if ((firstByte & 0xc) === 0xc) {
      // PAI Request Subspace Capability
      yield await decodePaiRequestSubspaceCapability(bytes);
    } else if ((firstByte & 0x8) === 0x8) {
      // PAI Reply Fragment
      yield await decodePaiReplyFragment(
        bytes,
        opts.encodings.groupMember.decodeStream,
      );
    } else if ((firstByte & 0x4) === 0x4) {
      // PAI Bind Fragment
      yield await decodePaiBindFragment(
        bytes,
        opts.encodings.groupMember.decodeStream,
      );
    } else {
      // Couldn't decode.
      console.warn("Could not decode!");
      break;
    }
  }
}
