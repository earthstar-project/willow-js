import { FIFO } from "../../../deps.ts";
import {
  MSG_COMMITMENT_REVEAL,
  MSG_CONTROL_ABSOLVE,
  MSG_CONTROL_ANNOUNCE_DROPPING,
  MSG_CONTROL_APOLOGISE,
  MSG_CONTROL_FREE,
  MSG_CONTROL_ISSUE_GUARANTEE,
  MSG_CONTROL_PLEAD,
  MSG_PAI_BIND_FRAGMENT,
  MSG_PAI_REPLY_FRAGMENT,
  MSG_PAI_REPLY_SUBSPACE_CAPABILITY,
  MSG_PAI_REQUEST_SUBSPACE_CAPABILITY,
  SyncEncodings,
  SyncMessage,
  Transport,
} from "../types.ts";

import {
  encodeControlAbsolve,
  encodeControlAnnounceDropping,
  encodeControlApologise,
  encodeControlFree,
  encodeControlIssueGuarantee,
  encodeControlPlead,
} from "./control.ts";
import {
  encodeCommitmentReveal,
  encodePaiBindFragment,
  encodePaiReplyFragment,
  encodePaiReplySubspaceCapability,
  encodePaiRequestSubspaceCapability,
} from "./pai.ts";

export class MessageEncoder<
  PsiGroup,
  SubspaceCapability,
  SyncSubspaceSignature,
> {
  private outgoing = new FIFO<Uint8Array>();

  constructor(
    transport: Transport,
    readonly encodings: SyncEncodings<
      PsiGroup,
      SubspaceCapability,
      SyncSubspaceSignature
    >,
  ) {
    (async () => {
      for await (const bytes of this.outgoing) {
        await transport.send(bytes);
      }
    })();
  }

  send(
    message: SyncMessage<PsiGroup, SubspaceCapability, SyncSubspaceSignature>,
  ) {
    let bytes: Uint8Array;

    switch (message.kind) {
      // Control messages
      case MSG_CONTROL_ISSUE_GUARANTEE: {
        bytes = encodeControlIssueGuarantee(message);
        break;
      }
      case MSG_CONTROL_ABSOLVE: {
        bytes = encodeControlAbsolve(message);
        break;
      }
      case MSG_CONTROL_PLEAD: {
        bytes = encodeControlPlead(message);
        break;
      }
      case MSG_CONTROL_ANNOUNCE_DROPPING: {
        bytes = encodeControlAnnounceDropping(message);
        break;
      }
      case MSG_CONTROL_APOLOGISE: {
        bytes = encodeControlApologise(message);
        break;
      }
      case MSG_CONTROL_FREE: {
        bytes = encodeControlFree(message);
        break;
      }

      // Commitment scheme and PAI
      case MSG_COMMITMENT_REVEAL: {
        bytes = encodeCommitmentReveal(message);
        break;
      }
      case MSG_PAI_BIND_FRAGMENT: {
        bytes = encodePaiBindFragment(
          message,
          this.encodings.groupMember.encode,
        );
        break;
      }
      case MSG_PAI_REPLY_FRAGMENT: {
        bytes = encodePaiReplyFragment(
          message,
          this.encodings.groupMember.encode,
        );
        break;
      }
      case MSG_PAI_REQUEST_SUBSPACE_CAPABILITY: {
        bytes = encodePaiRequestSubspaceCapability(message);
        break;
      }
      case MSG_PAI_REPLY_SUBSPACE_CAPABILITY: {
        bytes = encodePaiReplySubspaceCapability(
          message,
          this.encodings.subspaceCapability.encode,
          this.encodings.syncSubspaceSignature.encode,
        );
      }
    }

    this.outgoing.push(bytes);
  }
}
