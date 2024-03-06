import { FIFO } from "../../../deps.ts";
import {
  LogicalChannel,
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
  MSG_SETUP_BIND_READ_CAPABILITY,
  SyncEncodings,
  SyncMessage,
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
import { encodeSetupBindReadCapability } from "./setup.ts";

export type EncodedSyncMessage = {
  channel: LogicalChannel | null;
  message: Uint8Array;
};

export class MessageEncoder<
  ReadCapabilityPartial,
  SyncSignature,
  PsiGroup,
  SubspaceCapability,
  SyncSubspaceSignature,
> {
  private messageChannel = new FIFO<EncodedSyncMessage>();

  constructor(
    readonly encodings: SyncEncodings<
      ReadCapabilityPartial,
      SyncSignature,
      PsiGroup,
      SubspaceCapability,
      SyncSubspaceSignature
    >,
  ) {
  }

  async *[Symbol.asyncIterator]() {
    for await (const message of this.messageChannel) {
      yield message;
    }
  }

  encode(
    message: SyncMessage<
      ReadCapabilityPartial,
      SyncSignature,
      PsiGroup,
      SubspaceCapability,
      SyncSubspaceSignature
    >,
  ) {
    const push = (channel: LogicalChannel | null, message: Uint8Array) => {
      this.messageChannel.push({
        channel,
        message,
      });
    };

    switch (message.kind) {
      // Control messages
      case MSG_CONTROL_ISSUE_GUARANTEE: {
        const bytes = encodeControlIssueGuarantee(message);
        push(null, bytes);
        break;
      }
      case MSG_CONTROL_ABSOLVE: {
        const bytes = encodeControlAbsolve(message);
        push(null, bytes);
        break;
      }
      case MSG_CONTROL_PLEAD: {
        const bytes = encodeControlPlead(message);
        push(null, bytes);
        break;
      }
      case MSG_CONTROL_ANNOUNCE_DROPPING: {
        const bytes = encodeControlAnnounceDropping(message);
        push(null, bytes);
        break;
      }
      case MSG_CONTROL_APOLOGISE: {
        const bytes = encodeControlApologise(message);
        push(null, bytes);
        break;
      }
      case MSG_CONTROL_FREE: {
        const bytes = encodeControlFree(message);
        push(null, bytes);
        break;
      }

      // Commitment scheme and PAI
      case MSG_COMMITMENT_REVEAL: {
        const bytes = encodeCommitmentReveal(message);
        push(null, bytes);
        break;
      }
      case MSG_PAI_BIND_FRAGMENT: {
        const bytes = encodePaiBindFragment(
          message,
          this.encodings.groupMember.encode,
        );
        push(LogicalChannel.IntersectionChannel, bytes);
        break;
      }
      case MSG_PAI_REPLY_FRAGMENT: {
        const bytes = encodePaiReplyFragment(
          message,
          this.encodings.groupMember.encode,
        );
        push(LogicalChannel.IntersectionChannel, bytes);
        break;
      }
      case MSG_PAI_REQUEST_SUBSPACE_CAPABILITY: {
        const bytes = encodePaiRequestSubspaceCapability(message);
        push(LogicalChannel.IntersectionChannel, bytes);
        break;
      }
      case MSG_PAI_REPLY_SUBSPACE_CAPABILITY: {
        const bytes = encodePaiReplySubspaceCapability(
          message,
          this.encodings.subspaceCapability.encode,
          this.encodings.syncSubspaceSignature.encode,
        );
        push(LogicalChannel.IntersectionChannel, bytes);
        break;
      }

      // Setup
      case MSG_SETUP_BIND_READ_CAPABILITY: {
        const bytes = encodeSetupBindReadCapability(
          message,
          this.encodings.readCapabilityPartial.encode,
          this.encodings.syncSignature.encode,
        );

        push(LogicalChannel.CapabilityChannel, bytes);
      }
    }
  }
}
