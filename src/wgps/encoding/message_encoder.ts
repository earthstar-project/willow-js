import { FIFO } from "../../../deps.ts";
import { WillowError } from "../../errors.ts";
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
  MSG_RECONCILIATION_ANNOUNCE_ENTRIES,
  MSG_RECONCILIATION_SEND_FINGERPRINT,
  MSG_SETUP_BIND_AREA_OF_INTEREST,
  MSG_SETUP_BIND_READ_CAPABILITY,
  MSG_SETUP_BIND_STATIC_TOKEN,
  ReadCapPrivy,
  ReconciliationPrivy,
  SyncMessage,
  SyncSchemes,
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
import {
  encodeSetupBindAreaOfInterest,
  encodeSetupBindReadCapability,
  encodeSetupBindStaticToken,
} from "./setup.ts";
import {
  encodeReconciliationAnnounceEntries,
  encodeReconciliationSendFingerprint,
} from "./reconciliation.ts";

export type EncodedSyncMessage = {
  channel: LogicalChannel | null;
  message: Uint8Array;
};

export class MessageEncoder<
  ReadCapability,
  Receiver,
  SyncSignature,
  ReceiverSecretKey,
  PsiGroup,
  PsiScalar,
  SubspaceCapability,
  SubspaceReceiver,
  SyncSubspaceSignature,
  SubspaceSecretKey,
  Fingerprint,
  AuthorisationToken,
  StaticToken,
  NamespaceId,
  SubspaceId,
  PayloadDigest,
  AuthorisationOpts,
> {
  private messageChannel = new FIFO<EncodedSyncMessage>();

  constructor(
    readonly schemes: SyncSchemes<
      ReadCapability,
      Receiver,
      SyncSignature,
      ReceiverSecretKey,
      PsiGroup,
      PsiScalar,
      SubspaceCapability,
      SubspaceReceiver,
      SyncSubspaceSignature,
      SubspaceSecretKey,
      Fingerprint,
      AuthorisationToken,
      StaticToken,
      NamespaceId,
      SubspaceId,
      PayloadDigest,
      AuthorisationOpts
    >,
    readonly opts: {
      getIntersectionPrivy: (
        handle: bigint,
      ) => ReadCapPrivy<NamespaceId, SubspaceId>;
      getReconciliationPrivy: () => ReconciliationPrivy<SubspaceId>;
      getCap: (handle: bigint) => ReadCapability;
    },
  ) {
  }

  async *[Symbol.asyncIterator]() {
    for await (const message of this.messageChannel) {
      yield message;
    }
  }

  encode(
    message: SyncMessage<
      ReadCapability,
      SyncSignature,
      PsiGroup,
      SubspaceCapability,
      SyncSubspaceSignature,
      Fingerprint,
      StaticToken,
      SubspaceId
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
          this.schemes.pai.groupMemberEncoding.encode,
        );
        push(LogicalChannel.IntersectionChannel, bytes);
        break;
      }
      case MSG_PAI_REPLY_FRAGMENT: {
        const bytes = encodePaiReplyFragment(
          message,
          this.schemes.pai.groupMemberEncoding.encode,
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
          this.schemes.subspaceCap.encodings.subspaceCapability.encode,
          this.schemes.subspaceCap.encodings.syncSubspaceSignature.encode,
        );
        push(LogicalChannel.IntersectionChannel, bytes);
        break;
      }

      // Setup
      case MSG_SETUP_BIND_READ_CAPABILITY: {
        const privy = this.opts.getIntersectionPrivy(message.handle);

        const bytes = encodeSetupBindReadCapability(
          message,
          this.schemes.accessControl.encodings.readCapability.encode,
          this.schemes.accessControl.encodings.syncSignature.encode,
          privy,
        );

        push(LogicalChannel.CapabilityChannel, bytes);
        break;
      }

      case MSG_SETUP_BIND_AREA_OF_INTEREST: {
        const cap = this.opts.getCap(message.authorisation);

        const outer = this.schemes.accessControl.getGrantedArea(cap);

        const bytes = encodeSetupBindAreaOfInterest(
          message,
          {
            encodeSubspace: this.schemes.subspace.encode,
            orderSubspace: this.schemes.subspace.order,
            pathScheme: this.schemes.path,
            outer,
          },
        );

        push(LogicalChannel.AreaOfInterestChannel, bytes);
        break;
      }

      case MSG_SETUP_BIND_STATIC_TOKEN: {
        const bytes = encodeSetupBindStaticToken(
          message,
          this.schemes.authorisationToken.encodings.staticToken.encode,
        );

        push(LogicalChannel.StaticTokenChannel, bytes);
        break;
      }

      // Reconciliation

      case MSG_RECONCILIATION_SEND_FINGERPRINT: {
        const bytes = encodeReconciliationSendFingerprint(
          message,
          {
            isFingerprintNeutral: (fp) => {
              return this.schemes.fingerprint.isEqual(
                fp,
                this.schemes.fingerprint.neutral,
              );
            },
            encodeSubspaceId: this.schemes.subspace.encode,
            orderSubspace: this.schemes.subspace.order,
            pathScheme: this.schemes.path,
            privy: this.opts.getReconciliationPrivy(),
            encodeFingerprint: this.schemes.fingerprint.encoding.encode,
          },
        );

        push(LogicalChannel.ReconciliationChannel, bytes);

        break;
      }

      case MSG_RECONCILIATION_ANNOUNCE_ENTRIES: {
        const bytes = encodeReconciliationAnnounceEntries(message, {
          encodeSubspaceId: this.schemes.subspace.encode,
          orderSubspace: this.schemes.subspace.order,
          pathScheme: this.schemes.path,
          privy: this.opts.getReconciliationPrivy(),
        });

        push(LogicalChannel.ReconciliationChannel, bytes);

        break;
      }

      default:
        new WillowError(
          `Did not know how to encode a message: ${message}`,
        );
    }
  }
}
