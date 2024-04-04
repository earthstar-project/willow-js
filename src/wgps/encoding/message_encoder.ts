import { defaultEntry, Entry, FIFO } from "../../../deps.ts";
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
  MSG_DATA_SEND_ENTRY,
  MSG_DATA_SEND_PAYLOAD,
  MSG_DATA_SET_EAGERNESS,
  MSG_PAI_BIND_FRAGMENT,
  MSG_PAI_REPLY_FRAGMENT,
  MSG_PAI_REPLY_SUBSPACE_CAPABILITY,
  MSG_PAI_REQUEST_SUBSPACE_CAPABILITY,
  MSG_RECONCILIATION_ANNOUNCE_ENTRIES,
  MSG_RECONCILIATION_SEND_ENTRY,
  MSG_RECONCILIATION_SEND_FINGERPRINT,
  MSG_SETUP_BIND_AREA_OF_INTEREST,
  MSG_SETUP_BIND_READ_CAPABILITY,
  MSG_SETUP_BIND_STATIC_TOKEN,
  ReadCapPrivy,
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
  encodeReconciliationSendEntry,
  encodeReconciliationSendFingerprint,
} from "./reconciliation.ts";
import {
  ReconcileMsgTracker,
  ReconcileMsgTrackerOpts,
} from "../reconciliation/reconcile_msg_tracker.ts";
import {
  encodeDataSendEntry,
  encodeDataSendPayload,
  encodeDataSetEagerness,
} from "./data.ts";

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
  DynamicToken,
  NamespaceId,
  SubspaceId,
  PayloadDigest,
  AuthorisationOpts,
> {
  private messageChannel = new FIFO<EncodedSyncMessage>();

  private reconcileMsgTracker: ReconcileMsgTracker<
    Fingerprint,
    DynamicToken,
    NamespaceId,
    SubspaceId,
    PayloadDigest
  >;

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
      DynamicToken,
      NamespaceId,
      SubspaceId,
      PayloadDigest,
      AuthorisationOpts
    >,
    readonly opts: {
      getIntersectionPrivy: (
        handle: bigint,
      ) => ReadCapPrivy<NamespaceId, SubspaceId>;
      getCap: (handle: bigint) => ReadCapability;
      getCurrentlySentEntry: () => Entry<
        NamespaceId,
        SubspaceId,
        PayloadDigest
      >;
    } & ReconcileMsgTrackerOpts<NamespaceId, SubspaceId, PayloadDigest>,
  ) {
    this.reconcileMsgTracker = new ReconcileMsgTracker(opts);
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
      DynamicToken,
      NamespaceId,
      SubspaceId,
      PayloadDigest
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
            privy: this.reconcileMsgTracker.getPrivy(),
            encodeFingerprint: this.schemes.fingerprint.encoding.encode,
          },
        );

        push(LogicalChannel.ReconciliationChannel, bytes);

        this.reconcileMsgTracker.onSendFingerprint(message);

        break;
      }

      case MSG_RECONCILIATION_ANNOUNCE_ENTRIES: {
        const bytes = encodeReconciliationAnnounceEntries(message, {
          encodeSubspaceId: this.schemes.subspace.encode,
          orderSubspace: this.schemes.subspace.order,
          pathScheme: this.schemes.path,
          privy: this.reconcileMsgTracker.getPrivy(),
        });

        push(LogicalChannel.ReconciliationChannel, bytes);

        this.reconcileMsgTracker.onAnnounceEntries(message);

        break;
      }

      case MSG_RECONCILIATION_SEND_ENTRY: {
        const bytes = encodeReconciliationSendEntry(
          message,
          {
            privy: this.reconcileMsgTracker.getPrivy(),
            encodeDynamicToken:
              this.schemes.authorisationToken.encodings.dynamicToken.encode,
            encodeSubspaceId: this.schemes.subspace.encode,
            encodeNamespaceId: this.schemes.namespace.encode,
            encodePayloadDigest: this.schemes.payload.encode,
            orderSubspace: this.schemes.subspace.order,
            pathScheme: this.schemes.path,
            isEqualNamespace: this.schemes.namespace.isEqual,
          },
        );

        push(LogicalChannel.ReconciliationChannel, bytes);

        this.reconcileMsgTracker.onSendEntry(message);

        break;
      }

      // Data

      case MSG_DATA_SEND_ENTRY: {
        const bytes = encodeDataSendEntry(message, {
          encodeNamespaceId: this.schemes.namespace.encode,
          encodeDynamicToken:
            this.schemes.authorisationToken.encodings.dynamicToken.encode,
          encodeSubspaceId: this.schemes.subspace.encode,
          encodePayloadDigest: this.schemes.payload.encode,
          isEqualNamespace: this.schemes.namespace.isEqual,
          orderSubspace: this.schemes.subspace.order,
          pathScheme: this.schemes.path,
          currentlySentEntry: this.opts.getCurrentlySentEntry(),
        });

        push(LogicalChannel.DataChannel, bytes);
        break;
      }

      case MSG_DATA_SEND_PAYLOAD: {
        const bytes = encodeDataSendPayload(message);
        push(LogicalChannel.DataChannel, bytes);
        break;
      }

      case MSG_DATA_SET_EAGERNESS: {
        const bytes = encodeDataSetEagerness(message);
        push(LogicalChannel.DataChannel, bytes);
        break;
      }

      default:
        new WillowError(
          `Did not know how to encode a message: ${message}`,
        );
    }
  }
}
