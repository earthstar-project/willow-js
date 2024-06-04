import { WillowError } from "../../errors.ts";
import {
  type LogicalChannel,
  MsgKind,
  type ReadCapPrivy,
  type SyncMessage,
  type SyncSchemes,
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
  encodeReconciliationSendPayload,
  encodeReconciliationTerminatePayload,
} from "./reconciliation.ts";
import {
  ReconcileMsgTracker,
  type ReconcileMsgTrackerOpts,
} from "../reconciliation/reconcile_msg_tracker.ts";
import {
  encodeDataBindPayloadRequest,
  encodeDataReplyPayload,
  encodeDataSendEntry,
  encodeDataSendPayload,
  encodeDataSetEagerness,
} from "./data.ts";
import { msgLogicalChannels } from "../channels.ts";
import type { Entry } from "@earthstar/willow-utils";
import { FIFO } from "@korkje/fifo";

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
  Prefingeprint,
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
      Prefingeprint,
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

    let bytes: Uint8Array;

    switch (message.kind) {
      // Control messages
      case MsgKind.ControlIssueGuarantee: {
        bytes = encodeControlIssueGuarantee(message);
        break;
      }
      case MsgKind.ControlAbsolve: {
        bytes = encodeControlAbsolve(message);
        break;
      }
      case MsgKind.ControlPlead: {
        bytes = encodeControlPlead(message);
        break;
      }
      case MsgKind.ControlAnnounceDropping: {
        bytes = encodeControlAnnounceDropping(message);
        break;
      }
      case MsgKind.ControlApologise: {
        bytes = encodeControlApologise(message);
        break;
      }
      case MsgKind.ControlFree: {
        bytes = encodeControlFree(message);
        break;
      }

      // Commitment scheme and PAI
      case MsgKind.CommitmentReveal: {
        bytes = encodeCommitmentReveal(message);
        break;
      }
      case MsgKind.PaiBindFragment: {
        bytes = encodePaiBindFragment(
          message,
          this.schemes.pai.groupMemberEncoding.encode,
        );
        break;
      }
      case MsgKind.PaiReplyFragment: {
        bytes = encodePaiReplyFragment(
          message,
          this.schemes.pai.groupMemberEncoding.encode,
        );
        break;
      }
      case MsgKind.PaiRequestSubspaceCapability: {
        bytes = encodePaiRequestSubspaceCapability(message);
        break;
      }
      case MsgKind.PaiReplySubspaceCapability: {
        bytes = encodePaiReplySubspaceCapability(
          message,
          this.schemes.subspaceCap.encodings.subspaceCapability.encode,
          this.schemes.subspaceCap.encodings.syncSubspaceSignature.encode,
        );
        break;
      }

      // Setup
      case MsgKind.SetupBindReadCapability: {
        const privy = this.opts.getIntersectionPrivy(message.handle);
        bytes = encodeSetupBindReadCapability(
          message,
          this.schemes.accessControl.encodings.readCapability.encode,
          this.schemes.accessControl.encodings.syncSignature.encode,
          privy,
        );
        break;
      }

      case MsgKind.SetupBindAreaOfInterest: {
        const cap = this.opts.getCap(message.authorisation);
        const outer = this.schemes.accessControl.getGrantedArea(cap);
        bytes = encodeSetupBindAreaOfInterest(
          message,
          {
            encodeSubspace: this.schemes.subspace.encode,
            orderSubspace: this.schemes.subspace.order,
            pathScheme: this.schemes.path,
            outer,
          },
        );
        break;
      }

      case MsgKind.SetupBindStaticToken: {
        bytes = encodeSetupBindStaticToken(
          message,
          this.schemes.authorisationToken.encodings.staticToken.encode,
        );
        break;
      }

      // Reconciliation

      case MsgKind.ReconciliationSendFingerprint: {
        bytes = encodeReconciliationSendFingerprint(
          message,
          {
            isFingerprintNeutral: (fp) => {
              return this.schemes.fingerprint.isEqual(
                fp,
                this.schemes.fingerprint.neutralFinalised,
              );
            },
            encodeSubspaceId: this.schemes.subspace.encode,
            orderSubspace: this.schemes.subspace.order,
            pathScheme: this.schemes.path,
            privy: this.reconcileMsgTracker.getPrivy(),
            encodeFingerprint: this.schemes.fingerprint.encoding.encode,
          },
        );
        this.reconcileMsgTracker.onSendFingerprint(message);
        break;
      }

      case MsgKind.ReconciliationAnnounceEntries: {
        bytes = encodeReconciliationAnnounceEntries(message, {
          encodeSubspaceId: this.schemes.subspace.encode,
          orderSubspace: this.schemes.subspace.order,
          pathScheme: this.schemes.path,
          privy: this.reconcileMsgTracker.getPrivy(),
        });
        this.reconcileMsgTracker.onAnnounceEntries(message);
        break;
      }

      case MsgKind.ReconciliationSendEntry: {
        bytes = encodeReconciliationSendEntry(
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
        this.reconcileMsgTracker.onSendEntry(message);
        break;
      }

      case MsgKind.ReconciliationSendPayload: {
        bytes = encodeReconciliationSendPayload(message);
        break;
      }

      case MsgKind.ReconciliationTerminatePayload: {
        bytes = encodeReconciliationTerminatePayload();
        break;
      }

      // Data

      case MsgKind.DataSendEntry: {
        bytes = encodeDataSendEntry(message, {
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
        break;
      }

      case MsgKind.DataSendPayload: {
        bytes = encodeDataSendPayload(message);
        break;
      }

      case MsgKind.DataSetMetadata: {
        bytes = encodeDataSetEagerness(message);
        break;
      }

      case MsgKind.DataBindPayloadRequest: {
        bytes = encodeDataBindPayloadRequest(message, {
          encodeNamespaceId: this.schemes.namespace.encode,
          encodeSubspaceId: this.schemes.subspace.encode,
          encodePayloadDigest: this.schemes.payload.encode,
          isEqualNamespace: this.schemes.namespace.isEqual,
          orderSubspace: this.schemes.subspace.order,
          pathScheme: this.schemes.path,
          currentlySentEntry: this.opts.getCurrentlySentEntry(),
        });
        break;
      }

      case MsgKind.DataReplyPayload: {
        bytes = encodeDataReplyPayload(message);
        break;
      }

      default:
        throw new WillowError(
          `Did not know how to encode a message: ${message}`,
        );
    }

    push(msgLogicalChannels[message.kind], bytes);
  }
}
