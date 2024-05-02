import { Area, Entry, GrowingBytes } from "../../../deps.ts";
import {
  MsgKind,
  MsgReconciliationTerminatePayload,
  ReadCapPrivy,
  SyncMessage,
  SyncSchemes,
  Transport,
} from "../types.ts";
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
import {
  decodeSetupBindAreaOfInterest,
  decodeSetupBindReadCapability,
  decodeSetupBindStaticToken,
} from "./setup.ts";
import {
  decodeReconciliationAnnounceEntries,
  decodeReconciliationSendEntry,
  decodeReconciliationSendFingerprint,
  decodeReconciliationSendPayload,
} from "./reconciliation.ts";
import {
  ReconcileMsgTracker,
  ReconcileMsgTrackerOpts,
} from "../reconciliation/reconcile_msg_tracker.ts";
import {
  decodeDataBindPayloadRequest,
  decodeDataReplyPayload,
  decodeDataSendEntry,
  decodeDataSendPayload,
  decodeDataSetEagerness,
} from "./data.ts";
import { WgpsMessageValidationError } from "../../errors.ts";

export type DecodeMessagesOpts<
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
  Prefingerprint,
  Fingerprint,
  AuthorisationToken,
  StaticToken,
  DynamicToken,
  NamespaceId,
  SubspaceId,
  PayloadDigest,
  AuthorisationOpts,
> = {
  schemes: SyncSchemes<
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
    Prefingerprint,
    Fingerprint,
    AuthorisationToken,
    StaticToken,
    DynamicToken,
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    AuthorisationOpts
  >;
  transport: Transport;
  challengeLength: number;
  getIntersectionPrivy: (
    handle: bigint,
  ) => ReadCapPrivy<NamespaceId, SubspaceId>;
  getTheirCap: (handle: bigint) => Promise<ReadCapability>;
  getCurrentlyReceivedEntry: () => Entry<
    NamespaceId,
    SubspaceId,
    PayloadDigest
  >;
  aoiHandlesToNamespace: (
    senderHandle: bigint,
    receivedHandle: bigint,
  ) => NamespaceId;
  aoiHandlesToArea: (
    senderHandle: bigint,
    receivedHandle: bigint,
  ) => Area<SubspaceId>;
} & ReconcileMsgTrackerOpts<NamespaceId, SubspaceId, PayloadDigest>;

export async function* decodeMessages<
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
  Prefingerprint,
  Fingerprint,
  AuthorisationToken,
  StaticToken,
  DynamicToken,
  NamespaceId,
  SubspaceId,
  PayloadDigest,
  AuthorisationOpts,
>(
  opts: DecodeMessagesOpts<
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
    Prefingerprint,
    Fingerprint,
    AuthorisationToken,
    StaticToken,
    DynamicToken,
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    AuthorisationOpts
  >,
): AsyncIterable<
  SyncMessage<
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
  >
> {
  const reconcileMsgTracker = new ReconcileMsgTracker(opts);

  const bytes = new GrowingBytes(opts.transport);

  while (!opts.transport.isClosed) {
    await bytes.nextAbsolute(1);

    // Find out the type of decoder to use by bitmasking the first byte of the message.
    const [firstByte] = bytes.array;

    if (firstByte === 0x0) {
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
    } else if ((firstByte & 0x70) === 0x70) {
      // Data Reply Payload
      yield await decodeDataReplyPayload(bytes);
    } else if ((firstByte & 0x6c) === 0x6c) {
      // Data Bind Payload request
      yield await decodeDataBindPayloadRequest(bytes, {
        decodeNamespaceId: opts.schemes.namespace.decodeStream,
        decodePayloadDigest: opts.schemes.payload.decodeStream,
        decodeSubspaceId: opts.schemes.subspace.decodeStream,
        pathScheme: opts.schemes.path,
        getCurrentlyReceivedEntry: () => opts.getCurrentlyReceivedEntry(),
        aoiHandlesToNamespace: opts.aoiHandlesToNamespace,
        aoiHandlesToArea: opts.aoiHandlesToArea,
      });
    } else if ((firstByte & 0x68) === 0x68) {
      // Data Set Eagerness
      yield await decodeDataSetEagerness(bytes);
    } else if ((firstByte & 0x64) === 0x64) {
      // Data Send payload
      yield await decodeDataSendPayload(bytes);
    } else if ((firstByte & 0x60) === 0x60) {
      // Data send entry
      yield await decodeDataSendEntry(bytes, {
        decodeNamespaceId: opts.schemes.namespace.decodeStream,
        decodeDynamicToken:
          opts.schemes.authorisationToken.encodings.dynamicToken.decodeStream,
        decodePayloadDigest: opts.schemes.payload.decodeStream,
        decodeSubspaceId: opts.schemes.subspace.decodeStream,
        pathScheme: opts.schemes.path,
        currentlyReceivedEntry: opts.getCurrentlyReceivedEntry(),
        aoiHandlesToNamespace: opts.aoiHandlesToNamespace,
        aoiHandlesToArea: opts.aoiHandlesToArea,
      });
    } else if ((firstByte & 0x50) === 0x50) {
      // ReconciliationAnnounceEntries
      // OR ReconciliationSendEntry

      // OR ReconciliationSendPayload
      // OR ReconciliationTerminatePayload.

      // All depends on what we're expecting.

      if (reconcileMsgTracker.isExpectingPayloadOrTermination()) {
        if ((firstByte & 0x58) === 0x58) {
          reconcileMsgTracker.onTerminatePayload();
          // It's a terminate message.

          bytes.prune(1);
          yield {
            kind: MsgKind.ReconciliationTerminatePayload,
          };
        } else {
          yield await decodeReconciliationSendPayload(bytes);
        }
      } else if (reconcileMsgTracker.isExpectingReconciliationSendEntry()) {
        const message = await decodeReconciliationSendEntry(
          bytes,
          {
            getPrivy: () => {
              return reconcileMsgTracker.getPrivy();
            },
            decodeDynamicToken:
              opts.schemes.authorisationToken.encodings.dynamicToken
                .decodeStream,
            decodeNamespaceId: opts.schemes.namespace.decodeStream,
            decodePayloadDigest: opts.schemes.payload.decodeStream,
            decodeSubspaceId: opts.schemes.subspace.decodeStream,
            pathScheme: opts.schemes.path,
          },
        );

        reconcileMsgTracker.onSendEntry(message);

        yield message;
      } else {
        const message = await decodeReconciliationAnnounceEntries(
          bytes,
          {
            decodeSubspaceId: opts.schemes.subspace.decodeStream,
            pathScheme: opts.schemes.path,
            getPrivy: () => {
              return reconcileMsgTracker.getPrivy();
            },
            aoiHandlesToRange3d: opts.aoiHandlesToRange3d,
          },
        );

        reconcileMsgTracker.onAnnounceEntries(message);

        yield message;
      }
    } else if ((firstByte & 0x40) === 0x40) {
      // Reconciliation send fingerprint
      const message = await decodeReconciliationSendFingerprint(
        bytes,
        {
          decodeFingerprint: opts.schemes.fingerprint.encoding.decodeStream,
          decodeSubspaceId: opts.schemes.subspace.decodeStream,
          neutralFingerprint: opts.schemes.fingerprint.neutralFinalised,
          pathScheme: opts.schemes.path,
          getPrivy: () => {
            return reconcileMsgTracker.getPrivy();
          },
          aoiHandlesToRange3d: opts.aoiHandlesToRange3d,
        },
      );

      reconcileMsgTracker.onSendFingerprint(message);

      yield message;
    } else if ((firstByte & 0x30) === 0x30) {
      // Setup Bind Static Token
      yield await decodeSetupBindStaticToken(
        bytes,
        opts.schemes.authorisationToken.encodings.staticToken.decodeStream,
      );
    } else if ((firstByte & 0x28) === 0x28) {
      // Setup Bind Area of Interest

      yield await decodeSetupBindAreaOfInterest(
        bytes,
        async (authHandle) => {
          const cap = await opts.getTheirCap(authHandle);
          return opts.schemes.accessControl.getGrantedArea(cap);
        },
        opts.schemes.subspace.decodeStream,
        opts.schemes.path,
      );
    } else if ((firstByte & 0x20) === 0x20) {
      // Setup Bind Read Capability
      yield await decodeSetupBindReadCapability(
        bytes,
        opts.schemes.accessControl.encodings.readCapability,
        opts.getIntersectionPrivy,
        opts.schemes.accessControl.encodings.syncSignature.decodeStream,
      );
    } else if ((firstByte & 0x10) === 0x10) {
      // PAI Reply Subspace Capability
      yield await decodePaiReplySubspaceCapability(
        bytes,
        opts.schemes.subspaceCap.encodings.subspaceCapability.decodeStream,
        opts.schemes.subspaceCap.encodings.syncSubspaceSignature.decodeStream,
      );
    } else if ((firstByte & 0xc) === 0xc) {
      // PAI Request Subspace Capability
      yield await decodePaiRequestSubspaceCapability(bytes);
    } else if ((firstByte & 0x8) === 0x8) {
      // PAI Reply Fragment
      yield await decodePaiReplyFragment(
        bytes,
        opts.schemes.pai.groupMemberEncoding.decodeStream,
      );
    } else if ((firstByte & 0x4) === 0x4) {
      // PAI Bind Fragment
      yield await decodePaiBindFragment(
        bytes,
        opts.schemes.pai.groupMemberEncoding.decodeStream,
      );
    } else {
      // Couldn't decode.
      throw new WgpsMessageValidationError("Could not decode!");
    }
  }
}
