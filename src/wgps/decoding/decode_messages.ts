import {
  type ReadCapPrivy,
  type SyncMessage,
  type SyncSchemes,
  type Transport,
} from "../types.ts";
import { decodeCommitmentReveal } from "./commitment_reveal.ts";
import {
  decodeControlAbsolve,
  decodeControlAnnounceDropping,
  decodeControlApologise,
  decodeControlFree,
  decodeControlIssueGuarantee,
  decodeControlLimitReceiving,
  decodeControlLimitSending,
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
  decodeReconciliationTerminatePayload,
} from "./reconciliation.ts";
import {
  ReconcileMsgTracker,
  type ReconcileMsgTrackerOpts,
} from "../reconciliation/reconcile_msg_tracker.ts";
import {
  decodeDataBindPayloadRequest,
  decodeDataReplyPayload,
  decodeDataSendEntry,
  decodeDataSendPayload,
  decodeDataSetEagerness,
} from "./data.ts";
import { WgpsMessageValidationError } from "../../errors.ts";
import { type Area, type Entry, GrowingBytes } from "@earthstar/willow-utils";

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
    yield await decodeNext();
  }

  // outlined function for better control flow
  async function decodeNext() {
    await bytes.nextAbsolute(1);

    // Find out the type of decoder to use by bitmasking the first byte of the message.
    const [firstByte] = bytes.array;
    const category = firstByte >> 5;

    switch (category) {
      case 0b000: // Commitment Scheme and Private Area Intersection
        switch (decodeKind(firstByte, 3)) {
          case 0b000: // Commitment Reveal
            return await decodeCommitmentReveal(
              bytes,
              opts.challengeLength,
            );
          case 0b001: // PAI Bind Fragment
            return await decodePaiBindFragment(
              bytes,
              opts.schemes.pai.groupMemberEncoding.decodeStream,
            );
          case 0b010: // PAI Reply Fragment
            return await decodePaiReplyFragment(
              bytes,
              opts.schemes.pai.groupMemberEncoding.decodeStream,
            );
          case 0b011: // PAI Request Subspace Capability
            return await decodePaiRequestSubspaceCapability(bytes);
          case 0b100: // PAI Reply Subspace Capability
            return await decodePaiReplySubspaceCapability(
              bytes,
              opts.schemes.subspaceCap.encodings.subspaceCapability.decodeStream,
              opts.schemes.subspaceCap.encodings.syncSubspaceSignature.decodeStream,
            );
        }
        throw new WgpsMessageValidationError("Could not decode!");

      case 0b001: // Setup
        switch (decodeKind(firstByte, 2)) {
          case 0b00: // Setup Bind Read Capability
            return await decodeSetupBindReadCapability(
              bytes,
              opts.schemes.accessControl.encodings.readCapability,
              opts.getIntersectionPrivy,
              opts.schemes.accessControl.encodings.syncSignature.decodeStream,
            );
          case 0b01: // Setup Bind Area of Interest
            return await decodeSetupBindAreaOfInterest(
              bytes,
              async (authHandle) => {
                const cap = await opts.getTheirCap(authHandle);
                return opts.schemes.accessControl.getGrantedArea(cap);
              },
              opts.schemes.subspace.decodeStream,
              opts.schemes.path,
            );
          case 0b10: // Setup Bind Static Token
            return await decodeSetupBindStaticToken(
              bytes,
              opts.schemes.authorisationToken.encodings.staticToken.decodeStream,
            );
        }
        throw new WgpsMessageValidationError("Could not decode!");

      case 0b010:  // Reconciliation
        switch (decodeKind(firstByte, 1)) {
          case 0: {
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

            return message;
          }
          case 1: {
            // ReconciliationAnnounceEntries
            // OR ReconciliationSendEntry

            // OR ReconciliationSendPayload
            // OR ReconciliationTerminatePayload.

            // All depends on what we're expecting.

            if (reconcileMsgTracker.isExpectingPayloadOrTermination()) {
              if ((firstByte & 0x58) === 0x58) {
                const message = await decodeReconciliationTerminatePayload(bytes);
                reconcileMsgTracker.onTerminatePayload(message);
                return message;
              } else {
                return await decodeReconciliationSendPayload(bytes);
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

              return message;
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

              return message;
            }
          }
        }
        throw new WgpsMessageValidationError("Could not decode!");

      case 0b011: // Data
        switch (decodeKind(firstByte, 3)) {
          case 0b000:// Data send entry
            return await decodeDataSendEntry(bytes, {
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
          case 0b001: // Data Send payload
            return await decodeDataSendPayload(bytes);
          case 0b010: // Data Set Metadata
            return await decodeDataSetEagerness(bytes);
          case 0b011: // Data Bind Payload request
            return await decodeDataBindPayloadRequest(bytes, {
              decodeNamespaceId: opts.schemes.namespace.decodeStream,
              decodePayloadDigest: opts.schemes.payload.decodeStream,
              decodeSubspaceId: opts.schemes.subspace.decodeStream,
              pathScheme: opts.schemes.path,
              getCurrentlyReceivedEntry: () => opts.getCurrentlyReceivedEntry(),
              aoiHandlesToNamespace: opts.aoiHandlesToNamespace,
              aoiHandlesToArea: opts.aoiHandlesToArea,
            });
          case 0b100: // Data Reply Payload
            return await decodeDataReplyPayload(bytes);
        }
        throw new WgpsMessageValidationError("Could not decode!");

      case 0b100: // Control
        if (decodeKind(firstByte, 4) == 0b0000) {
          // Control Issue Guarantee.
          return await decodeControlIssueGuarantee(bytes);
        } else if (decodeKind(firstByte, 4) == 0b0001) {
          // Control Absolve
          return await decodeControlAbsolve(bytes);
        } else if (decodeKind(firstByte, 4) == 0b0010) {
          // Control plead
          return await decodeControlPlead(bytes);
        } else if (decodeKind(firstByte, 5) == 0b00110) {
          // Constrol limit sending
          return await decodeControlLimitSending(bytes);
        } else if (decodeKind(firstByte, 5) == 0b00111) {
          // Constrol limit receiving
          return await decodeControlLimitReceiving(bytes);
        } else if (decodeKind(firstByte, 2) == 0b10) {
          // Control announce dropping
          return await decodeControlAnnounceDropping(bytes);
        } else if (decodeKind(firstByte, 2) == 0b11) {
          // Control Apologise
          return await decodeControlApologise(bytes);
        } else if (decodeKind(firstByte, 2) == 0b01) {
          // Control free
          return await decodeControlFree(bytes);
        }
        throw new WgpsMessageValidationError("Could not decode!");
    }

    throw new WgpsMessageValidationError("Could not decode!");
  }
}


function decodeKind(byte: number, length: number): number {
  return (byte >> (5 - length)) & ((1 << length) - 1);
}
