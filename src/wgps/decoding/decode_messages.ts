import { GrowingBytes } from "../../../deps.ts";
import {
  ReadCapPrivy,
  ReconciliationPrivy,
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
  decodeReconciliationSendFingerprint,
} from "./reconciliation.ts";

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
  Fingerprint,
  AuthorisationToken,
  StaticToken,
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
    Fingerprint,
    AuthorisationToken,
    StaticToken,
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
  getReconciliationPrivy: () => ReconciliationPrivy<SubspaceId>;
  getCap: (handle: bigint) => Promise<ReadCapability>;
};

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
  Fingerprint,
  AuthorisationToken,
  StaticToken,
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
    Fingerprint,
    AuthorisationToken,
    StaticToken,
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
    SubspaceId
  >
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
    } else if ((firstByte & 0x50) === 0x50) {
      // Reconciliation Announce Entries
      yield await decodeReconciliationAnnounceEntries(
        bytes,
        {
          decodeSubspaceId: opts.schemes.subspace.decodeStream,
          pathScheme: opts.schemes.path,
          getPrivy: opts.getReconciliationPrivy,
        },
      );
    } else if ((firstByte & 0x40) === 0x40) {
      // Reconciliation send fingerprint
      yield await decodeReconciliationSendFingerprint(
        bytes,
        {
          decodeFingerprint: opts.schemes.fingerprint.encoding.decodeStream,
          decodeSubspaceId: opts.schemes.subspace.decodeStream,
          neutralFingerprint: opts.schemes.fingerprint.neutral,
          pathScheme: opts.schemes.path,
          getPrivy: opts.getReconciliationPrivy,
        },
      );
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
          const cap = await opts.getCap(authHandle);
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
      console.warn("Could not decode!");
      break;
    }
  }
}
