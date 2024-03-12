import {
  Area,
  AreaOfInterest,
  EncodingScheme,
  PathScheme,
  PrivyEncodingScheme,
  SignatureScheme,
} from "../../deps.ts";
import { NamespaceScheme, SubspaceScheme } from "../store/types.ts";
import { PaiScheme } from "./pai/types.ts";

/** The peer which initiated the synchronisation session. */
export const IS_ALFIE = Symbol("alfie");
/** The peer which did not initiate the synchronisation session. */
export const IS_BETTY = Symbol("betty");

/** we refer to the peer that initiated the synchronisation session as Alfie, and the other peer as Betty. */
export type SyncRole = typeof IS_ALFIE | typeof IS_BETTY;

/** Represents an authorisation to read an area of data in a Namespace */
export type ReadAuthorisation<
  ReadCapability,
  SubspaceReadCapability,
> = {
  capability: ReadCapability;
} | {
  capability: ReadCapability;
  subspaceCapability: SubspaceReadCapability;
};

/** A transport for receiving and sending data to with another peer */
export interface Transport {
  /** Whether this transport comes from the initiating party (Alfie), or not (Betty). */
  role: SyncRole;
  /** Send bytes to the other peer using this transport. */
  send(bytes: Uint8Array): Promise<void>;
  /** An async iterator of bytes received from the other peer via this transport. */
  [Symbol.asyncIterator](): AsyncIterator<Uint8Array>;
}

// Handle types

export enum HandleType {
  /** Resource handle for the private set intersection part of private area intersection. More precisely, an IntersectionHandle stores a PsiGroup member together with one of two possible states:
   * - pending (waiting for the other peer to perform scalar multiplication),
   * - completed (both peers performed scalar multiplication). */
  IntersectionHandle,
  /** Logical channel for controlling the binding of new CapabilityHandles. */
  CapabilityHandle,
  /** Resource handle for AreaOfInterests that peers wish to sync. */
  AreaOfInterestHandle,
}

// Channels

export enum LogicalChannel {
  /** Logical channel for controlling the binding of new IntersectionHandles. */
  IntersectionChannel,
  /** Logical channel for controlling the binding of new CapabilityHandles. */
  CapabilityChannel,
  /** Logical channel for controlling the binding of new AreaOfInterestHandles. */
  AreaOfInterestChannel,
}

// Message types

// 1. Control messages

export const MSG_CONTROL_ISSUE_GUARANTEE = Symbol(
  "msg_control_issue_guarantee",
);
/** Make a binding promise of available buffer capacity to the other peer. */
export type MsgControlIssueGuarantee = {
  kind: typeof MSG_CONTROL_ISSUE_GUARANTEE;
  amount: bigint;
  channel: LogicalChannel;
};

export const MSG_CONTROL_ABSOLVE = Symbol("msg_control_absolve");
/** Allow the other peer to reduce its total buffer capacity by amount. */
export type MsgControlAbsolve = {
  kind: typeof MSG_CONTROL_ABSOLVE;
  amount: bigint;
  channel: LogicalChannel;
};

export const MSG_CONTROL_PLEAD = Symbol("msg_control_plead");
/** Ask the other peer to send an ControlAbsolve message such that the receiver remaining guarantees will be target. */
export type MsgControlPlead = {
  kind: typeof MSG_CONTROL_PLEAD;
  target: bigint;
  channel: LogicalChannel;
};

export const MSG_CONTROL_ANNOUNCE_DROPPING = Symbol(
  "msg_control_announce_dropping",
);
/** Ask the other peer to send an ControlAbsolve message such that the receiver remaining guarantees will be target. */
export type MsgControlAnnounceDropping = {
  kind: typeof MSG_CONTROL_ANNOUNCE_DROPPING;
  channel: LogicalChannel;
};

export const MSG_CONTROL_APOLOGISE = Symbol("msg_control_apologise");
/** Notify the other peer that it can stop dropping messages of this logical channel. */
export type MsgControlApologise = {
  kind: typeof MSG_CONTROL_APOLOGISE;
  channel: LogicalChannel;
};

export const MSG_CONTROL_FREE = Symbol("msg_control_free");
export type MsgControlFree = {
  kind: typeof MSG_CONTROL_FREE;
  handle: bigint;
  /** Indicates whether the peer sending this message is the one who created the handle (true) or not (false). */
  mine: boolean;
  handleType: HandleType;
};

export const MSG_COMMITMENT_REVEAL = Symbol("msg_commitment_reveal");
/** Complete the commitment scheme to determine the challenge for read authentication. */
export type MsgCommitmentReveal = {
  kind: typeof MSG_COMMITMENT_REVEAL;
  /** The nonce of the sender, encoded as a big-endian unsigned integer. */
  nonce: Uint8Array;
};

export type ControlMessage =
  | MsgControlIssueGuarantee
  | MsgControlAbsolve
  | MsgControlPlead
  | MsgControlAnnounceDropping
  | MsgControlApologise
  | MsgControlFree
  | MsgCommitmentReveal;

// 2. Intersection messages

export const MSG_PAI_BIND_FRAGMENT = Symbol("msg_pai_bind_fragment");
/** Bind data to an IntersectionHandle for performing private area intersection. */
export type MsgPaiBindFragment<PsiGroup> = {
  kind: typeof MSG_PAI_BIND_FRAGMENT;
  /** The result of first applying hash_into_group to some fragment for private area intersection and then performing scalar multiplication with scalar. */
  groupMember: PsiGroup;
  /** Set to true if the private set intersection item is a secondary fragment. */
  isSecondary: boolean;
};

export const MSG_PAI_REPLY_FRAGMENT = Symbol("msg_pai_reply_fragment");
/** Finalise private set intersection for a single item. */
export type MsgPaiReplyFragment<PsiGroup> = {
  kind: typeof MSG_PAI_REPLY_FRAGMENT;
  /** The IntersectionHandle of the PaiBindFragment message which this finalises. */
  handle: bigint;
  /** The result of performing scalar multiplication between the group_member of the message that this is replying to and scalar. */
  groupMember: PsiGroup;
};

export const MSG_PAI_REQUEST_SUBSPACE_CAPABILITY = Symbol(
  "msg_pai_request_subspace_capability",
);
/** Ask the receiver to send a SubspaceCapability. */
export type MsgPaiRequestSubspaceCapability = {
  kind: typeof MSG_PAI_REQUEST_SUBSPACE_CAPABILITY;
  /** The IntersectionHandle bound by the sender for the least-specific secondary fragment for whose NamespaceId to request the */
  handle: bigint;
};

export const MSG_PAI_REPLY_SUBSPACE_CAPABILITY = Symbol(
  "msg_pai_reply_subspace_capability",
);
/** Send a previously requested SubspaceCapability. */
export type MsgPaiReplySubspaceCapability<
  SubspaceCapability,
  SyncSubspaceSignature,
> = {
  kind: typeof MSG_PAI_REPLY_SUBSPACE_CAPABILITY;
  /** The handle of the PaiRequestSubspaceCapability message that this answers (hence, an IntersectionHandle bound by the receiver of this message). */
  handle: bigint;
  /** A SubspaceCapability whose granted namespace corresponds to the request this answers. */
  capability: SubspaceCapability;
  /** The SyncSubspaceSignature issued by the receiver of the capability over the sender’s challenge. */
  signature: SyncSubspaceSignature;
};

export type IntersectionMessage<
  PsiGroup,
  SubspaceCapability,
  SyncSubspaceSignature,
> =
  | MsgPaiBindFragment<PsiGroup>
  | MsgPaiReplyFragment<PsiGroup>
  | MsgPaiRequestSubspaceCapability
  | MsgPaiReplySubspaceCapability<SubspaceCapability, SyncSubspaceSignature>;

// Setup
export const MSG_SETUP_BIND_READ_CAPABILITY = Symbol(
  "msg_setup_bind_read_capability",
);
export type MsgSetupBindReadCapability<ReadCapability, SyncSignature> = {
  kind: typeof MSG_SETUP_BIND_READ_CAPABILITY;
  /** A ReadCapability that the peer wishes to reference in future messages. */
  capability: ReadCapability;
  /** The IntersectionHandle, bound by the sender, of the capability’s fragment with the longest Path in the intersection of the fragments. If both a primary and secondary such fragment exist, choose the primary one. */
  handle: bigint;
  /** The SyncSignature issued by the Receiver of the capability over the sender’s challenge. */
  signature: SyncSignature;
};

export const MSG_SETUP_BIND_AREA_OF_INTEREST = Symbol(
  "msg_setup_bind_area_of_interest",
);
export type MsgSetupBindAreaOfInterest<SubspaceId> = {
  kind: typeof MSG_SETUP_BIND_AREA_OF_INTEREST;
  /** An AreaOfInterest that the peer wishes to reference in future messages. */
  areaOfInterest: AreaOfInterest<SubspaceId>;
  /** A CapabilityHandle bound by the sender that grants access to all entries in the message’s area_of_interest. */
  authorisation: bigint;
};

export type SetupMessage<ReadCapability, SyncSignature, SubspaceId> =
  | MsgSetupBindReadCapability<ReadCapability, SyncSignature>
  | MsgSetupBindAreaOfInterest<SubspaceId>;

export type SyncMessage<
  ReadCapability,
  SyncSignature,
  PsiGroup,
  SubspaceCapability,
  SyncSubspaceSignature,
  SubspaceId,
> =
  | ControlMessage
  | IntersectionMessage<PsiGroup, SubspaceCapability, SyncSubspaceSignature>
  | SetupMessage<ReadCapability, SyncSignature, SubspaceId>;

// Encodings

export type ReadCapPrivy<NamespaceId, SubspaceId> = {
  outer: Area<SubspaceId>;
  namespace: NamespaceId;
};

export type ReadCapEncodingScheme<ReadCapability, NamespaceId, SubspaceId> =
  PrivyEncodingScheme<
    ReadCapability,
    ReadCapPrivy<NamespaceId, SubspaceId>
  >;

export type SyncEncodings<
  ReadCapability,
  SyncSignature,
  PsiGroup,
  SubspaceCapability,
  SyncSubspaceSignature,
  NamespaceId,
  SubspaceId,
> = {
  readCapability: ReadCapEncodingScheme<
    ReadCapability,
    NamespaceId,
    SubspaceId
  >;
  syncSignature: EncodingScheme<SyncSignature>;
  groupMember: EncodingScheme<PsiGroup>;
  subspaceCapability: EncodingScheme<SubspaceCapability>;
  syncSubspaceSignature: EncodingScheme<SyncSubspaceSignature>;
  subspace: EncodingScheme<SubspaceId>;
};

export type SyncSchemes<
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
  NamespaceId,
  SubspaceId,
> = {
  accessControl: AccessControlScheme<
    ReadCapability,
    Receiver,
    SyncSignature,
    ReceiverSecretKey,
    NamespaceId,
    SubspaceId
  >;
  subspaceCap: SubspaceCapScheme<
    SubspaceCapability,
    SubspaceReceiver,
    SyncSubspaceSignature,
    SubspaceSecretKey,
    NamespaceId
  >;
  pai: PaiScheme<
    ReadCapability,
    PsiGroup,
    PsiScalar,
    NamespaceId,
    SubspaceId
  >;
  namespace: NamespaceScheme<NamespaceId>;
  subspace: SubspaceScheme<SubspaceId>;
  path: PathScheme;
};

export type AccessControlScheme<
  ReadCapability,
  Receiver,
  SyncSignature,
  ReceiverSecretKey,
  NamespaceId,
  SubspaceId,
> = {
  getReceiver: (cap: ReadCapability) => Receiver;
  getSecretKey: (receiver: Receiver) => ReceiverSecretKey;
  getGrantedArea: (cap: ReadCapability) => Area<SubspaceId>;
  signatures: SignatureScheme<Receiver, ReceiverSecretKey, SyncSignature>;

  encodings: {
    readCapability: ReadCapEncodingScheme<
      ReadCapability,
      NamespaceId,
      SubspaceId
    >;
    syncSignature: EncodingScheme<SyncSignature>;
  };
};

export type SubspaceCapScheme<
  SubspaceCapability,
  SubspaceReceiver,
  SyncSubspaceSignature,
  SubspaceSecretKey,
  NamespaceId,
> = {
  getSecretKey: (receiver: SubspaceReceiver) => SubspaceSecretKey | undefined;
  getNamespace: (cap: SubspaceCapability) => NamespaceId;
  getReceiver: (cap: SubspaceCapability) => SubspaceReceiver;
  signatures: SignatureScheme<
    SubspaceReceiver,
    SubspaceSecretKey,
    SyncSubspaceSignature
  >;
  encodings: {
    subspaceCapability: EncodingScheme<SubspaceCapability>;
    syncSubspaceSignature: EncodingScheme<SyncSubspaceSignature>;
  };
};
