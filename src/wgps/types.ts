import {
  Area,
  AreaOfInterest,
  EncodingScheme,
  Entry,
  PathScheme,
  PrivyEncodingScheme,
  Range3d,
  SignatureScheme,
} from "../../deps.ts";
import {
  AuthorisationScheme,
  FingerprintScheme,
  LengthyEntry,
  NamespaceScheme,
  PayloadScheme,
  SubspaceScheme,
} from "../store/types.ts";
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
  /** Close the connection with the other peer. */
  close(): void;
  readonly isClosed: boolean;
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
  /** Resource handle that controls the matching from Payload transmissions to Payload requests. */
  PayloadRequestHandle,
  /** Resource handle for StaticTokens that peers need to transmit. */
  StaticTokenHandle,
}

// Channels

export enum LogicalChannel {
  /** Logical channel for performing 3d range-based set reconciliation. */
  ReconciliationChannel,
  /** Logical channel for transmitting Entries and Payloads outside of 3d range-based set reconciliation. */
  DataChannel,
  /** Logical channel for controlling the binding of new IntersectionHandles. */
  IntersectionChannel,
  /** Logical channel for controlling the binding of new CapabilityHandles. */
  CapabilityChannel,
  /** Logical channel for controlling the binding of new AreaOfInterestHandles. */
  AreaOfInterestChannel,
  /** Logical channel for controlling the binding of new PayloadRequestHandles. */
  PayloadRequestChannel,
  /** Logical channel for controlling the binding of new StaticTokenHandles. */
  StaticTokenChannel,
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

export const MSG_SETUP_BIND_STATIC_TOKEN = Symbol(
  "msg_setup_static_token",
);
export type MsgSetupBindStaticToken<StaticToken> = {
  kind: typeof MSG_SETUP_BIND_STATIC_TOKEN;
  staticToken: StaticToken;
};

export type SetupMessage<
  ReadCapability,
  SyncSignature,
  StaticToken,
  SubspaceId,
> =
  | MsgSetupBindReadCapability<ReadCapability, SyncSignature>
  | MsgSetupBindAreaOfInterest<SubspaceId>
  | MsgSetupBindStaticToken<StaticToken>;

export const MSG_RECONCILIATION_SEND_FINGERPRINT = Symbol(
  "msg_reconciliation_send_fingerprint",
);
/** Send a Fingerprint as part of 3d range-based set reconciliation. */
export type MsgReconciliationSendFingerprint<SubspaceId, Fingerprint> = {
  kind: typeof MSG_RECONCILIATION_SEND_FINGERPRINT;
  /** The 3dRange whose Fingerprint is transmitted. */
  range: Range3d<SubspaceId>;
  /** The Fingerprint of the range, that is, of all LengthyEntries the peer has in the range. */
  fingerprint: Fingerprint;
  /** An AreaOfInterestHandle, bound by the sender of this message, that fully contains the range. */
  senderHandle: bigint;
  /** An AreaOfInterestHandle, bound by the receiver of this message, that fully contains the range. */
  receiverHandle: bigint;
};

export const MSG_RECONCILIATION_ANNOUNCE_ENTRIES = Symbol(
  "msg_reconciliation_announce_entries",
);
/** Prepare transmission of the LengthyEntries a peer has in a 3dRange as part of 3d range-based set reconciliation. */
export type MsgReconciliationAnnounceEntries<SubspaceId> = {
  kind: typeof MSG_RECONCILIATION_ANNOUNCE_ENTRIES;
  /** The 3dRange whose LengthyEntries to transmit. */
  range: Range3d<SubspaceId>;
  /** The number of Entries the sender has in the range. */
  count: bigint;
  /** A boolean flag to indicate whether the sender wishes to receive a ReconciliationAnnounceEntries message for the same 3dRange in return. */
  wantResponse: boolean;
  /** Whether the sender promises to send the Entries in the range sorted from oldest to newest. */
  willSort: boolean;
  /** An AreaOfInterestHandle, bound by the sender of this message, that fully contains the range. */
  senderHandle: bigint;
  /** An AreaOfInterestHandle, bound by the receiver of this message, that fully contains the range. */
  receiverHandle: bigint;
};

export const MSG_RECONCILIATION_SEND_ENTRY = Symbol(
  "msg_reconciliation_send_entry",
);
export type MsgReconciliationSendEntry<
  DynamicToken,
  NamespaceId,
  SubspaceId,
  PayloadDigest,
> = {
  kind: typeof MSG_RECONCILIATION_SEND_ENTRY;
  /** The LengthyEntry itself. */
  entry: LengthyEntry<NamespaceId, SubspaceId, PayloadDigest>;
  /** A StaticTokenHandle, bound by the sender of this message, that is bound to the static part of the entry’s AuthorisationToken. */
  staticTokenHandle: bigint;
  /** The dynamic part of the entry’s AuthorisationToken. */
  dynamicToken: DynamicToken;
};

export type ReconciliationMessage<
  Fingerprint,
  DynamicToken,
  NamespaceId,
  SubspaceId,
  PayloadDigest,
> =
  | MsgReconciliationSendFingerprint<SubspaceId, Fingerprint>
  | MsgReconciliationAnnounceEntries<SubspaceId>
  | MsgReconciliationSendEntry<
    DynamicToken,
    NamespaceId,
    SubspaceId,
    PayloadDigest
  >;

export const MSG_DATA_SEND_ENTRY = Symbol("msg_data_send_entry");
/** Transmit an AuthorisedEntry to the other peer, and optionally prepare transmission of its Payload. */
export type MsgDataSendEntry<
  DynamicToken,
  NamespaceId,
  SubspaceId,
  PayloadDigest,
> = {
  kind: typeof MSG_DATA_SEND_ENTRY;
  /** The Entry to transmit. */
  entry: Entry<NamespaceId, SubspaceId, PayloadDigest>;
  /** A StaticTokenHandle bound to the StaticToken of the Entry to transmit. */
  staticTokenHandle: bigint;
  /** The DynamicToken of the Entry to transmit. */
  dynamicToken: DynamicToken;
  /** The offset in the Payload in bytes at which Payload transmission will begin. If this is equal to the Entry’s payload_length, the Payload will not be transmitted. */
  offset: bigint;
};

export const MSG_DATA_SEND_PAYLOAD = Symbol("msg_data_send_payload");
/** Transmit some Payload bytes. */
export type MsgDataSendPayload = {
  kind: typeof MSG_DATA_SEND_PAYLOAD;
  /** The number of transmitted bytes. */
  amount: bigint;
  /** amount many bytes, to be added to the Payload of the receiver’s currently_received_entry at offset currently_received_offset. */
  bytes: Uint8Array;
};

export const MSG_DATA_SET_EAGERNESS = Symbol("msg_data_set_eagerness");
/** Express a preference whether the other peer should eagerly forward Payloads in the intersection of two AreaOfInterests. */
export type MsgDataSetEagerness = {
  kind: typeof MSG_DATA_SET_EAGERNESS;
  /** Whether Payloads should be pushed. */
  isEager: boolean;
  /** An AreaOfInterestHandle, bound by the sender of this message. */
  senderHandle: bigint;
  /** An AreaOfInterestHandle, bound by the receiver of this message. */
  receiverHandle: bigint;
};

export const MSG_DATA_BIND_PAYLOAD_REQUEST = Symbol(
  "msg_data_bind_payload_request",
);
/** Bind an Entry to a PayloadRequestHandle and request transmission of its Payload from an offset. */
export type MsgDataBindPayloadRequest<NamespaceId, SubspaceId, PayloadDigest> =
  {
    kind: typeof MSG_DATA_BIND_PAYLOAD_REQUEST;
    /** The Entry to request. */
    entry: Entry<NamespaceId, SubspaceId, PayloadDigest>;
    /** The offset in the Payload starting from which the sender would like to receive the Payload bytes. */
    offset: bigint;
    /** A resource handle for a ReadCapability bound by the sender that grants them read access to the bound Entry. */
    capability: bigint;
  };

export const MSG_DATA_REPLY_PAYLOAD = Symbol("msg_data_reply_payload");

/** Set up the state for replying to a DataBindPayloadRequest message. */
export type MsgDataReplyPayload = {
  kind: typeof MSG_DATA_REPLY_PAYLOAD;
  /** The PayloadRequestHandle to which to reply. */
  handle: bigint;
};

export type DataMessage<
  DynamicToken,
  NamespaceId,
  SubspaceId,
  PayloadDigest,
> =
  | MsgDataSendEntry<
    DynamicToken,
    NamespaceId,
    SubspaceId,
    PayloadDigest
  >
  | MsgDataSendPayload
  | MsgDataSetEagerness
  | MsgDataBindPayloadRequest<NamespaceId, SubspaceId, PayloadDigest>
  | MsgDataReplyPayload;

export type SyncMessage<
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
  PayloadDigest,
> =
  | ControlMessage
  | IntersectionMessage<PsiGroup, SubspaceCapability, SyncSubspaceSignature>
  | SetupMessage<ReadCapability, SyncSignature, StaticToken, SubspaceId>
  | ReconciliationMessage<
    Fingerprint,
    DynamicToken,
    NamespaceId,
    SubspaceId,
    PayloadDigest
  >
  | DataMessage<DynamicToken, NamespaceId, SubspaceId, PayloadDigest>;

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

export type ReconciliationPrivy<NamespaceId, SubspaceId, PayloadDigest> = {
  prevSenderHandle: bigint;
  prevReceiverHandle: bigint;
  prevRange: Range3d<SubspaceId>;
  prevStaticTokenHandle: bigint;
  prevEntry: Entry<NamespaceId, SubspaceId, PayloadDigest>;
  announced: {
    range: Range3d<SubspaceId>;
    namespace: NamespaceId;
  };
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
  Fingerprint,
  AuthorisationToken,
  StaticToken,
  DynamicToken,
  NamespaceId,
  SubspaceId,
  PayloadDigest,
  AuthorisationOpts,
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
  authorisationToken: AuthorisationTokenScheme<
    AuthorisationToken,
    StaticToken,
    DynamicToken
  >;
  authorisation: AuthorisationScheme<
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    AuthorisationOpts,
    AuthorisationToken
  >;
  payload: PayloadScheme<PayloadDigest>;
  fingerprint: FingerprintScheme<
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    Fingerprint
  >;
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
  getGrantedNamespace: (cap: ReadCapability) => NamespaceId;
  signatures: SignatureScheme<Receiver, ReceiverSecretKey, SyncSignature>;
  isValidCap: (cap: ReadCapability) => Promise<boolean>;
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
  isValidCap: (cap: SubspaceCapability) => Promise<boolean>;
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

export type AuthorisationTokenScheme<
  AuthorisationToken,
  StaticToken,
  DynamicToken,
> = {
  recomposeAuthToken: (
    staticToken: StaticToken,
    dynamicToken: DynamicToken,
  ) => AuthorisationToken;
  decomposeAuthToken: (
    authToken: AuthorisationToken,
  ) => [StaticToken, DynamicToken];
  encodings: {
    staticToken: EncodingScheme<StaticToken>;
    dynamicToken: EncodingScheme<DynamicToken>;
  };
};
