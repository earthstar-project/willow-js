/** The peer which initiated the synchronisation session. */
export const IS_ALFIE = Symbol("alfie");
/** The peer which did not initiate the synchronisation session. */
export const IS_BETTY = Symbol("betty");

/** we refer to the peer that initiated the synchronisation session as Alfie, and the other peer as Betty. */
export type SyncRole = typeof IS_ALFIE | typeof IS_BETTY;

/** A transport for receiving and sending data to with another peer */
export interface Transport {
  /** Whether this transport comes from the initiating party (Alfie), or not (Betty). */
  role: SyncRole;
  /** Send bytes to the other peer using this transport. */
  send(bytes: Uint8Array): Promise<void>;
  /** An async iterator of bytes received from the other peer via this transport. */
  [Symbol.asyncIterator](): AsyncIterator<Uint8Array>;
}

// Message types

export const MSG_COMMITMENT_REVEAL = Symbol("msg_commitment_reveal");
/** Complete the commitment scheme to determine the challenge for read authentication. */
export type MsgCommitmentReveal = {
  kind: typeof MSG_COMMITMENT_REVEAL;
  /** The nonce of the sender, encoded as a big-endian unsigned integer. */
  nonce: Uint8Array;
};

// Message groups

export type ControlMessage = MsgCommitmentReveal;

export type SyncMessage = ControlMessage;
