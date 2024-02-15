import { FIFO } from "../../../deps.ts";
import { MSG_COMMITMENT_REVEAL, SyncMessage, Transport } from "../types.ts";
import { encodeCommitmentReveal } from "./encode_commitment_reveal.ts";

export class MessageEncoder {
  private outgoing = new FIFO<Uint8Array>();

  constructor(transport: Transport) {
    (async () => {
      for await (const bytes of this.outgoing) {
        await transport.send(bytes);
      }
    })();
  }

  send(message: SyncMessage) {
    let bytes: Uint8Array;

    switch (message.kind) {
      case MSG_COMMITMENT_REVEAL: {
        bytes = encodeCommitmentReveal(message);
      }
    }

    this.outgoing.push(bytes);
  }
}
