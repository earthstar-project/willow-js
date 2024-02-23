import { SyncMessage, Transport } from "../types.ts";
import { decodeCommitmentReveal } from "./commitment_reveal.ts";
import { GrowingBytes } from "./growing_bytes.ts";

export type DecodeMessagesOpts = {
  transport: Transport;
  challengeLength: number;
};

export async function* decodeMessages(
  opts: DecodeMessagesOpts,
): AsyncIterable<SyncMessage> {
  const growingBytes = new GrowingBytes(opts.transport);

  // TODO: Not while true, but while transport is open.
  while (true) {
    await growingBytes.nextAbsolute(1);

    // Find out the type of decoder to use by bitmasking the first byte of the message.
    const [firstByte] = growingBytes.array;

    if (firstByte === 0) {
      yield await decodeCommitmentReveal(
        growingBytes,
        opts.challengeLength,
      );
    }
  }
}
