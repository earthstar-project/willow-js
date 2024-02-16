import { delay } from "https://deno.land/std@0.202.0/async/delay.ts";
import { decodeMessages } from "./decoding/decode_messages.ts";
import { MessageEncoder } from "./encoding/message_encoder.ts";
import { transportPairInMemory } from "./transports/in_memory.ts";
import { MSG_COMMITMENT_REVEAL, SyncMessage } from "./types.ts";
import { assertEquals } from "https://deno.land/std@0.202.0/assert/assert_equals.ts";

Deno.test("Encoding roundtrip test", async () => {
  const [alfie, betty] = transportPairInMemory();

  const msgEncoder = new MessageEncoder(alfie);

  const messages: SyncMessage[] = [];

  (async () => {
    for await (
      const message of decodeMessages({
        transport: betty,
        challengeLength: 4,
      })
    ) {
      messages.push(message);
    }
  })();

  const nonce = crypto.getRandomValues(new Uint8Array(4));

  msgEncoder.send({
    kind: MSG_COMMITMENT_REVEAL,
    nonce,
  });

  await delay(0);

  assertEquals(messages, [{
    kind: MSG_COMMITMENT_REVEAL,
    nonce,
  }]);
});
