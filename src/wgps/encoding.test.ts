import { delay } from "https://deno.land/std@0.202.0/async/delay.ts";
import { decodeMessages } from "./decoding/decode_messages.ts";
import { MessageEncoder } from "./encoding/message_encoder.ts";
import { transportPairInMemory } from "./transports/in_memory.ts";
import {
  HandleType,
  LogicalChannel,
  MSG_COMMITMENT_REVEAL,
  MSG_CONTROL_ABSOLVE,
  MSG_CONTROL_ANNOUNCE_DROPPING,
  MSG_CONTROL_APOLOGISE,
  MSG_CONTROL_FREE,
  MSG_CONTROL_ISSUE_GUARANTEE,
  MSG_CONTROL_PLEAD,
  MSG_PAI_BIND_FRAGMENT,
  MSG_PAI_REPLY_FRAGMENT,
  MSG_PAI_REPLY_SUBSPACE_CAPABILITY,
  MSG_PAI_REQUEST_SUBSPACE_CAPABILITY,
  SyncEncodings,
  SyncMessage,
} from "./types.ts";
import { assertEquals } from "https://deno.land/std@0.202.0/assert/assert_equals.ts";
import { shuffle } from "https://deno.land/x/proc@0.21.9/mod3.ts";

const encodings: SyncEncodings<Uint8Array, Uint8Array, Uint8Array> = {
  groupMember: {
    encode: (group) => group,
    decode: async (bytes) => {
      await bytes.nextAbsolute(9);
      const group = bytes.array.slice(0, 9);
      bytes.prune(9);
      return group;
    },
  },
  subspaceCapability: {
    encode: (cap) => cap,
    decode: async (bytes) => {
      await bytes.nextAbsolute(8);
      const cap = bytes.array.slice(0, 8);
      bytes.prune(8);
      return cap;
    },
  },
  syncSubspaceSignature: {
    encode: (sig) => sig,
    decode: async (bytes) => {
      await bytes.nextAbsolute(7);
      const sig = bytes.array.slice(0, 7);
      bytes.prune(7);
      return sig;
    },
  },
};

const vectors: SyncMessage<Uint8Array, Uint8Array, Uint8Array>[] = [
  {
    kind: MSG_COMMITMENT_REVEAL,
    nonce: crypto.getRandomValues(new Uint8Array(4)),
  },
  {
    kind: MSG_CONTROL_ISSUE_GUARANTEE,
    channel: LogicalChannel.IntersectionChannel,
    amount: BigInt(1),
  },
  {
    kind: MSG_CONTROL_ISSUE_GUARANTEE,
    channel: LogicalChannel.IntersectionChannel,
    amount: BigInt(256),
  },
  {
    kind: MSG_CONTROL_ISSUE_GUARANTEE,
    channel: LogicalChannel.IntersectionChannel,
    amount: BigInt(65536),
  },
  {
    kind: MSG_CONTROL_ISSUE_GUARANTEE,
    channel: LogicalChannel.IntersectionChannel,
    amount: BigInt(2147483648),
  },
  {
    kind: MSG_CONTROL_ABSOLVE,
    channel: LogicalChannel.IntersectionChannel,
    amount: BigInt(1),
  },
  {
    kind: MSG_CONTROL_ABSOLVE,
    channel: LogicalChannel.IntersectionChannel,
    amount: BigInt(256),
  },
  {
    kind: MSG_CONTROL_ABSOLVE,
    channel: LogicalChannel.IntersectionChannel,
    amount: BigInt(65536),
  },

  {
    kind: MSG_CONTROL_ABSOLVE,
    channel: LogicalChannel.IntersectionChannel,
    amount: BigInt(2147483648),
  },
  {
    kind: MSG_CONTROL_PLEAD,
    channel: LogicalChannel.IntersectionChannel,
    target: BigInt(1),
  },
  {
    kind: MSG_CONTROL_PLEAD,
    channel: LogicalChannel.IntersectionChannel,
    target: BigInt(256),
  },
  {
    kind: MSG_CONTROL_PLEAD,
    channel: LogicalChannel.IntersectionChannel,
    target: BigInt(65536),
  },
  {
    kind: MSG_CONTROL_PLEAD,
    channel: LogicalChannel.IntersectionChannel,
    target: BigInt(2147483648),
  },
  {
    kind: MSG_CONTROL_ANNOUNCE_DROPPING,
    channel: LogicalChannel.IntersectionChannel,
  },
  {
    kind: MSG_CONTROL_APOLOGISE,
    channel: LogicalChannel.IntersectionChannel,
  },
  {
    kind: MSG_CONTROL_FREE,
    handle: BigInt(1),
    handleType: HandleType.IntersectionHandle,
    mine: true,
  },
  {
    kind: MSG_CONTROL_FREE,
    handle: BigInt(256),
    handleType: HandleType.IntersectionHandle,
    mine: false,
  },
  {
    kind: MSG_CONTROL_FREE,
    handle: BigInt(65536),
    handleType: HandleType.IntersectionHandle,
    mine: true,
  },
  {
    kind: MSG_CONTROL_FREE,
    handle: BigInt(2147483648),
    handleType: HandleType.IntersectionHandle,
    mine: false,
  },
  // PAI

  {
    kind: MSG_PAI_BIND_FRAGMENT,
    isSecondary: false,
    groupMember: crypto.getRandomValues(new Uint8Array(9)),
  },
  {
    kind: MSG_PAI_BIND_FRAGMENT,
    isSecondary: true,
    groupMember: crypto.getRandomValues(new Uint8Array(9)),
  },
  {
    kind: MSG_PAI_REPLY_FRAGMENT,
    groupMember: crypto.getRandomValues(new Uint8Array(9)),
    handle: BigInt(1),
  },
  {
    kind: MSG_PAI_REPLY_FRAGMENT,
    groupMember: crypto.getRandomValues(new Uint8Array(9)),
    handle: BigInt(256),
  },
  {
    kind: MSG_PAI_REPLY_FRAGMENT,
    groupMember: crypto.getRandomValues(new Uint8Array(9)),
    handle: BigInt(65536),
  },
  {
    kind: MSG_PAI_REPLY_FRAGMENT,
    groupMember: crypto.getRandomValues(new Uint8Array(9)),
    handle: BigInt(2147483648),
  },

  {
    kind: MSG_PAI_REQUEST_SUBSPACE_CAPABILITY,
    handle: BigInt(1),
  },
  {
    kind: MSG_PAI_REQUEST_SUBSPACE_CAPABILITY,
    handle: BigInt(256),
  },
  {
    kind: MSG_PAI_REQUEST_SUBSPACE_CAPABILITY,
    handle: BigInt(65536),
  },
  {
    kind: MSG_PAI_REQUEST_SUBSPACE_CAPABILITY,
    handle: BigInt(2147483648),
  },

  {
    kind: MSG_PAI_REPLY_SUBSPACE_CAPABILITY,
    handle: BigInt(1),
    capability: crypto.getRandomValues(new Uint8Array(8)),
    signature: crypto.getRandomValues(new Uint8Array(7)),
  },
  {
    kind: MSG_PAI_REPLY_SUBSPACE_CAPABILITY,
    handle: BigInt(256),
    capability: crypto.getRandomValues(new Uint8Array(8)),
    signature: crypto.getRandomValues(new Uint8Array(7)),
  },
  {
    kind: MSG_PAI_REPLY_SUBSPACE_CAPABILITY,
    handle: BigInt(65536),
    capability: crypto.getRandomValues(new Uint8Array(8)),
    signature: crypto.getRandomValues(new Uint8Array(7)),
  },
  {
    kind: MSG_PAI_REPLY_SUBSPACE_CAPABILITY,
    handle: BigInt(2147483648),
    capability: crypto.getRandomValues(new Uint8Array(8)),
    signature: crypto.getRandomValues(new Uint8Array(7)),
  },
];

Deno.test("Encoding roundtrip test", async () => {
  const [alfie, betty] = transportPairInMemory();

  const msgEncoder = new MessageEncoder(alfie, encodings);

  const messages: SyncMessage<Uint8Array, Uint8Array, Uint8Array>[] = [];

  (async () => {
    for await (
      const message of decodeMessages({
        transport: betty,
        challengeLength: 4,
        encodings,
      })
    ) {
      messages.push(message);
    }
  })();

  shuffle(vectors);

  for (const message of vectors) {
    msgEncoder.send(message);
  }

  await delay(15);

  assertEquals(messages, vectors);
});
