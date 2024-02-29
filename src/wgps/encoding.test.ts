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
  SyncMessage,
} from "./types.ts";
import { assertEquals } from "https://deno.land/std@0.202.0/assert/assert_equals.ts";
import { shuffle } from "https://deno.land/x/proc@0.21.9/mod3.ts";
import {
  TestNamespace,
  testSchemePai,
  testSchemeSubspaceCap,
  TestSubspace,
  TestSubspaceReadCap,
} from "../test/test_schemes.ts";
import { randomPath } from "../test/utils.ts";
import { onAsyncIterate } from "./util.ts";

const vectors: SyncMessage<Uint8Array, TestSubspaceReadCap, Uint8Array>[] = [
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
    groupMember: crypto.getRandomValues(new Uint8Array(32)),
  },
  {
    kind: MSG_PAI_BIND_FRAGMENT,
    isSecondary: true,
    groupMember: crypto.getRandomValues(new Uint8Array(32)),
  },

  {
    kind: MSG_PAI_REPLY_FRAGMENT,
    groupMember: crypto.getRandomValues(new Uint8Array(32)),
    handle: BigInt(1),
  },
  {
    kind: MSG_PAI_REPLY_FRAGMENT,
    groupMember: crypto.getRandomValues(new Uint8Array(32)),
    handle: BigInt(256),
  },
  {
    kind: MSG_PAI_REPLY_FRAGMENT,
    groupMember: crypto.getRandomValues(new Uint8Array(32)),
    handle: BigInt(65536),
  },
  {
    kind: MSG_PAI_REPLY_FRAGMENT,
    groupMember: crypto.getRandomValues(new Uint8Array(32)),
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
    capability: {
      namespace: TestNamespace.Family,
      receiver: TestSubspace.Alfie,
      path: randomPath(),
    },
    signature: crypto.getRandomValues(new Uint8Array(33)),
  },

  {
    kind: MSG_PAI_REPLY_SUBSPACE_CAPABILITY,
    handle: BigInt(256),
    capability: {
      namespace: TestNamespace.Family,
      receiver: TestSubspace.Alfie,
      path: randomPath(),
    },
    signature: crypto.getRandomValues(new Uint8Array(33)),
  },
  {
    kind: MSG_PAI_REPLY_SUBSPACE_CAPABILITY,
    handle: BigInt(65536),
    capability: {
      namespace: TestNamespace.Family,
      receiver: TestSubspace.Alfie,
      path: randomPath(),
    },
    signature: crypto.getRandomValues(new Uint8Array(33)),
  },
  {
    kind: MSG_PAI_REPLY_SUBSPACE_CAPABILITY,
    handle: BigInt(2147483648),
    capability: {
      namespace: TestNamespace.Family,
      receiver: TestSubspace.Alfie,
      path: randomPath(),
    },
    signature: crypto.getRandomValues(new Uint8Array(33)),
  },
];

Deno.test("Encoding roundtrip test", async () => {
  const [alfie, betty] = transportPairInMemory();

  const encodings = {
    subspaceCapability: testSchemeSubspaceCap.encodings.subspaceCapability,
    syncSubspaceSignature:
      testSchemeSubspaceCap.encodings.syncSubspaceSignature,
    groupMember: testSchemePai.groupMemberEncoding,
  };

  const msgEncoder = new MessageEncoder(encodings);

  const messages: SyncMessage<Uint8Array, TestSubspaceReadCap, Uint8Array>[] =
    [];

  onAsyncIterate(msgEncoder, ({ message }) => {
    alfie.send(message);
  });

  onAsyncIterate(
    decodeMessages({
      transport: betty,
      challengeLength: 4,
      encodings,
    }),
    (message) => {
      messages.push(message);
    },
  );

  shuffle(vectors);

  for (const message of vectors) {
    msgEncoder.encode(message);
  }

  await delay(15);

  assertEquals(messages, vectors);
});
