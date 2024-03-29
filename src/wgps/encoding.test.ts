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
  MSG_SETUP_BIND_AREA_OF_INTEREST,
  MSG_SETUP_BIND_READ_CAPABILITY,
  MSG_SETUP_BIND_STATIC_TOKEN,
  SyncEncodings,
  SyncMessage,
  SyncSchemes,
} from "./types.ts";
import { assertEquals } from "https://deno.land/std@0.202.0/assert/assert_equals.ts";
import { shuffle } from "https://deno.land/x/proc@0.21.9/mod3.ts";
import {
  TestNamespace,
  TestReadCap,
  testSchemeAccessControl,
  testSchemeAuthorisationToken,
  testSchemeNamespace,
  testSchemePai,
  testSchemePath,
  testSchemeSubspace,
  testSchemeSubspaceCap,
  TestSubspace,
  TestSubspaceReadCap,
} from "../test/test_schemes.ts";
import { randomPath } from "../test/utils.ts";
import { onAsyncIterate } from "./util.ts";
import { ANY_SUBSPACE, OPEN_END } from "../../deps.ts";

const vectors: SyncMessage<
  TestReadCap,
  Uint8Array,
  Uint8Array,
  TestSubspaceReadCap,
  Uint8Array,
  TestSubspace,
  TestSubspace
>[] = [
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
    channel: LogicalChannel.CapabilityChannel,
    amount: BigInt(256),
  },
  {
    kind: MSG_CONTROL_ISSUE_GUARANTEE,
    channel: LogicalChannel.IntersectionChannel,
    amount: BigInt(65536),
  },
  {
    kind: MSG_CONTROL_ISSUE_GUARANTEE,
    channel: LogicalChannel.CapabilityChannel,
    amount: BigInt(2147483648),
  },
  {
    kind: MSG_CONTROL_ABSOLVE,
    channel: LogicalChannel.IntersectionChannel,
    amount: BigInt(1),
  },
  {
    kind: MSG_CONTROL_ABSOLVE,
    channel: LogicalChannel.CapabilityChannel,
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
    channel: LogicalChannel.CapabilityChannel,
    target: BigInt(1),
  },
  {
    kind: MSG_CONTROL_PLEAD,
    channel: LogicalChannel.IntersectionChannel,
    target: BigInt(256),
  },
  {
    kind: MSG_CONTROL_PLEAD,
    channel: LogicalChannel.CapabilityChannel,
    target: BigInt(65536),
  },
  {
    kind: MSG_CONTROL_PLEAD,
    channel: LogicalChannel.IntersectionChannel,
    target: BigInt(2147483648),
  },
  {
    kind: MSG_CONTROL_ANNOUNCE_DROPPING,
    channel: LogicalChannel.CapabilityChannel,
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
    handleType: HandleType.CapabilityHandle,
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
      time: {
        start: BigInt(7),
        end: OPEN_END,
      },
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
      time: {
        start: BigInt(7),
        end: OPEN_END,
      },
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
      time: {
        start: BigInt(7),
        end: OPEN_END,
      },
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
      time: {
        start: BigInt(7),
        end: OPEN_END,
      },
      path: randomPath(),
    },
    signature: crypto.getRandomValues(new Uint8Array(33)),
  },

  // Setup

  {
    kind: MSG_SETUP_BIND_READ_CAPABILITY,
    capability: {
      receiver: TestSubspace.Alfie,
      namespace: TestNamespace.Family,
      path: [new Uint8Array(2), new Uint8Array(1)],
      subspace: TestSubspace.Betty,
      time: {
        start: BigInt(10),
        end: OPEN_END,
      },
    },
    handle: BigInt(64),
    signature: crypto.getRandomValues(new Uint8Array(33)),
  },

  {
    kind: MSG_SETUP_BIND_READ_CAPABILITY,
    capability: {
      receiver: TestSubspace.Alfie,
      namespace: TestNamespace.Project,
      path: [new Uint8Array(7)],
      subspace: TestSubspace.Phoebe,
      time: {
        start: BigInt(10),
        end: OPEN_END,
      },
    },
    handle: BigInt(12),
    signature: crypto.getRandomValues(new Uint8Array(33)),
  },

  {
    kind: MSG_SETUP_BIND_AREA_OF_INTEREST,
    authorisation: BigInt(23),
    areaOfInterest: {
      area: {
        includedSubspaceId: TestSubspace.Gemma,
        pathPrefix: [new Uint8Array(3)],
        timeRange: {
          start: BigInt(1000),
          end: BigInt(2000),
        },
      },
      maxCount: 0,
      maxSize: BigInt(0),
    },
  },

  {
    kind: MSG_SETUP_BIND_AREA_OF_INTEREST,
    authorisation: BigInt(5),
    areaOfInterest: {
      area: {
        includedSubspaceId: TestSubspace.Dalton,
        pathPrefix: [new Uint8Array(13)],
        timeRange: {
          start: BigInt(7),
          end: BigInt(13),
        },
      },
      maxCount: 12,
      maxSize: BigInt(3400),
    },
  },

  {
    kind: MSG_SETUP_BIND_STATIC_TOKEN,
    staticToken: TestSubspace.Epson,
  },
];

Deno.test("Encoding roundtrip test", async () => {
  const [alfie, betty] = transportPairInMemory();

  const encodings: SyncEncodings<
    TestReadCap,
    Uint8Array,
    Uint8Array,
    TestSubspaceReadCap,
    Uint8Array,
    TestSubspace,
    TestNamespace,
    TestSubspace
  > = {
    subspaceCapability: testSchemeSubspaceCap.encodings.subspaceCapability,
    syncSubspaceSignature:
      testSchemeSubspaceCap.encodings.syncSubspaceSignature,
    groupMember: testSchemePai.groupMemberEncoding,
    readCapability: testSchemeAccessControl.encodings.readCapability,
    subspace: testSchemeSubspace,
    syncSignature: testSchemeAccessControl.encodings.syncSignature,
    staticToken: testSchemeAuthorisationToken.encodings.staticToken,
  };

  const schemes: SyncSchemes<
    TestReadCap,
    TestSubspace,
    Uint8Array,
    TestSubspace,
    Uint8Array,
    Uint8Array,
    TestSubspaceReadCap,
    TestSubspace,
    Uint8Array,
    TestSubspace,
    TestSubspace,
    TestNamespace,
    TestSubspace
  > = {
    accessControl: testSchemeAccessControl,
    namespace: testSchemeNamespace,
    pai: testSchemePai,
    path: testSchemePath,
    subspace: testSchemeSubspace,
    subspaceCap: testSchemeSubspaceCap,
    authorisationToken: testSchemeAuthorisationToken,
  };

  const msgEncoder = new MessageEncoder(encodings, schemes, {
    getIntersectionPrivy: (handle) => {
      if (handle === BigInt(64)) {
        return {
          namespace: TestNamespace.Family,
          outer: {
            includedSubspaceId: TestSubspace.Betty,
            pathPrefix: [new Uint8Array(2), new Uint8Array(1)],
            timeRange: {
              start: BigInt(0),
              end: OPEN_END,
            },
          },
        };
      }

      return {
        namespace: TestNamespace.Project,
        outer: {
          includedSubspaceId: ANY_SUBSPACE,
          pathPrefix: [],
          timeRange: {
            start: BigInt(0),
            end: OPEN_END,
          },
        },
      };
    },
    getCap: (handle) => {
      if (handle === BigInt(23)) {
        return {
          namespace: TestNamespace.Bookclub,
          path: [new Uint8Array(3)],
          receiver: TestSubspace.Alfie,
          subspace: TestSubspace.Gemma,
          time: {
            start: BigInt(1),
            end: OPEN_END,
          },
        } as TestReadCap;
      }

      return {
        namespace: TestNamespace.Bookclub,
        path: [new Uint8Array(13)],
        receiver: TestSubspace.Alfie,
        subspace: TestSubspace.Dalton,
        time: {
          start: BigInt(2),
          end: BigInt(17),
        },
      };
    },
  });

  const messages: SyncMessage<
    TestReadCap,
    Uint8Array,
    Uint8Array,
    TestSubspaceReadCap,
    Uint8Array,
    TestSubspace,
    TestSubspace
  >[] = [];

  onAsyncIterate(msgEncoder, ({ message }) => {
    alfie.send(message);
  });

  onAsyncIterate(
    decodeMessages({
      transport: betty,
      challengeLength: 4,
      encodings,
      schemes: schemes,
      getIntersectionPrivy: (handle) => {
        if (handle === BigInt(64)) {
          return {
            namespace: TestNamespace.Family,
            outer: {
              includedSubspaceId: TestSubspace.Betty,
              pathPrefix: [new Uint8Array(2), new Uint8Array(1)],
              timeRange: {
                start: BigInt(0),
                end: OPEN_END,
              },
            },
          };
        }

        return {
          namespace: TestNamespace.Project,
          outer: {
            includedSubspaceId: ANY_SUBSPACE,
            pathPrefix: [new Uint8Array(7)],
            timeRange: {
              start: BigInt(0),
              end: OPEN_END,
            },
          },
        };
      },
      getCap: (handle) => {
        if (handle === BigInt(23)) {
          return Promise.resolve({
            namespace: TestNamespace.Bookclub,
            path: [new Uint8Array(3)],
            receiver: TestSubspace.Alfie,
            subspace: TestSubspace.Gemma,
            time: {
              start: BigInt(1),
              end: OPEN_END,
            },
          }) as Promise<TestReadCap>;
        }

        return Promise.resolve({
          namespace: TestNamespace.Bookclub,
          path: [new Uint8Array(13)],
          receiver: TestSubspace.Alfie,
          subspace: TestSubspace.Dalton,
          time: {
            start: BigInt(2),
            end: BigInt(17),
          },
        });
      },
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
