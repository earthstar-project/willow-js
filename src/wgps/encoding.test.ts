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
  MSG_DATA_SEND_ENTRY,
  MSG_DATA_SEND_PAYLOAD,
  MSG_DATA_SET_EAGERNESS,
  MSG_PAI_BIND_FRAGMENT,
  MSG_PAI_REPLY_FRAGMENT,
  MSG_PAI_REPLY_SUBSPACE_CAPABILITY,
  MSG_PAI_REQUEST_SUBSPACE_CAPABILITY,
  MSG_RECONCILIATION_ANNOUNCE_ENTRIES,
  MSG_RECONCILIATION_SEND_ENTRY,
  MSG_RECONCILIATION_SEND_FINGERPRINT,
  MSG_SETUP_BIND_AREA_OF_INTEREST,
  MSG_SETUP_BIND_READ_CAPABILITY,
  MSG_SETUP_BIND_STATIC_TOKEN,
  MsgReconciliationAnnounceEntries,
  MsgReconciliationSendEntry,
  SyncMessage,
  SyncSchemes,
} from "./types.ts";
import { assertEquals } from "https://deno.land/std@0.202.0/assert/assert_equals.ts";
import { shuffle } from "https://deno.land/x/proc@0.21.9/mod3.ts";
import {
  TestNamespace,
  TestReadCap,
  testSchemeAccessControl,
  testSchemeAuthorisation,
  testSchemeAuthorisationToken,
  testSchemeFingerprint,
  testSchemeNamespace,
  testSchemePai,
  testSchemePath,
  testSchemePayload,
  testSchemeSubspace,
  testSchemeSubspaceCap,
  TestSubspace,
  TestSubspaceReadCap,
} from "../test/test_schemes.ts";
import { randomPath } from "../test/utils.ts";
import { onAsyncIterate } from "./util.ts";
import {
  ANY_SUBSPACE,
  defaultEntry,
  defaultRange3d,
  fullArea,
  OPEN_END,
} from "../../deps.ts";

const vectors: SyncMessage<
  TestReadCap,
  Uint8Array,
  Uint8Array,
  TestSubspaceReadCap,
  Uint8Array,
  Uint8Array,
  TestSubspace,
  Uint8Array,
  TestNamespace,
  TestSubspace,
  ArrayBuffer
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

  // Reconciliation

  {
    kind: MSG_RECONCILIATION_SEND_FINGERPRINT,
    fingerprint: crypto.getRandomValues(new Uint8Array(32)),
    receiverHandle: 2048n,
    senderHandle: 500000n,
    range: {
      subspaceRange: {
        start: TestSubspace.Alfie,
        end: TestSubspace.Gemma,
      },
      pathRange: {
        start: [new Uint8Array([1])],
        end: [new Uint8Array([2])],
      },
      timeRange: {
        start: 1000n,
        end: 3500n,
      },
    },
  },

  {
    kind: MSG_RECONCILIATION_SEND_FINGERPRINT,
    fingerprint: crypto.getRandomValues(new Uint8Array(32)),
    receiverHandle: 0n,
    senderHandle: 0n,
    range: {
      subspaceRange: {
        start: TestSubspace.Alfie,
        end: TestSubspace.Gemma,
      },
      pathRange: {
        start: [new Uint8Array([1])],
        end: [new Uint8Array([2])],
      },
      timeRange: {
        start: 1000n,
        end: 3500n,
      },
    },
  },

  // (announce entries are a special case - see below)

  // Data

  {
    kind: MSG_DATA_SEND_ENTRY,
    dynamicToken: crypto.getRandomValues(new Uint8Array(32)),
    staticTokenHandle: 8000000000n,
    entry: {
      namespaceId: TestNamespace.Family,
      path: [new Uint8Array([7])],
      payloadDigest: crypto.getRandomValues(new Uint8Array(32)),
      payloadLength: 500n,
      subspaceId: TestSubspace.Betty,
      timestamp: 40000n,
    },
    offset: 0n,
  },

  {
    kind: MSG_DATA_SEND_ENTRY,
    dynamicToken: crypto.getRandomValues(new Uint8Array(32)),
    staticTokenHandle: 23n,
    entry: {
      namespaceId: TestNamespace.Family,
      path: [new Uint8Array([7, 3])],
      payloadDigest: crypto.getRandomValues(new Uint8Array(32)),
      payloadLength: 500n,
      subspaceId: TestSubspace.Betty,
      timestamp: 2n,
    },
    offset: 30n,
  },

  {
    kind: MSG_DATA_SEND_ENTRY,
    dynamicToken: crypto.getRandomValues(new Uint8Array(32)),
    staticTokenHandle: 23n,
    entry: {
      namespaceId: TestNamespace.Family,
      path: [new Uint8Array([7, 3])],
      payloadDigest: crypto.getRandomValues(new Uint8Array(32)),
      payloadLength: 500n,
      subspaceId: TestSubspace.Betty,
      timestamp: 1000n,
    },
    offset: 500n,
  },

  {
    kind: MSG_DATA_SEND_PAYLOAD,
    amount: 32n,
    bytes: crypto.getRandomValues(new Uint8Array(32)),
  },

  {
    kind: MSG_DATA_SET_EAGERNESS,
    isEager: true,
    receiverHandle: 3n,
    senderHandle: 579n,
  },

  {
    kind: MSG_DATA_SET_EAGERNESS,
    isEager: false,
    receiverHandle: 0n,
    senderHandle: 21555n,
  },
];

// Because ReconciliationSendEntry vectors are decoded using state (an ReconciliationAnnounceEntries must have come before)
// We need to split them off into their own little section.
const sendEntryVectors: (
  | MsgReconciliationAnnounceEntries<TestSubspace>
  | MsgReconciliationSendEntry<
    Uint8Array,
    TestNamespace,
    TestSubspace,
    ArrayBuffer
  >
)[] = [
  {
    kind: MSG_RECONCILIATION_ANNOUNCE_ENTRIES,
    receiverHandle: 220n,
    senderHandle: 250n,
    wantResponse: true,
    count: 1n,
    willSort: true,
    range: {
      subspaceRange: {
        start: TestSubspace.Alfie,
        end: TestSubspace.Gemma,
      },
      pathRange: {
        start: [new Uint8Array([1])],
        end: [new Uint8Array([2])],
      },
      timeRange: {
        start: 1000n,
        end: 3500n,
      },
    },
  },
  {
    kind: MSG_RECONCILIATION_SEND_ENTRY,
    dynamicToken: crypto.getRandomValues(new Uint8Array(32)),
    staticTokenHandle: 8n,
    entry: {
      available: 3000n,
      entry: {
        namespaceId: TestNamespace.Family,
        path: [new Uint8Array([7])],
        payloadDigest: crypto.getRandomValues(new Uint8Array(32)),
        payloadLength: 500n,
        subspaceId: TestSubspace.Betty,
        timestamp: 2000n,
      },
    },
  },
  {
    kind: MSG_RECONCILIATION_ANNOUNCE_ENTRIES,
    receiverHandle: 0n,
    senderHandle: 0n,
    wantResponse: false,
    count: 0n,
    willSort: false,
    range: {
      subspaceRange: {
        start: TestSubspace.Alfie,
        end: TestSubspace.Gemma,
      },
      pathRange: {
        start: [new Uint8Array([1])],
        end: [new Uint8Array([2])],
      },
      timeRange: {
        start: 1000n,
        end: 3500n,
      },
    },
  },
  {
    kind: MSG_RECONCILIATION_ANNOUNCE_ENTRIES,
    receiverHandle: 0n,
    senderHandle: 0n,
    wantResponse: false,
    count: 4n,
    willSort: false,
    range: {
      subspaceRange: {
        start: TestSubspace.Alfie,
        end: TestSubspace.Gemma,
      },
      pathRange: {
        start: [new Uint8Array([1])],
        end: [new Uint8Array([2])],
      },
      timeRange: {
        start: 1000n,
        end: 3500n,
      },
    },
  },

  {
    kind: MSG_RECONCILIATION_SEND_ENTRY,
    dynamicToken: crypto.getRandomValues(new Uint8Array(32)),
    staticTokenHandle: 80n,
    entry: {
      available: 3000n,
      entry: {
        namespaceId: TestNamespace.Family,
        path: [new Uint8Array([7])],
        payloadDigest: crypto.getRandomValues(new Uint8Array(32)),
        payloadLength: 500n,
        subspaceId: TestSubspace.Betty,
        timestamp: 40000n,
      },
    },
  },
  {
    kind: MSG_RECONCILIATION_SEND_ENTRY,
    dynamicToken: crypto.getRandomValues(new Uint8Array(32)),
    staticTokenHandle: 800n,
    entry: {
      available: 3000n,
      entry: {
        namespaceId: TestNamespace.Family,
        path: [new Uint8Array([7])],
        payloadDigest: crypto.getRandomValues(new Uint8Array(32)),
        payloadLength: 500n,
        subspaceId: TestSubspace.Betty,
        timestamp: 40000n,
      },
    },
  },
  {
    kind: MSG_RECONCILIATION_SEND_ENTRY,
    dynamicToken: crypto.getRandomValues(new Uint8Array(32)),
    staticTokenHandle: 8000000n,
    entry: {
      available: 3000n,
      entry: {
        namespaceId: TestNamespace.Family,
        path: [new Uint8Array([7])],
        payloadDigest: crypto.getRandomValues(new Uint8Array(32)),
        payloadLength: 500n,
        subspaceId: TestSubspace.Betty,
        timestamp: 40000n,
      },
    },
  },
  {
    kind: MSG_RECONCILIATION_SEND_ENTRY,
    dynamicToken: crypto.getRandomValues(new Uint8Array(32)),
    staticTokenHandle: 8000000000n,
    entry: {
      available: 3000n,
      entry: {
        namespaceId: TestNamespace.Family,
        path: [new Uint8Array([7])],
        payloadDigest: crypto.getRandomValues(new Uint8Array(32)),
        payloadLength: 500n,
        subspaceId: TestSubspace.Betty,
        timestamp: 40000n,
      },
    },
  },
];

Deno.test("Encoding roundtrip test", async () => {
  const [alfie, betty] = transportPairInMemory();

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
    Uint8Array,
    Uint8Array,
    TestSubspace,
    Uint8Array,
    TestNamespace,
    TestSubspace,
    ArrayBuffer,
    TestSubspace
  > = {
    accessControl: testSchemeAccessControl,
    namespace: testSchemeNamespace,
    pai: testSchemePai,
    path: testSchemePath,
    subspace: testSchemeSubspace,
    subspaceCap: testSchemeSubspaceCap,
    authorisationToken: testSchemeAuthorisationToken,
    payload: testSchemePayload,
    fingerprint: testSchemeFingerprint,
    authorisation: testSchemeAuthorisation,
  };

  const msgEncoder = new MessageEncoder(schemes, {
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
    defaultNamespaceId: TestNamespace.Family,
    defaultSubspaceId: TestSubspace.Alfie,
    defaultPayloadDigest: new Uint8Array(32),
    handleToNamespaceId: () => TestNamespace.Family,
    aoiHandlesToRange3d: () => {
      return Promise.resolve(defaultRange3d(TestSubspace.Alfie));
    },
    getCurrentlySentEntry: () =>
      defaultEntry(
        TestNamespace.Family,
        TestSubspace.Alfie,
        new Uint8Array(32),
      ),
  });

  const messages: SyncMessage<
    TestReadCap,
    Uint8Array,
    Uint8Array,
    TestSubspaceReadCap,
    Uint8Array,
    Uint8Array,
    TestSubspace,
    Uint8Array,
    TestNamespace,
    TestSubspace,
    ArrayBuffer
  >[] = [];

  onAsyncIterate(msgEncoder, ({ message }) => {
    alfie.send(message);
  });

  // Announcements and SendEntry are conditionally decoded depending on whether entries are expected or not.
  // Here we just decode announcements first and SendEntry later in a separate batch.

  onAsyncIterate(
    decodeMessages({
      transport: betty,
      challengeLength: 4,
      schemes: schemes,
      getCurrentlyReceivedEntry: () =>
        defaultEntry(
          TestNamespace.Family,
          TestSubspace.Alfie,
          new Uint8Array(32),
        ),
      aoiHandlesToArea: () => fullArea<TestSubspace>(),
      aoiHandlesToNamespace: () => TestNamespace.Family,
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
      defaultNamespaceId: TestNamespace.Family,
      defaultSubspaceId: TestSubspace.Alfie,
      defaultPayloadDigest: new Uint8Array(32),
      handleToNamespaceId: () => TestNamespace.Family,
      aoiHandlesToRange3d: () => {
        return Promise.resolve(defaultRange3d(TestSubspace.Alfie));
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

  for (const message of sendEntryVectors) {
    msgEncoder.encode(message);
  }

  await delay(15);

  assertEquals(messages, [...vectors, ...sendEntryVectors]);
});
