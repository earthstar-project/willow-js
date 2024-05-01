import { delay } from "https://deno.land/std@0.202.0/async/delay.ts";
import { decodeMessages } from "./decoding/decode_messages.ts";
import { MessageEncoder } from "./encoding/message_encoder.ts";
import { transportPairInMemory } from "./transports/in_memory.ts";
import {
  HandleType,
  LogicalChannel,
  MsgKind,
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
    kind: MsgKind.CommitmentReveal,
    nonce: crypto.getRandomValues(new Uint8Array(4)),
  },
  {
    kind: MsgKind.ControlIssueGuarantee,
    channel: LogicalChannel.IntersectionChannel,
    amount: BigInt(1),
  },
  {
    kind: MsgKind.ControlIssueGuarantee,
    channel: LogicalChannel.CapabilityChannel,
    amount: BigInt(256),
  },
  {
    kind: MsgKind.ControlIssueGuarantee,
    channel: LogicalChannel.IntersectionChannel,
    amount: BigInt(65536),
  },
  {
    kind: MsgKind.ControlIssueGuarantee,
    channel: LogicalChannel.CapabilityChannel,
    amount: BigInt(2147483648),
  },
  {
    kind: MsgKind.ControlAbsolve,
    channel: LogicalChannel.IntersectionChannel,
    amount: BigInt(1),
  },
  {
    kind: MsgKind.ControlAbsolve,
    channel: LogicalChannel.CapabilityChannel,
    amount: BigInt(256),
  },
  {
    kind: MsgKind.ControlAbsolve,
    channel: LogicalChannel.IntersectionChannel,
    amount: BigInt(65536),
  },

  {
    kind: MsgKind.ControlAbsolve,
    channel: LogicalChannel.IntersectionChannel,
    amount: BigInt(2147483648),
  },
  {
    kind: MsgKind.ControlPlead,
    channel: LogicalChannel.CapabilityChannel,
    target: BigInt(1),
  },
  {
    kind: MsgKind.ControlPlead,
    channel: LogicalChannel.IntersectionChannel,
    target: BigInt(256),
  },
  {
    kind: MsgKind.ControlPlead,
    channel: LogicalChannel.CapabilityChannel,
    target: BigInt(65536),
  },
  {
    kind: MsgKind.ControlPlead,
    channel: LogicalChannel.IntersectionChannel,
    target: BigInt(2147483648),
  },
  {
    kind: MsgKind.ControlAnnounceDropping,
    channel: LogicalChannel.CapabilityChannel,
  },
  {
    kind: MsgKind.ControlApologise,
    channel: LogicalChannel.IntersectionChannel,
  },
  {
    kind: MsgKind.ControlFree,
    handle: BigInt(1),
    handleType: HandleType.IntersectionHandle,
    mine: true,
  },
  {
    kind: MsgKind.ControlFree,
    handle: BigInt(256),
    handleType: HandleType.CapabilityHandle,
    mine: false,
  },
  {
    kind: MsgKind.ControlFree,
    handle: BigInt(65536),
    handleType: HandleType.IntersectionHandle,
    mine: true,
  },
  {
    kind: MsgKind.ControlFree,
    handle: BigInt(2147483648),
    handleType: HandleType.IntersectionHandle,
    mine: false,
  },

  // PAI

  {
    kind: MsgKind.PaiBindFragment,
    isSecondary: false,
    groupMember: crypto.getRandomValues(new Uint8Array(32)),
  },
  {
    kind: MsgKind.PaiBindFragment,
    isSecondary: true,
    groupMember: crypto.getRandomValues(new Uint8Array(32)),
  },

  {
    kind: MsgKind.PaiReplyFragment,
    groupMember: crypto.getRandomValues(new Uint8Array(32)),
    handle: BigInt(1),
  },
  {
    kind: MsgKind.PaiReplyFragment,
    groupMember: crypto.getRandomValues(new Uint8Array(32)),
    handle: BigInt(256),
  },
  {
    kind: MsgKind.PaiReplyFragment,
    groupMember: crypto.getRandomValues(new Uint8Array(32)),
    handle: BigInt(65536),
  },
  {
    kind: MsgKind.PaiReplyFragment,
    groupMember: crypto.getRandomValues(new Uint8Array(32)),
    handle: BigInt(2147483648),
  },

  {
    kind: MsgKind.PaiRequestSubspaceCapability,
    handle: BigInt(1),
  },
  {
    kind: MsgKind.PaiRequestSubspaceCapability,
    handle: BigInt(256),
  },
  {
    kind: MsgKind.PaiRequestSubspaceCapability,
    handle: BigInt(65536),
  },
  {
    kind: MsgKind.PaiRequestSubspaceCapability,
    handle: BigInt(2147483648),
  },

  {
    kind: MsgKind.PaiReplySubspaceCapability,
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
    kind: MsgKind.PaiReplySubspaceCapability,
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
    kind: MsgKind.PaiReplySubspaceCapability,
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
    kind: MsgKind.PaiReplySubspaceCapability,
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
    kind: MsgKind.SetupBindReadCapability,
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
    kind: MsgKind.SetupBindReadCapability,
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
    kind: MsgKind.SetupBindAreaOfInterest,
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
    kind: MsgKind.SetupBindAreaOfInterest,
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
    kind: MsgKind.SetupBindStaticToken,
    staticToken: TestSubspace.Epson,
  },

  // Reconciliation

  {
    kind: MsgKind.ReconciliationSendFingerprint,
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
    kind: MsgKind.ReconciliationSendFingerprint,
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
    kind: MsgKind.DataSendEntry,
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
    kind: MsgKind.DataSendEntry,
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
    kind: MsgKind.DataSendEntry,
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
    kind: MsgKind.DataSendPayload,
    amount: 32n,
    bytes: crypto.getRandomValues(new Uint8Array(32)),
  },

  {
    kind: MsgKind.DataSetMetadata,
    isEager: true,
    receiverHandle: 3n,
    senderHandle: 579n,
  },

  {
    kind: MsgKind.DataSetMetadata,
    isEager: false,
    receiverHandle: 0n,
    senderHandle: 21555n,
  },

  {
    kind: MsgKind.DataBindPayloadRequest,
    capability: 43n,
    entry: {
      namespaceId: TestNamespace.Family,
      path: [new Uint8Array([7, 3])],
      payloadDigest: crypto.getRandomValues(new Uint8Array(32)),
      payloadLength: 500n,
      subspaceId: TestSubspace.Betty,
      timestamp: 1000n,
    },
    offset: 0n,
  },

  {
    kind: MsgKind.DataBindPayloadRequest,
    capability: 1000n,
    entry: {
      namespaceId: TestNamespace.Family,
      path: [],
      payloadDigest: crypto.getRandomValues(new Uint8Array(32)),
      payloadLength: 500n,
      subspaceId: TestSubspace.Betty,
      timestamp: 1000n,
    },
    offset: 25500n,
  },

  {
    kind: MsgKind.DataReplyPayload,
    handle: 400n,
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
    kind: MsgKind.ReconciliationAnnounceEntries,
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
    kind: MsgKind.ReconciliationSendEntry,
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
    kind: MsgKind.ReconciliationAnnounceEntries,
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
    kind: MsgKind.ReconciliationAnnounceEntries,
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
    kind: MsgKind.ReconciliationSendEntry,
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
    kind: MsgKind.ReconciliationSendEntry,
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
    kind: MsgKind.ReconciliationSendEntry,
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
    kind: MsgKind.ReconciliationSendEntry,
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
