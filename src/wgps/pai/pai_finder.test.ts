import { ANY_SUBSPACE, OPEN_END } from "../../../deps.ts";
import { Intersection } from "./types.ts";
import { PaiFinder } from "./pai_finder.ts";
import { HandleStore } from "../handle_store.ts";
import { delay } from "https://deno.land/std@0.202.0/async/delay.ts";
import { assertEquals } from "https://deno.land/std@0.202.0/assert/mod.ts";
import {
  TestNamespace,
  TestReadAuth,
  TestReadCap,
  testSchemeNamespace,
  testSchemePai,
  TestSubspace,
  TestSubspaceReadCap,
} from "../../test/test_schemes.ts";
import { ReadAuthorisation } from "../types.ts";

function setupFinders(): {
  alfie: PaiFinder<
    TestReadCap,
    Uint8Array,
    Uint8Array,
    TestSubspaceReadCap,
    TestNamespace,
    TestSubspace
  >;

  betty: PaiFinder<
    TestReadCap,
    Uint8Array,
    Uint8Array,
    TestSubspaceReadCap,
    TestNamespace,
    TestSubspace
  >;
} {
  const finderAlfie = new PaiFinder<
    TestReadCap,
    Uint8Array,
    Uint8Array,
    TestSubspaceReadCap,
    TestNamespace,
    TestSubspace
  >({
    namespaceScheme: testSchemeNamespace,
    paiScheme: testSchemePai,
    intersectionHandlesOurs: new HandleStore<Intersection<Uint8Array>>(),
    intersectionHandlesTheirs: new HandleStore<Intersection<Uint8Array>>(),
  });

  const finderBetty = new PaiFinder<
    TestReadCap,
    Uint8Array,
    Uint8Array,
    TestSubspaceReadCap,
    TestNamespace,
    TestSubspace
  >({
    namespaceScheme: testSchemeNamespace,
    paiScheme: testSchemePai,
    intersectionHandlesOurs: new HandleStore<Intersection<Uint8Array>>(),
    intersectionHandlesTheirs: new HandleStore<Intersection<Uint8Array>>(),
  });

  return {
    alfie: finderAlfie,
    betty: finderBetty,
  };
}

async function* intersections(
  of: PaiFinder<
    TestReadCap,
    Uint8Array,
    Uint8Array,
    TestSubspaceReadCap,
    TestNamespace,
    TestSubspace
  >,
  against: PaiFinder<
    TestReadCap,
    Uint8Array,
    Uint8Array,
    TestSubspaceReadCap,
    TestNamespace,
    TestSubspace
  >,
): AsyncIterable<
  ReadAuthorisation<
    TestReadCap,
    TestSubspaceReadCap
  >
> {
  // Set up fragment binds
  (async () => {
    for await (const bind of against.fragmentBinds()) {
      of.receivedBind(bind.group, bind.isSecondary);
    }
  })();

  // Set up fragment replies
  (async () => {
    for await (const reply of against.fragmentReplies()) {
      of.receivedReply(reply.handle, reply.groupMember);
    }
  })();

  // Set up subspace reqs
  (async () => {
    for await (const handle of against.subspaceCapRequests()) {
      of.receivedSubspaceCapRequest(handle);
    }
  })();

  (async () => {
    for await (const reply of against.subspaceCapReplies()) {
      of.receivedVerifiedSubspaceCapReply(
        reply.handle,
        reply.subspaceCap.namespace,
      );
    }
  })();

  for await (const { authorisation } of of.intersections()) {
    yield authorisation;
  }
}

Deno.test("PaiFinder (standard intersection)", async () => {
  const { alfie, betty } = setupFinders();

  const intersectionsAlfie: ReadAuthorisation<
    TestReadCap,
    TestSubspaceReadCap
  >[] = [];
  const intersectionsBetty: ReadAuthorisation<
    TestReadCap,
    TestSubspaceReadCap
  >[] = [];

  (async () => {
    for await (const int of intersections(alfie, betty)) {
      intersectionsAlfie.push(int);
    }
  })();

  (async () => {
    for await (const int of intersections(betty, alfie)) {
      intersectionsBetty.push(int);
    }
  })();

  const intersectingAuth: TestReadAuth = {
    capability: {
      namespace: TestNamespace.Family,
      subspace: TestSubspace.Alfie,
      receiver: TestSubspace.Alfie,
      path: [new Uint8Array([0]), new Uint8Array([1]), new Uint8Array([2])],
      time: {
        start: BigInt(0),
        end: OPEN_END,
      },
    },
  };

  const disjointAuth: TestReadAuth = {
    capability: {
      namespace: TestNamespace.Project,
      subspace: TestSubspace.Betty,
      receiver: TestSubspace.Betty,
      path: [new Uint8Array([0]), new Uint8Array([1]), new Uint8Array([2])],
      time: {
        start: BigInt(0),
        end: OPEN_END,
      },
    },
  };

  alfie.submitAuthorisation(intersectingAuth);
  betty.submitAuthorisation(disjointAuth);

  await delay(1);

  assertEquals(intersectionsAlfie, []);
  assertEquals(intersectionsBetty, []);

  betty.submitAuthorisation(intersectingAuth);

  await delay(1);

  assertEquals(intersectionsAlfie, [intersectingAuth]);
  assertEquals(intersectionsBetty, [intersectingAuth]);

  const moreSpecificAuth: TestReadAuth = {
    capability: {
      namespace: TestNamespace.Bookclub,
      subspace: TestSubspace.Gemma,
      receiver: TestSubspace.Gemma,
      path: [new Uint8Array([9]), new Uint8Array([8]), new Uint8Array([7])],
      time: {
        start: BigInt(0),
        end: OPEN_END,
      },
    },
  };

  const lessSpecificAuth: TestReadAuth = {
    capability: {
      namespace: TestNamespace.Bookclub,
      subspace: TestSubspace.Gemma,
      receiver: TestSubspace.Gemma,
      path: [new Uint8Array([9])],
      time: {
        start: BigInt(0),
        end: OPEN_END,
      },
    },
  };

  alfie.submitAuthorisation(moreSpecificAuth);
  betty.submitAuthorisation(lessSpecificAuth);

  await delay(1);

  assertEquals(intersectionsAlfie, [intersectingAuth]);
  assertEquals(intersectionsBetty, [intersectingAuth, lessSpecificAuth]);
});

Deno.test("PaiFinder (requesting subspace caps)", async () => {
  const { alfie, betty } = setupFinders();

  const intersectionsAlfie: ReadAuthorisation<
    TestReadCap,
    TestSubspaceReadCap
  >[] = [];
  const intersectionsBetty: ReadAuthorisation<
    TestReadCap,
    TestSubspaceReadCap
  >[] = [];

  (async () => {
    for await (const auth of intersections(alfie, betty)) {
      intersectionsAlfie.push(auth);
    }
  })();

  (async () => {
    for await (const auth of intersections(betty, alfie)) {
      intersectionsBetty.push(auth);
    }
  })();

  const cap: TestReadAuth = {
    capability: {
      namespace: TestNamespace.Family,
      subspace: TestSubspace.Gemma,
      receiver: TestSubspace.Gemma,
      path: [],
      time: {
        start: BigInt(0),
        end: OPEN_END,
      },
    },
  };

  const subspaceCap: TestReadAuth = {
    capability: {
      namespace: TestNamespace.Family,
      subspace: ANY_SUBSPACE,
      receiver: TestSubspace.Gemma,
      path: [new Uint8Array([7])],
      time: {
        start: BigInt(0),
        end: OPEN_END,
      },
    },
    subspaceCapability: {
      namespace: TestNamespace.Family,
      receiver: TestSubspace.Gemma,
      path: [new Uint8Array([7])],
      time: {
        start: BigInt(0),
        end: OPEN_END,
      },
    },
  };

  alfie.submitAuthorisation(cap);
  betty.submitAuthorisation(subspaceCap);

  await delay(2);

  assertEquals(intersectionsAlfie, [cap]);
  assertEquals(intersectionsBetty, []);

  const alfieCap: TestReadAuth = {
    capability: {
      namespace: TestNamespace.Project,
      subspace: TestSubspace.Alfie,
      receiver: TestSubspace.Alfie,
      path: [new Uint8Array([7])],
      time: {
        start: BigInt(0),
        end: OPEN_END,
      },
    },
  };

  const bettyCap: TestReadAuth = {
    capability: {
      namespace: TestNamespace.Project,
      subspace: TestSubspace.Alfie,
      receiver: TestSubspace.Betty,
      path: [new Uint8Array([7])],
      time: {
        start: BigInt(0),
        end: OPEN_END,
      },
    },
  };

  alfie.submitAuthorisation(alfieCap);
  betty.submitAuthorisation(bettyCap);

  await delay(2);

  assertEquals(intersectionsAlfie, [cap, alfieCap]);
  assertEquals(intersectionsBetty, [bettyCap]);
});
