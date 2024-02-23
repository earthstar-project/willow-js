import { ANY_SUBSPACE, concat, Path } from "../../../deps.ts";
import { Intersection, PaiScheme } from "./types.ts";
import { isFragmentTriple, PaiFinder } from "./pai_finder.ts";
import { x25519 } from "npm:@noble/curves/ed25519";
import { encodePathWithSeparators } from "../../store/storage/storage_3d/triple_storage.ts";
import { HandleStore } from "../handle_store.ts";
import { delay } from "https://deno.land/std@0.202.0/async/delay.ts";
import { equals as equalsBytes } from "https://deno.land/std@0.202.0/bytes/equals.ts";
import { ReadAuthorisation } from "../types.ts";
import { assertEquals } from "https://deno.land/std@0.202.0/assert/mod.ts";

type TestNamespace = "family" | "project";
type TestSubspace = "alfie" | "betty" | "gemma" | typeof ANY_SUBSPACE;

type TestReadCap = {
  namespace: TestNamespace;
  subspace: TestSubspace;
  path: Path;
};

type TestSubspaceReadCap = {
  namespace: TestNamespace;
  path: Path;
};

type TestReadAuth = ReadAuthorisation<
  TestReadCap,
  TestSubspaceReadCap,
  null,
  null
>;

const testPaiScheme: PaiScheme<
  TestNamespace,
  TestSubspace,
  Uint8Array,
  Uint8Array,
  TestReadCap
> = {
  isGroupEqual: (a, b) => {
    return equalsBytes(a, b);
  },
  getScalar: () => {
    return crypto.getRandomValues(new Uint8Array(32));
  },
  getFragmentKit: (cap) => {
    if (cap.subspace === ANY_SUBSPACE) {
      return {
        grantedNamespace: cap.namespace,
        grantedPath: cap.path,
      };
    }

    return {
      grantedNamespace: cap.namespace,
      grantedSubspace: cap.subspace,
      grantedPath: cap.path,
    };
  },
  scalarMult(group, scalar) {
    return x25519.scalarMult(scalar, group);
  },
  async fragmentToGroup(fragment) {
    if (!isFragmentTriple(fragment)) {
      // Pair
      const [namespace, path] = fragment;

      const pairOrTripleByte = 1;
      const namespaceByte = namespace === "family" ? 0 : 1;
      const pathEncoded = encodePathWithSeparators(path);

      const bytes = concat(
        new Uint8Array([pairOrTripleByte, namespaceByte]),
        pathEncoded,
      );

      const digest = await crypto.subtle.digest("SHA-256", bytes);

      return new Uint8Array(digest);
    }

    const [namespace, subspace, path] = fragment;

    const pairOrTripleByte = 0;
    const namespaceByte = namespace === "family" ? 0 : 1;
    const subspaceByte = subspace === "alfie"
      ? 0
      : subspace === "betty"
      ? 1
      : subspace === "gemma"
      ? 2
      : 3;
    const pathEncoded = encodePathWithSeparators(path);

    const bytes = concat(
      new Uint8Array([pairOrTripleByte, namespaceByte, subspaceByte]),
      pathEncoded,
    );

    const digest = await crypto.subtle.digest("SHA-256", bytes);

    return new Uint8Array(digest);
  },
};

function setupFinders(): {
  alfie: PaiFinder<
    TestNamespace,
    TestSubspace,
    Uint8Array,
    Uint8Array,
    TestReadCap,
    TestSubspaceReadCap,
    null,
    null
  >;

  betty: PaiFinder<
    TestNamespace,
    TestSubspace,
    Uint8Array,
    Uint8Array,
    TestReadCap,
    TestSubspaceReadCap,
    null,
    null
  >;
} {
  const finderAlfie = new PaiFinder<
    TestNamespace,
    TestSubspace,
    Uint8Array,
    Uint8Array,
    TestReadCap,
    TestSubspaceReadCap,
    null,
    null
  >({
    paiScheme: testPaiScheme,
    intersectionHandlesOurs: new HandleStore<Intersection<Uint8Array>>(),
    intersectionHandlesTheirs: new HandleStore<Intersection<Uint8Array>>(),
  });

  const finderBetty = new PaiFinder<
    TestNamespace,
    TestSubspace,
    Uint8Array,
    Uint8Array,
    TestReadCap,
    TestSubspaceReadCap,
    null,
    null
  >({
    paiScheme: testPaiScheme,
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
    TestNamespace,
    TestSubspace,
    Uint8Array,
    Uint8Array,
    TestReadCap,
    TestSubspaceReadCap,
    null,
    null
  >,
  against: PaiFinder<
    TestNamespace,
    TestSubspace,
    Uint8Array,
    Uint8Array,
    TestReadCap,
    TestSubspaceReadCap,
    null,
    null
  >,
): AsyncIterable<TestReadAuth> {
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

  for await (const auth of of.intersections()) {
    yield auth;
  }
}

async function* subspaceCaps(
  of: PaiFinder<
    TestNamespace,
    TestSubspace,
    Uint8Array,
    Uint8Array,
    TestReadCap,
    TestSubspaceReadCap,
    null,
    null
  >,
  against: PaiFinder<
    TestNamespace,
    TestSubspace,
    Uint8Array,
    Uint8Array,
    TestReadCap,
    TestSubspaceReadCap,
    null,
    null
  >,
): AsyncIterable<{ handle: bigint; subspaceCap: TestSubspaceReadCap }> {
  // Set up subspace reqs
  (async () => {
    for await (const handle of against.subspaceCapRequests()) {
      of.receivedSubspaceCapRequest(handle);
    }
  })();

  for await (const reply of of.subspaceCapReplies()) {
    yield reply;
  }
}

Deno.test("PaiFinder (standard intersection)", async () => {
  const { alfie, betty } = setupFinders();

  const intersectionsAlfie: TestReadAuth[] = [];
  const intersectionsBetty: TestReadAuth[] = [];

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
      namespace: "family",
      subspace: "alfie",
      path: [new Uint8Array([0]), new Uint8Array([1]), new Uint8Array([2])],
    },
    signature: null,
  };

  const disjointAuth: TestReadAuth = {
    capability: {
      namespace: "project",
      subspace: "betty",
      path: [new Uint8Array([0]), new Uint8Array([1]), new Uint8Array([2])],
    },
    signature: null,
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
      namespace: "project",
      subspace: "gemma",
      path: [new Uint8Array([9]), new Uint8Array([8]), new Uint8Array([7])],
    },
    signature: null,
  };

  const lessSpecificAuth: TestReadAuth = {
    capability: {
      namespace: "project",
      subspace: "gemma",
      path: [new Uint8Array([9])],
    },
    signature: null,
  };

  alfie.submitAuthorisation(moreSpecificAuth);
  betty.submitAuthorisation(lessSpecificAuth);

  await delay(1);

  assertEquals(intersectionsAlfie, [intersectingAuth]);
  assertEquals(intersectionsBetty, [intersectingAuth, lessSpecificAuth]);
});

Deno.test("PaiFinder (requesting subspace caps)", async () => {
  const { alfie, betty } = setupFinders();

  const intersectionsAlfie: TestReadAuth[] = [];
  const intersectionsBetty: TestReadAuth[] = [];

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

  const subspaceCapsAlfie: TestSubspaceReadCap[] = [];
  const subspaceCapsBetty: TestSubspaceReadCap[] = [];

  (async () => {
    for await (const reply of subspaceCaps(alfie, betty)) {
      subspaceCapsAlfie.push(reply.subspaceCap);
    }
  })();

  (async () => {
    for await (const reply of subspaceCaps(betty, alfie)) {
      subspaceCapsBetty.push(reply.subspaceCap);
    }
  })();

  const cap: TestReadAuth = {
    capability: {
      namespace: "family",
      subspace: "gemma",
      path: [],
    },
    signature: null,
    subspaceSignature: null,
  };

  const subspaceCap: TestReadAuth = {
    capability: {
      namespace: "family",
      subspace: ANY_SUBSPACE,
      path: [new Uint8Array([7])],
    },
    subspaceCapability: {
      namespace: "family",
      path: [new Uint8Array([7])],
    },
    signature: null,
    subspaceSignature: null,
  };

  alfie.submitAuthorisation(cap);
  betty.submitAuthorisation(subspaceCap);

  await delay(2);

  assertEquals(intersectionsAlfie, []);
  assertEquals(intersectionsBetty, []);
  assertEquals(subspaceCapsAlfie, []);
  assertEquals(subspaceCapsBetty, [subspaceCap.subspaceCapability]);

  const alfieCap: TestReadAuth = {
    capability: {
      namespace: "project",
      subspace: "alfie",
      path: [new Uint8Array([7])],
    },
    signature: null,
    subspaceSignature: null,
  };

  const bettyCap: TestReadAuth = {
    capability: {
      namespace: "project",
      subspace: "betty",
      path: [new Uint8Array([7])],
    },
    signature: null,
    subspaceSignature: null,
  };

  alfie.submitAuthorisation(alfieCap);
  betty.submitAuthorisation(bettyCap);

  await delay(2);

  assertEquals(intersectionsAlfie, []);
  assertEquals(intersectionsBetty, []);
  assertEquals(subspaceCapsAlfie, []);
  assertEquals(subspaceCapsBetty, [subspaceCap.subspaceCapability]);
});
