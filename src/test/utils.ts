import { Path } from "../../deps.ts";
import { TestNamespace, TestSubspace } from "./test_schemes.ts";

export function randomNamespace(): TestNamespace {
  return Math.floor(Math.random() * 5);
}

export function randomSubspace(): TestSubspace {
  return Math.floor(Math.random() * 7);
}

export const ALL_SUBSPACES: TestSubspace[] = [0, 1, 2, 3, 4, 5, 6];

export function randomTimestamp() {
  return BigInt(Math.floor(Math.random() * 1000));
}

/** Makes a random path which fits within the constraints of `testSchemePath`. */
export function randomPath(): Path {
  const pathLength = Math.floor(Math.random() * 4);

  const maxComponentLength = pathLength === 4
    ? 2
    : pathLength === 3
    ? 2
    : pathLength === 2
    ? 4
    : pathLength === 1
    ? 8
    : 0;

  const path = [];

  // Now create components with random uint.
  for (let i = 0; i < pathLength; i++) {
    const pathLength = Math.floor(Math.random() * maxComponentLength);

    path.push(crypto.getRandomValues(new Uint8Array(pathLength)));
  }

  return path;
}
