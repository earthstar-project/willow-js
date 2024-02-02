import { Path } from "../../deps.ts";
import { makeSubspaceKeypair } from "./crypto.ts";

export async function getSubspaces(size: number) {
  const subspaces = [];

  for (let i = 0; i < size; i++) {
    const keypair = await makeSubspaceKeypair();

    subspaces.push(keypair);
  }

  return subspaces;
}

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
