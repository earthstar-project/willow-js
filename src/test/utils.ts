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
