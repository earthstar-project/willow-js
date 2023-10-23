export async function makeNamespaceKeypair() {
  const { publicKey, privateKey } = await crypto.subtle.generateKey(
    {
      name: "ECDSA",
      namedCurve: "P-256",
    },
    true,
    ["sign", "verify"],
  );

  return {
    namespace: new Uint8Array(
      await window.crypto.subtle.exportKey("raw", publicKey),
    ),
    privateKey,
  };
}

export async function makeSubspaceKeypair() {
  const { publicKey, privateKey } = await crypto.subtle.generateKey(
    {
      name: "ECDSA",
      namedCurve: "P-256",
    },
    true,
    ["sign", "verify"],
  );

  return {
    subspace: new Uint8Array(
      await window.crypto.subtle.exportKey("raw", publicKey),
    ),
    privateKey,
  };
}

export function importPublicKey(raw: ArrayBuffer) {
  return crypto.subtle.importKey(
    "raw",
    raw,
    {
      name: "ECDSA",
      namedCurve: "P-256",
    },
    true,
    ["verify"],
  );
}
