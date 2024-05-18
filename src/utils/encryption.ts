import type { Path } from "@earthstar/willow-utils";

/** Encrypt a `Path`.
 *
 * https://willowprotocol.org/specs/e2e/index.html#e2e_paths
 */
export async function encryptPath<EncryptionKey>(
  opts: {
    key: EncryptionKey;
    encryptFn: (key: EncryptionKey, bytes: Uint8Array) => Promise<Uint8Array>;
    deriveKey: (
      key: EncryptionKey,
      component: Uint8Array,
    ) => Promise<EncryptionKey>;
  },
  path: Path,
): Promise<[Path, EncryptionKey]> {
  if (path.length === 0) {
    return [[], opts.key];
  }

  if (path.length === 1) {
    const encryptedComponent = await encryptComponent(
      { key: opts.key, encryptFn: opts.encryptFn },
      path[0],
    );

    const derivedKey = await opts.deriveKey(opts.key, path[0]);

    return [[encryptedComponent], derivedKey];
  }

  const [encryptedSoFar, derivedKey] = await encryptPath(
    opts,
    path.slice(0, path.length - 1),
  );

  const encryptedComponentLast = await encryptComponent({
    key: derivedKey,
    encryptFn: opts.encryptFn,
  }, path[path.length - 1]);

  const finalDerivedKey = await opts.deriveKey(
    derivedKey,
    path[path.length - 1],
  );

  return [[...encryptedSoFar, encryptedComponentLast], finalDerivedKey];
}

export function encryptComponent<EncryptionKey>(
  opts: {
    key: EncryptionKey;
    encryptFn: (key: EncryptionKey, bytes: Uint8Array) => Promise<Uint8Array>;
  },
  component: Uint8Array,
): Promise<Uint8Array> {
  return opts.encryptFn(opts.key, component);
}

/** Decrypt a `Path`.
 *
 * https://willowprotocol.org/specs/e2e/index.html#e2e_paths
 */
export async function decryptPath<EncryptionKey>(opts: {
  key: EncryptionKey;
  decryptFn: (key: EncryptionKey, bytes: Uint8Array) => Promise<Uint8Array>;
  deriveKey: (
    key: EncryptionKey,
    component: Uint8Array,
  ) => Promise<EncryptionKey>;
}, path: Path): Promise<[Path, EncryptionKey]> {
  if (path.length === 0) {
    return [[], opts.key];
  }

  if (path.length === 1) {
    const decryptedComponent = await decryptComponent(
      { key: opts.key, decryptFn: opts.decryptFn },
      path[0],
    );

    const derivedKey = await opts.deriveKey(opts.key, decryptedComponent);

    return [[decryptedComponent], derivedKey];
  }

  const [decryptedSoFar, derivedKey] = await decryptPath(
    opts,
    path.slice(0, path.length - 1),
  );

  const decryptedComponentLast = await decryptComponent({
    key: derivedKey,
    decryptFn: opts.decryptFn,
  }, path[path.length - 1]);

  const finalDerivedKey = await opts.deriveKey(
    derivedKey,
    decryptedComponentLast,
  );

  return [[...decryptedSoFar, decryptedComponentLast], finalDerivedKey];
}

export function decryptComponent<EncryptionKey>(
  opts: {
    key: EncryptionKey;
    decryptFn: (key: EncryptionKey, bytes: Uint8Array) => Promise<Uint8Array>;
  },
  component: Uint8Array,
): Promise<Uint8Array> {
  return opts.decryptFn(opts.key, component);
}

export function encryptPathAtOffset<EncryptionKey>(
  opts: {
    key: EncryptionKey;
    encryptFn: (key: EncryptionKey, bytes: Uint8Array) => Promise<Uint8Array>;
    deriveKey: (
      key: EncryptionKey,
      component: Uint8Array,
    ) => Promise<EncryptionKey>;
    offset: number;
  },
  path: Path,
): Promise<[Path, EncryptionKey]> {
  const offsetPath = path.slice(0, opts.offset);

  return encryptPath(opts, offsetPath);
}

export function decryptPathAtOffset<EncryptionKey>(opts: {
  key: EncryptionKey;
  decryptFn: (key: EncryptionKey, bytes: Uint8Array) => Promise<Uint8Array>;
  deriveKey: (
    key: EncryptionKey,
    component: Uint8Array,
  ) => Promise<EncryptionKey>;
  offset: number;
}, path: Path): Promise<[Path, EncryptionKey]> {
  const offsetPath = path.slice(0, opts.offset);

  return decryptPath(opts, offsetPath);
}
