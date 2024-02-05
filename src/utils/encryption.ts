import { Path } from "../../deps.ts";

export async function encryptPath<EncryptionKey>(opts: {
  key: EncryptionKey;
  /** Output must be of size maximum component length, input ... just a path component or MORE? */
  encryptFn: (key: EncryptionKey, bytes: Uint8Array) => Promise<Uint8Array>;
  deriveKey: (
    key: EncryptionKey,
    component: Uint8Array,
  ) => Promise<EncryptionKey>;
}, path: Path): Promise<[Path, EncryptionKey]> {
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
    /** Output must be of size maximum component length, input ... just a path component or MORE? */
    encryptFn: (key: EncryptionKey, bytes: Uint8Array) => Promise<Uint8Array>;
  },
  component: Uint8Array,
): Promise<Uint8Array> {
  return opts.encryptFn(opts.key, component);
}

export async function decryptPath<EncryptionKey>(opts: {
  key: EncryptionKey;
  /** Output must be of size maximum component length, input ... just a path component or MORE? */
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
    /** Output must be of size maximum component length, input ... just a path component or MORE? */
    decryptFn: (key: EncryptionKey, bytes: Uint8Array) => Promise<Uint8Array>;
  },
  component: Uint8Array,
): Promise<Uint8Array> {
  return opts.decryptFn(opts.key, component);
}
