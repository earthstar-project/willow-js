import type {
  AccessControlScheme,
  AuthorisationTokenScheme,
  ReadAuthorisation,
  SubspaceCapScheme,
} from "../wgps/types.ts";
import { crypto } from "@std/crypto";
import type {
  AuthorisationScheme,
  FingerprintScheme,
  NamespaceScheme,
  PayloadScheme,
  SubspaceScheme,
} from "../store/types.ts";
import type { PaiScheme } from "../wgps/pai/types.ts";
import { x25519 } from "npm:@noble/curves/ed25519";
import { isFragmentTriple } from "../wgps/pai/pai_finder.ts";
import { concat } from "@std/bytes";
import {
  ANY_SUBSPACE,
  bigintToBytes,
  decodeAreaInArea,
  decodePath,
  decodeStreamAreaInArea,
  decodeStreamPath,
  encodeAreaInArea,
  encodeEntry,
  encodePath,
  OPEN_END,
  orderBytes,
  type Path,
  type PathScheme,
  type Range,
} from "@earthstar/willow-utils";
import { equals as equalsBytes } from "@std/bytes";

// Namespace

export enum TestNamespace {
  Family,
  Project,
  Bookclub,
  Gardening,
  Vibes,
}
export const testSchemeNamespace: NamespaceScheme<TestNamespace> = {
  defaultNamespaceId: TestNamespace.Family,
  encode: (v) => {
    return new Uint8Array([v]);
  },
  decode: (v) => {
    const [firstByte] = v;

    switch (firstByte) {
      case 0:
        return TestNamespace.Family;
      case 1:
        return TestNamespace.Project;
      case 2:
        return TestNamespace.Bookclub;
      case 3:
        return TestNamespace.Gardening;
      case 4:
        return TestNamespace.Vibes;
    }

    throw new Error("Badly encoded test namespace");
  },
  encodedLength: () => 1,
  isEqual: (a, b) => {
    return a === b;
  },
  decodeStream: async (bytes) => {
    await bytes.nextAbsolute(1);

    const [firstByte] = bytes.array;

    bytes.prune(1);

    switch (firstByte) {
      case 0:
        return TestNamespace.Family;
      case 1:
        return TestNamespace.Project;
      case 2:
        return TestNamespace.Bookclub;
      case 3:
        return TestNamespace.Gardening;
      case 4:
        return TestNamespace.Vibes;
    }

    throw new Error("Badly encoded test namespace");
  },
};

// Subspace

export enum TestSubspace {
  Alfie,
  Betty,
  Gemma,
  Dalton,
  Epson,
  Phoebe,
  Muriarty,
}

export const testSchemeSubspace: SubspaceScheme<TestSubspace> = {
  encode: (v) => {
    return new Uint8Array([v]);
  },
  decode: (v) => {
    const [firstByte] = v;

    switch (firstByte) {
      case 0:
        return TestSubspace.Alfie;
      case 1:
        return TestSubspace.Betty;
      case 2:
        return TestSubspace.Gemma;
      case 3:
        return TestSubspace.Dalton;
      case 4:
        return TestSubspace.Epson;
      case 5:
        return TestSubspace.Phoebe;
      case 6:
        return TestSubspace.Muriarty;
    }

    throw new Error(`Badly encoded test subspace (${firstByte})`);
  },
  encodedLength: () => 1,
  decodeStream: async (bytes) => {
    await bytes.nextAbsolute(1);

    const [firstByte] = bytes.array;

    bytes.prune(1);

    switch (firstByte) {
      case 0:
        return TestSubspace.Alfie;
      case 1:
        return TestSubspace.Betty;
      case 2:
        return TestSubspace.Gemma;
      case 3:
        return TestSubspace.Dalton;
      case 4:
        return TestSubspace.Epson;
      case 5:
        return TestSubspace.Phoebe;
      case 6:
        return TestSubspace.Muriarty;
    }

    throw new Error("Badly encoded test subspace");
  },
  minimalSubspaceId: TestSubspace.Alfie,
  order: (a, b) => {
    if (a < b) {
      return -1;
    } else if (a > b) {
      return 1;
    }

    return 0;
  },
  successor: (v) => {
    const next = v + 1;

    if (next > 6) {
      return null;
    }

    return next;
  },
};

export type TestReadCap = {
  namespace: TestNamespace;
  subspace: TestSubspace | typeof ANY_SUBSPACE;
  time: Range<bigint>;
  path: Path;
  receiver: TestSubspace;
};

export type TestSubspaceReadCap = {
  namespace: TestNamespace;
  path: Path;
  time: Range<bigint>;
  receiver: TestSubspace;
};

export type TestReadAuth = ReadAuthorisation<
  TestReadCap,
  TestSubspaceReadCap
>;

// Subspace caps

export const testSchemeSubspaceCap: SubspaceCapScheme<
  TestSubspaceReadCap,
  TestSubspace,
  Uint8Array,
  TestSubspace,
  TestNamespace
> = {
  getNamespace: (cap) => cap.namespace,
  getReceiver: (cap) => cap.receiver,
  getSecretKey: (receiver) => Promise.resolve(receiver),
  isValidCap: () => Promise.resolve(true),
  signatures: {
    sign: async (_pubkey, secret, msg) => {
      const hash = await crypto.subtle.digest("SHA-256", msg);

      return concat([new Uint8Array([secret]), new Uint8Array(hash)]);
    },
    verify: async (key, sig, bytestring) => {
      const hash = await crypto.subtle.digest("SHA-256", bytestring);

      const expected = concat([new Uint8Array([key]), new Uint8Array(hash)]);
      return equalsBytes(sig, expected);
    },
  },
  encodings: {
    syncSubspaceSignature: {
      encode: (sig) => sig,
      decode: (enc) => {
        return enc.subarray(0, 33);
      },
      encodedLength: () => 33,
      decodeStream: async (bytes) => {
        await bytes.nextAbsolute(33);

        const sig = bytes.array.slice(0, 33);

        bytes.prune(33);

        return sig;
      },
    },
    subspaceCapability: {
      encode: (cap) => {
        return concat(
          [
            new Uint8Array([cap.namespace, cap.receiver]),
            cap.time.end === OPEN_END
              ? concat([new Uint8Array([0]), bigintToBytes(cap.time.start)])
              : concat(
                [
                  new Uint8Array([1]),
                  bigintToBytes(cap.time.start),
                  bigintToBytes(cap.time.end),
                ],
              ),
            encodePath(testSchemePath, cap.path),
          ],
        );
      },
      decode: (enc) => {
        const [namespace, receiver, isOpenByte] = enc;

        const isOpen = isOpenByte === 0;

        let time: Range<bigint>;

        const dataView = new DataView(enc.buffer);

        if (isOpen) {
          time = {
            start: dataView.getBigUint64(4),
            end: OPEN_END,
          };
        } else {
          time = {
            start: dataView.getBigUint64(4),
            end: dataView.getBigUint64(4 + 8),
          };
        }

        const path = decodePath(
          testSchemePath,
          enc.subarray(isOpen ? 3 + 8 : 3 + 8 + 8),
        );

        return {
          namespace,
          receiver,
          path,
          time,
        };
      },
      encodedLength: (cap) => {
        return 2 + encodePath(testSchemePath, cap.path).length;
      },
      decodeStream: async (bytes) => {
        await bytes.nextAbsolute(2);

        const [namespace, receiver] = bytes.array;

        bytes.prune(2);

        await bytes.nextAbsolute(1);

        const [isOpenByte] = bytes.array;

        let time: Range<bigint>;

        const dataView = new DataView(
          bytes.array.buffer,
          bytes.array.byteOffset,
        );

        if (isOpenByte === 0) {
          await bytes.nextAbsolute(8);

          time = {
            start: dataView.getBigUint64(1),
            end: OPEN_END,
          };

          bytes.prune(1 + 8);
        } else {
          await bytes.nextAbsolute(8 + 8);

          time = {
            start: dataView.getBigUint64(1),
            end: dataView.getBigUint64(1 + 8),
          };

          bytes.prune(1 + 8 + 8);
        }

        const path = await decodeStreamPath(testSchemePath, bytes);

        return {
          namespace,
          receiver,
          path,
          time,
        };
      },
    },
  },
};

export const testSchemePath: PathScheme = {
  maxPathLength: 64,
  maxComponentCount: 4,
  maxComponentLength: 16,
};

export const testSchemePayload: PayloadScheme<ArrayBuffer> = {
  encode(hash) {
    return new Uint8Array(hash);
  },
  decode(bytes) {
    return bytes.subarray(0, 32);
  },
  encodedLength() {
    return 32;
  },
  async decodeStream(bytes) {
    await bytes.nextAbsolute(32);

    const digest = bytes.array.subarray(0, 32);

    bytes.prune(32);

    return digest;
  },

  async fromBytes(bytes) {
    return new Uint8Array(await crypto.subtle.digest("SHA-256", bytes));
  },
  order(a, b) {
    return orderBytes(new Uint8Array(a), new Uint8Array(b)) as
      | 1
      | 0
      | -1;
  },
  defaultDigest: new Uint8Array(32),
};

export function addBytes(a: Uint8Array, b: Uint8Array, length: number) {
  const bytes = new Uint8Array(length);

  let carried = 0;

  for (let i = 0; i < length; i++) {
    const byteA = a[a.byteLength - 1 - i] || 0;
    const byteB = b[b.byteLength - 1 - i] || 0;

    const added = carried + byteA + byteB;

    carried = added >> 8;

    bytes.set([added % 256], length - 1 - i);
  }

  return bytes;
}

export const testSchemeFingerprint: FingerprintScheme<
  TestNamespace,
  TestSubspace,
  Uint8Array,
  Uint8Array,
  Uint8Array
> = {
  neutral: new Uint8Array(0),
  neutralFinalised: new Uint8Array(32),
  async fingerprintSingleton(lengthy) {
    const encodedEntry = encodeEntry({
      encodeNamespace: testSchemeNamespace.encode,
      encodeSubspace: testSchemeSubspace.encode,
      pathScheme: testSchemePath,
      encodePayload: testSchemePayload.encode,
    }, lengthy.entry);

    const lengthEnc = bigintToBytes(lengthy.available);

    return new Uint8Array(
      await crypto.subtle.digest("SHA-256", concat([encodedEntry, lengthEnc])),
    );
  },
  fingerprintCombine(a, b) {
    return addBytes(a, b, 64);
  },
  fingerprintFinalise: async (pre) => {
    return new Uint8Array(
      await crypto.subtle.digest("SHA-256", pre),
    );
  },
  isEqual: (a, b) => {
    return orderBytes(a, b) === 0;
  },
  encoding: {
    encode: (fp) => fp,
    decode: (encoded) => encoded.slice(0, 32),
    encodedLength: () => 32,
    decodeStream: async (bytes) => {
      await bytes.nextAbsolute(32);

      const fp = bytes.array.slice(0, 32);

      bytes.prune(32);

      return fp;
    },
  },
};

export const testSchemeAuthorisation: AuthorisationScheme<
  TestNamespace,
  TestSubspace,
  ArrayBuffer,
  // The 'secret' is just the ID of the subspace.
  TestSubspace,
  Uint8Array
> = {
  async authorise(entry, secretKey) {
    const encodedEntry = encodeEntry({
      encodeNamespace: testSchemeNamespace.encode,
      encodeSubspace: testSchemeSubspace.encode,
      pathScheme: testSchemePath,
      encodePayload: testSchemePayload.encode,
    }, entry);

    const hash = await crypto.subtle.digest("SHA-256", encodedEntry);

    return concat([new Uint8Array([secretKey]), new Uint8Array(hash)]);
  },
  async isAuthorisedWrite(entry, token) {
    const encodedEntry = encodeEntry({
      encodeNamespace: testSchemeNamespace.encode,
      encodeSubspace: testSchemeSubspace.encode,
      pathScheme: testSchemePath,
      encodePayload: testSchemePayload.encode,
    }, entry);

    const hash = await crypto.subtle.digest("SHA-256", encodedEntry);
    const expected = concat(
      [new Uint8Array([entry.subspaceId]), new Uint8Array(hash)],
    );

    return equalsBytes(token, expected);
  },
  tokenEncoding: {
    encode: (ab) => new Uint8Array(ab),
    decode: (bytes) => bytes.subarray(0, 33),
    encodedLength: (ab) => ab.byteLength,
    decodeStream: async (bytes) => {
      await bytes.nextAbsolute(33);

      const decoded = bytes.array.slice(0, 33);

      bytes.prune(33);

      return decoded;
    },
  },
};

export const testSchemePai: PaiScheme<
  TestReadCap,
  Uint8Array,
  Uint8Array,
  TestNamespace,
  TestSubspace
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
      const namespaceEnc = testSchemeNamespace.encode(namespace);
      const pathEncoded = encodePath(testSchemePath, path);

      const bytes = concat(
        [new Uint8Array([pairOrTripleByte]), namespaceEnc, pathEncoded],
      );

      const digest = await crypto.subtle.digest("SHA-256", bytes);

      return new Uint8Array(digest);
    }

    const [namespace, subspace, path] = fragment;

    const pairOrTripleByte = 0;
    const namespaceEnc = testSchemeNamespace.encode(namespace);
    const subspaceEnc = testSchemeSubspace.encode(subspace);

    const pathEncoded = encodePath(testSchemePath, path);

    const bytes = concat(
      [
        new Uint8Array([pairOrTripleByte]),
        namespaceEnc,
        subspaceEnc,
        pathEncoded,
      ],
    );

    const digest = await crypto.subtle.digest("SHA-256", bytes);

    return new Uint8Array(digest);
  },
  groupMemberEncoding: {
    encode(group) {
      return group;
    },
    decode(encoded) {
      return encoded.subarray(0, 32);
    },
    encodedLength() {
      return 32;
    },
    async decodeStream(bytes) {
      await bytes.nextAbsolute(32);

      const group = bytes.array.slice(0, 32);

      bytes.prune(32);

      return group;
    },
  },
};

export const testSchemeAccessControl: AccessControlScheme<
  TestReadCap,
  TestSubspace,
  Uint8Array,
  TestSubspace,
  TestNamespace,
  TestSubspace
> = {
  getGrantedNamespace: (cap) => cap.namespace,
  getGrantedArea: (cap) => {
    return {
      includedSubspaceId: cap.subspace,
      pathPrefix: cap.path,
      timeRange: cap.time,
    };
  },
  getReceiver: (cap) => cap.receiver,
  getSecretKey: (receiver) => Promise.resolve(receiver),
  isValidCap: () => Promise.resolve(true),
  encodings: {
    readCapability: {
      encode: (cap, privy) => {
        const capGrantedArea = testSchemeAccessControl.getGrantedArea(cap);

        const areaInAreaEnc = encodeAreaInArea(
          {
            encodeSubspace: testSchemeSubspace.encode,
            orderSubspace: testSchemeSubspace.order,
            pathScheme: testSchemePath,
          },
          capGrantedArea,
          privy.outer,
        );

        const areaInAreaLength = bigintToBytes(
          BigInt(areaInAreaEnc.byteLength),
        );

        return concat(
          [new Uint8Array([cap.receiver]), areaInAreaLength, areaInAreaEnc],
        );
      },
      encodedLength: (cap, privy) => {
        const capGrantedArea = testSchemeAccessControl.getGrantedArea(cap);

        const areaInAreaEnc = encodeAreaInArea(
          {
            encodeSubspace: testSchemeSubspace.encode,
            orderSubspace: testSchemeSubspace.order,
            pathScheme: testSchemePath,
          },
          capGrantedArea,
          privy.outer,
        );

        return 1 + 8 + areaInAreaEnc.byteLength;
      },
      decode: (cap, privy) => {
        const [receiver] = cap;

        const view = new DataView(cap.buffer);

        const aInALength = view.getBigUint64(1);

        const area = decodeAreaInArea(
          {
            pathScheme: testSchemePath,
            decodeSubspaceId: testSchemeSubspace.decode,
          },
          cap.subarray(1, Number(aInALength) + 1),
          privy.outer,
        );

        return {
          namespace: privy.namespace,
          receiver,
          path: area.pathPrefix,
          subspace: area.includedSubspaceId,
          time: area.timeRange,
        };
      },
      decodeStream: async (bytes, privy) => {
        await bytes.nextAbsolute(1 + 8);

        const [receiver] = bytes.array;

        bytes.prune(1 + 8);

        const area = await decodeStreamAreaInArea(
          {
            pathScheme: testSchemePath,
            decodeStreamSubspace: testSchemeSubspace.decodeStream,
          },
          bytes,
          privy.outer,
        );

        return {
          namespace: privy.namespace,
          receiver,
          path: area.pathPrefix,
          subspace: area.includedSubspaceId,
          time: area.timeRange,
        };
      },
    },
    syncSignature: {
      encode: (sig) => sig,
      decode: (sig) => sig.subarray(0, 33),
      encodedLength: () => 33,
      decodeStream: async (bytes) => {
        await bytes.nextAbsolute(33);

        const sig = bytes.array.slice(0, 33);

        bytes.prune(33);

        return sig;
      },
    },
  },
  signatures: {
    sign: async (_pubkey, key, bytestring) => {
      const hash = await crypto.subtle.digest("SHA-256", bytestring);

      return concat([new Uint8Array([key]), new Uint8Array(hash)]);
    },
    verify: async (pubKey, sig, bytestring) => {
      const hash = await crypto.subtle.digest("SHA-256", bytestring);

      return equalsBytes(
        concat(
          [new Uint8Array([pubKey]), new Uint8Array(hash)],
        ),
        sig,
      );
    },
  },
};

export const testSchemeAuthorisationToken: AuthorisationTokenScheme<
  Uint8Array,
  TestSubspace,
  Uint8Array
> = {
  decomposeAuthToken: (authToken) => {
    const staticToken = authToken[0];

    const dynamicToken = authToken.subarray(1);

    return [staticToken, dynamicToken];
  },
  recomposeAuthToken: (staticToken, dynamicToken) => {
    const bytes = new Uint8Array(33);

    bytes.set([staticToken], 0);
    bytes.set(dynamicToken, 1);

    return bytes;
  },
  encodings: {
    dynamicToken: {
      encode(hash) {
        return new Uint8Array(hash);
      },
      decode(bytes) {
        return bytes.subarray(0, 32);
      },
      encodedLength() {
        return 32;
      },
      async decodeStream(bytes) {
        await bytes.nextAbsolute(32);

        const digest = bytes.array.subarray(0, 32);

        bytes.prune(32);

        return digest;
      },
    },
    staticToken: {
      encode: (subspace) => {
        return new Uint8Array([subspace]);
      },
      encodedLength: () => 1,
      decode: (encoded) => {
        return encoded[0];
      },
      decodeStream: async (bytes) => {
        await bytes.nextAbsolute(1);

        const [subspace] = bytes.array;

        bytes.prune(1);

        return subspace;
      },
    },
  },
};
