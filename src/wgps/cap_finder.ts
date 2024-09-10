import type { NamespaceScheme, SubspaceScheme } from "../store/types.ts";
import type { HandleStore } from "./handle_store.ts";
import type { AccessControlScheme } from "./types.ts";
import { WillowError } from "../errors.ts";
import { encodeBase64 } from "@std/encoding/base64";
import {
  type Entry,
  entryPosition,
  isIncludedArea,
} from "@earthstar/willow-utils";

/** Helps you get capabilities for given entries */
export class CapFinder<
  ReadCapability,
  Receiver,
  SyncSignature,
  ReceiverSecretKey,
  NamespaceId,
  SubspaceId,
  PayloadDigest,
> {
  private namespaceMap = new Map<string, Set<bigint>>();

  constructor(
    readonly opts: {
      handleStoreOurs: HandleStore<ReadCapability>;
      schemes: {
        namespace: NamespaceScheme<NamespaceId>;
        subspace: SubspaceScheme<SubspaceId>;
        accessControl: AccessControlScheme<
          ReadCapability,
          Receiver,
          SyncSignature,
          ReceiverSecretKey,
          NamespaceId,
          SubspaceId
        >;
      };
    },
  ) {
  }

  private getNamespaceKey(namespace: NamespaceId): string {
    const encoded = this.opts.schemes.namespace.encode(namespace);
    return encodeBase64(encoded);
  }

  addCap(handle: bigint) {
    const cap = this.opts.handleStoreOurs.get(handle);

    if (!cap) {
      throw new WillowError("Can't dereference capability handle");
    }

    const namespace = this.opts.schemes.accessControl.getGrantedNamespace(cap);

    const key = this.getNamespaceKey(namespace);

    const res = this.namespaceMap.get(key);

    if (res) {
      res.add(handle);
      return;
    }

    this.namespaceMap.set(key, new Set([handle]));
  }

  findCapHandle(
    entry: Entry<NamespaceId, SubspaceId, PayloadDigest>,
  ): bigint | undefined {
    const key = this.getNamespaceKey(entry.namespaceId);

    const set = this.namespaceMap.get(key);

    if (!set) {
      return undefined;
    }

    const entryPos = entryPosition(entry);

    for (const handle of set) {
      const cap = this.opts.handleStoreOurs.get(handle);

      if (!cap) {
        throw new WillowError("Can't dereference capability handle");
      }

      const grantedArea = this.opts.schemes.accessControl.getGrantedArea(cap);

      const isInArea = isIncludedArea(
        this.opts.schemes.subspace.order,
        grantedArea,
        entryPos,
      );

      if (isInArea) {
        return handle;
      }
    }

    return undefined;
  }
}
