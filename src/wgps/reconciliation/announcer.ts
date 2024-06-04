import type { Range3d } from "@earthstar/willow-utils";
import { encodeBase64 } from "@std/encoding/base64";
import { FIFO } from "@korkje/fifo";
import { WillowError } from "../../errors.ts";
import type { Store } from "../../store/store.ts";
import type { LengthyEntry, PayloadScheme } from "../../store/types.ts";
import type { HandleStore } from "../handle_store.ts";
import type { AuthorisationTokenScheme, COVERS_NONE } from "../types.ts";

export type AnnouncerOpts<
  AuthorisationToken,
  StaticToken,
  DynamicToken,
  PayloadDigest,
> = {
  authorisationTokenScheme: AuthorisationTokenScheme<
    AuthorisationToken,
    StaticToken,
    DynamicToken
  >;
  payloadScheme: PayloadScheme<PayloadDigest>;
  staticTokenHandleStoreOurs: HandleStore<StaticToken>;
};

type AnnouncementPack<
  StaticToken,
  DynamicToken,
  NamespaceId,
  SubspaceId,
  PayloadDigest,
> = {
  // Send these first in SetupBindStaticToken messages
  staticTokenBinds: StaticToken[];
  // Then send a ReconciliationAnnounceEntries
  announcement: {
    range: Range3d<SubspaceId>;
    count: number;
    wantResponse: boolean;
    senderHandle: bigint;
    receiverHandle: bigint;
    covers: bigint | typeof COVERS_NONE;
  };
  // Then send many ReconciliationSendEntry
  entries: {
    lengthyEntry: LengthyEntry<NamespaceId, SubspaceId, PayloadDigest>;
    staticTokenHandle: bigint;
    dynamicToken: DynamicToken;
  }[];
};

export class Announcer<
  Prefingerprint,
  Fingerprint,
  AuthorisationToken,
  StaticToken,
  DynamicToken,
  NamespaceId,
  SubspaceId,
  PayloadDigest,
  AuthorisationOpts,
> {
  private authorisationTokenScheme: AuthorisationTokenScheme<
    AuthorisationToken,
    StaticToken,
    DynamicToken
  >;
  private payloadScheme: PayloadScheme<PayloadDigest>;
  private staticTokenHandleStoreOurs: HandleStore<StaticToken>;

  private staticTokenHandleMap = new Map<string, bigint>();

  private announcementPackQueue = new FIFO<
    AnnouncementPack<
      StaticToken,
      DynamicToken,
      NamespaceId,
      SubspaceId,
      PayloadDigest
    >
  >();

  constructor(
    opts: AnnouncerOpts<
      AuthorisationToken,
      StaticToken,
      DynamicToken,
      PayloadDigest
    >,
  ) {
    this.authorisationTokenScheme = opts.authorisationTokenScheme;
    this.payloadScheme = opts.payloadScheme;
    this.staticTokenHandleStoreOurs = opts.staticTokenHandleStoreOurs;
  }

  private getStaticTokenHandle(
    staticToken: StaticToken,
  ): { handle: bigint; alreadyExisted: boolean } {
    const encoded = this.authorisationTokenScheme.encodings.staticToken.encode(
      staticToken,
    );
    const base64 = encodeBase64(encoded);

    const existingHandle = this.staticTokenHandleMap.get(base64);

    if (existingHandle !== undefined) {
      const canUse = this.staticTokenHandleStoreOurs.canUse(existingHandle);

      if (!canUse) {
        throw new WillowError("Could not use a static token handle");
      }

      return { handle: existingHandle, alreadyExisted: true };
    }

    const newHandle = this.staticTokenHandleStoreOurs.bind(staticToken);
    this.staticTokenHandleMap.set(base64, newHandle);

    return { handle: newHandle, alreadyExisted: false };
  }

  async queueAnnounce(announcement: {
    senderHandle: bigint;
    receiverHandle: bigint;
    store: Store<
      NamespaceId,
      SubspaceId,
      PayloadDigest,
      AuthorisationOpts,
      AuthorisationToken,
      Prefingerprint,
      Fingerprint
    >;
    namespace: NamespaceId;
    range: Range3d<SubspaceId>;
    wantResponse: boolean;
    covers: bigint | typeof COVERS_NONE;
  }) {
    // Queue announcement message.

    const staticTokenBinds: StaticToken[] = [];
    const entries: {
      lengthyEntry: LengthyEntry<NamespaceId, SubspaceId, PayloadDigest>;
      staticTokenHandle: bigint;
      dynamicToken: DynamicToken;
    }[] = [];

    for await (
      const [
        entry,
        payload,
        authToken,
      ] of announcement.store.queryRange(announcement.range, "oldest")
    ) {
      const [staticToken, dynamicToken] = this.authorisationTokenScheme
        .decomposeAuthToken(authToken);

      const {
        handle: staticTokenHandle,
        alreadyExisted: staticTokenHandleAlreadyExisted,
      } = this.getStaticTokenHandle(staticToken);

      if (!staticTokenHandleAlreadyExisted) {
        staticTokenBinds.push(staticToken);
      }

      entries.push({
        lengthyEntry: {
          entry,
          available: payload ? await payload.length() : 0n,
        },
        dynamicToken,
        staticTokenHandle,
      });
    }

    this.announcementPackQueue.push({
      staticTokenBinds,
      announcement: {
        count: entries.length,
        range: announcement.range,
        receiverHandle: announcement.receiverHandle,
        senderHandle: announcement.senderHandle,
        wantResponse: announcement.wantResponse,
        covers: announcement.covers,
      },
      entries,
    });
  }

  async *announcementPacks() {
    for await (const pack of this.announcementPackQueue) {
      yield pack;
    }
  }
}
