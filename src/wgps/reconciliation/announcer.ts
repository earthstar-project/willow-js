import { encodeBase64, FIFO, Range3d } from "../../../deps.ts";
import { WillowError } from "../../errors.ts";
import { Store } from "../../store/store.ts";
import { LengthyEntry, PayloadScheme } from "../../store/types.ts";
import { HandleStore } from "../handle_store.ts";
import { AuthorisationTokenScheme } from "../types.ts";

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
  };
  // Then send many ReconciliationSendEntry
  entries: {
    lengthyEntry: LengthyEntry<NamespaceId, SubspaceId, PayloadDigest>;
    staticTokenHandle: bigint;
    dynamicToken: DynamicToken;
  }[];
};

export class Announcer<
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

  private async getStaticTokenHandle(
    staticToken: StaticToken,
  ): Promise<{ handle: bigint; alreadyExisted: boolean }> {
    const encoded = this.authorisationTokenScheme.encodings.staticToken.encode(
      staticToken,
    );
    const digest = await this.payloadScheme.fromBytes(encoded);
    const encodedDigest = this.payloadScheme.encode(digest);
    const base64 = encodeBase64(encodedDigest);

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
      Fingerprint
    >;
    namespace: NamespaceId;
    range: Range3d<SubspaceId>;
    wantResponse: boolean;
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
      } = await this.getStaticTokenHandle(staticToken);

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
