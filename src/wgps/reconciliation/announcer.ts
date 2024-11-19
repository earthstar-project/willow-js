import type { Range3d } from "@earthstar/willow-utils";
import { FIFO } from "@korkje/fifo";
import { WillowError } from "../../errors.ts";
import type { Store } from "../../store/store.ts";
import type { LengthyEntry, Payload, PayloadScheme } from "../../store/types.ts";
import type { AuthorisationTokenScheme, COVERS_NONE } from "../types.ts";
import type { StaticTokenStore } from "../static_token_store.ts";

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
  staticTokenStore: StaticTokenStore<StaticToken>,
};

type AnnouncementPack<
  StaticToken,
  DynamicToken,
  NamespaceId,
  SubspaceId,
  PayloadDigest,
> = {
  // Send ReconciliationAnnounceEntries
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
    payload: Payload | null;
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
  private staticTokenStore: StaticTokenStore<StaticToken>;

  private announcementPackQueue = new FIFO<
    AnnouncementPack<
      StaticToken,
      DynamicToken,
      NamespaceId,
      SubspaceId,
      PayloadDigest
    >
  >();

  otherMaximumPayloadSize: bigint = BigInt(0);

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
    this.staticTokenStore = opts.staticTokenStore;
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

    const entries: {
      lengthyEntry: LengthyEntry<NamespaceId, SubspaceId, PayloadDigest>;
      staticTokenHandle: bigint;
      dynamicToken: DynamicToken;
      payload: Payload | null;
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

      const staticTokenHandle = this.staticTokenStore.getByValue(staticToken);

      let available = payload ? await payload.length() : 0n;

      // If the payload is less-equal to the other peer's maximum payload size,
      // we can immediatly send it along with the entry.
      const sendPayload = payload &&
        entry.payloadLength <= this.otherMaximumPayloadSize &&
        entry.payloadLength == available;

      entries.push({
        lengthyEntry: {
          entry,
          available,
        },
        dynamicToken,
        staticTokenHandle,
        payload: sendPayload ? payload : null,
      });
    }

    this.announcementPackQueue.push({
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
