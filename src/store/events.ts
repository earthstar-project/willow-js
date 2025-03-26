import type { Entry } from "@earthstar/willow-utils";
import type { Payload } from "./types.ts";

export const StoreEvents = {
  EntryIngest: "entryingest",
  EntryPayloadSet: "entrypayloadset",
  EntryRemove: "entryremove",
  PayloadIngest: "payloadingest",
  PayloadRemove: "payloadremove",
} as const;

export type StoreEventsMap<
  NamespacePublicKey,
  SubspacePublicKey,
  PayloadDigest,
  AuthorisationToken,
> = {
  [StoreEvents.EntryPayloadSet]: EntryPayloadSetEvent<
    NamespacePublicKey,
    SubspacePublicKey,
    PayloadDigest,
    AuthorisationToken
  >;
  [StoreEvents.EntryIngest]: EntryIngestEvent<
    NamespacePublicKey,
    SubspacePublicKey,
    PayloadDigest,
    AuthorisationToken
  >;
  [StoreEvents.PayloadIngest]: PayloadIngestEvent<
    NamespacePublicKey,
    SubspacePublicKey,
    PayloadDigest,
    AuthorisationToken
  >;
  [StoreEvents.EntryRemove]: EntryRemoveEvent<
    NamespacePublicKey,
    SubspacePublicKey,
    PayloadDigest,
    AuthorisationToken
  >;
  [StoreEvents.PayloadRemove]: PayloadRemoveEvent<
    NamespacePublicKey,
    SubspacePublicKey,
    PayloadDigest,
    AuthorisationToken
  >;
};

/** Emitted after a {@linkcode Store} creates a new {@linkcode Entry} for a given payload. */
export class EntryPayloadSetEvent<
  NamespacePublicKey,
  SubspacePublicKey,
  PayloadDigest,
  AuthorisationToken,
> extends CustomEvent<{
  entry: Entry<NamespacePublicKey, SubspacePublicKey, PayloadDigest>;
  authToken: AuthorisationToken;
  payload: Payload;
}> {
  constructor(
    entry: Entry<NamespacePublicKey, SubspacePublicKey, PayloadDigest>,
    authToken: AuthorisationToken,
    payload: Payload,
  ) {
    super(StoreEvents.EntryPayloadSet, {
      detail: {
        entry,
        authToken,
        payload,
      },
    });
  }
}

/** Emitted after a {@linkcode Store} attempts to ingest an {@linkcode Entry}. */
export class EntryIngestEvent<
  NamespacePublicKey,
  SubspacePublicKey,
  PayloadDigest,
  AuthorisationToken,
> extends CustomEvent<
  {
    entry: Entry<NamespacePublicKey, SubspacePublicKey, PayloadDigest>;
    authToken: AuthorisationToken;
  }
> {
  constructor(
    entry: Entry<NamespacePublicKey, SubspacePublicKey, PayloadDigest>,
    authToken: AuthorisationToken,
  ) {
    super(StoreEvents.EntryIngest, {
      detail: {
        entry,
        authToken,
      },
    });
  }
}

/** Emitted after a {@linkcode Store} attempts to ingest a payload. */
export class PayloadIngestEvent<
  NamespacePublicKey,
  SubspacePublicKey,
  PayloadDigest,
  AuthorisationToken,
> extends CustomEvent<{
  entry: Entry<NamespacePublicKey, SubspacePublicKey, PayloadDigest>;
  authToken: AuthorisationToken;
  payload: Payload;
}> {
  constructor(
    entry: Entry<NamespacePublicKey, SubspacePublicKey, PayloadDigest>,
    authToken: AuthorisationToken,
    payload: Payload,
  ) {
    super(StoreEvents.PayloadIngest, {
      detail: {
        entry,
        authToken,
        payload,
      },
    });
  }
}

/** Emitted after a {@linkcode Store} removes an {@linkcode Entry}. */
export class EntryRemoveEvent<
  NamespacePublicKey,
  SubspacePublicKey,
  PayloadDigest,
  AuthorisationToken,
> extends CustomEvent<
  {
    removed: Entry<NamespacePublicKey, SubspacePublicKey, PayloadDigest>;
    removedBy: {
      entry: Entry<NamespacePublicKey, SubspacePublicKey, PayloadDigest>;
      authToken: AuthorisationToken;
    };
  }
> {
  constructor(
    removed: Entry<NamespacePublicKey, SubspacePublicKey, PayloadDigest>,
    removedBy: {
      entry: Entry<NamespacePublicKey, SubspacePublicKey, PayloadDigest>;
      authToken: AuthorisationToken;
    },
  ) {
    super(StoreEvents.EntryRemove, {
      detail: {
        removed,
        removedBy,
      },
    });
  }
}

/** Emitted after a {@linkcode Store} removes a payload. */
export class PayloadRemoveEvent<
  NamespacePublicKey,
  SubspacePublicKey,
  PayloadDigest,
  AuthorisationToken,
> extends CustomEvent<
  {
    removedBy: {
      entry: Entry<NamespacePublicKey, SubspacePublicKey, PayloadDigest>;
      authToken: AuthorisationToken;
    };
  }
> {
  constructor(
    removedBy: {
      entry: Entry<NamespacePublicKey, SubspacePublicKey, PayloadDigest>;
      authToken: AuthorisationToken;
    },
  ) {
    super(StoreEvents.PayloadRemove, {
      detail: {
        removedBy,
      },
    });
  }
}
