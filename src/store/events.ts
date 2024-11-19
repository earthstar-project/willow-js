import type { Entry } from "@earthstar/willow-utils";
import type { Payload } from "./types.ts";

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
    super("entrypayloadset", {
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
    externalSourceId?: string;
  }
> {
  constructor(
    entry: Entry<NamespacePublicKey, SubspacePublicKey, PayloadDigest>,
    authToken: AuthorisationToken,
    externalSourceId?: string,
  ) {
    super("entryingest", {
      detail: {
        entry,
        authToken,
        externalSourceId,
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
  externalSourceId?: string;
}> {
  constructor(
    entry: Entry<NamespacePublicKey, SubspacePublicKey, PayloadDigest>,
    authToken: AuthorisationToken,
    payload: Payload,
    externalSourceId?: string,
  ) {
    super("payloadingest", {
      detail: {
        entry,
        authToken,
        payload,
        externalSourceId,
      },
    });
  }
}

/** Emitted after a {@linkcode Store} attempts to ingest a payload, but already had it. */
export class PayloadNoOpEvent<
  NamespacePublicKey,
  SubspacePublicKey,
  PayloadDigest,
  AuthorisationToken,
> extends CustomEvent<{
  entry: Entry<NamespacePublicKey, SubspacePublicKey, PayloadDigest>;
  externalSourceId?: string;
}> {
  constructor(
    entry: Entry<NamespacePublicKey, SubspacePublicKey, PayloadDigest>,
    externalSourceId?: string,
  ) {
    super("payloadnoop", {
      detail: {
        entry,
        externalSourceId,
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
    super("entryremove", {
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
    super("payloadRemove", {
      detail: {
        removedBy,
      },
    });
  }
}
