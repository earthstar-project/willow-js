import { Entry } from "../../deps.ts";
import { Payload } from "./types.ts";

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
    super("entryingest", {
      detail: {
        entry,
        authToken,
      },
    });
  }
}

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
    super("payloadingest", {
      detail: {
        entry,
        authToken,
        payload,
      },
    });
  }
}

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
