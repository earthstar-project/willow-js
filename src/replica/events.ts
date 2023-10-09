import { Entry } from "../entries/types.ts";
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
> extends CustomEvent<{
  entry: Entry<NamespacePublicKey, SubspacePublicKey, PayloadDigest>;
  payload: Payload;
}> {
  constructor(
    entry: Entry<NamespacePublicKey, SubspacePublicKey, PayloadDigest>,
    payload: Payload,
  ) {
    super("payloadingest", {
      detail: {
        entry,
        payload,
      },
    });
  }
}

export class EntryRemoveEvent<
  NamespacePublicKey,
  SubspacePublicKey,
  PayloadDigest,
> extends CustomEvent<
  { removed: Entry<NamespacePublicKey, SubspacePublicKey, PayloadDigest> }
> {
  constructor(
    removed: Entry<NamespacePublicKey, SubspacePublicKey, PayloadDigest>,
  ) {
    super("entryremove", {
      detail: {
        removed,
      },
    });
  }
}
