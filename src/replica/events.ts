import { SignedEntry } from "../entries/types.ts";
import { Payload } from "./types.ts";

export class EntryPayloadSetEvent extends CustomEvent<{
  signed: SignedEntry,
  payload: Payload
}> {
  constructor(signed: SignedEntry, payload: Payload) {
    super("entrypayloadset", {
      detail: {
        signed,
        payload,
      },
    });
  }
}

export class EntryIngestEvent extends CustomEvent<{signed: SignedEntry}> {
  constructor(signed: SignedEntry) {
    super("entryingest", {
      detail: {
        signed,
      },
    });
  }
}

export class PayloadIngestEvent extends CustomEvent<{
  signed: SignedEntry,
  payload: Payload
}> {
  constructor(signed: SignedEntry, payload: Payload) {
    super("payloadingest", {
      detail: {
        signed,
        payload,
      },
    });
  }
}

export class EntryRemoveEvent extends CustomEvent<{removed: SignedEntry}>  {
  constructor(removed: SignedEntry) {
    super("entryremove", {
      detail: {
        removed,
      },
    });
  }
}
