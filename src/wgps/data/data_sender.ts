import { FIFO } from "fifo";
import type { HandleStore } from "../handle_store.ts";
import { WillowError } from "../../errors.ts";
import type { Payload } from "../../store/types.ts";
import {
  type MsgDataReplyPayload,
  type MsgDataSendEntry,
  type MsgDataSendPayload,
  MsgKind,
} from "../types.ts";
import type { GetStoreFn } from "../wgps_messenger.ts";
import type { Entry } from "@earthstar/willow-utils";

type DataSendEntryPack<
  DynamicToken,
  NamespaceId,
  SubspaceId,
  PayloadDigest,
> = {
  entry: Entry<NamespaceId, SubspaceId, PayloadDigest>;
  offset: number;
  staticTokenHandle: bigint;
  dynamicToken: DynamicToken;
  payload: Payload;
};

type DataBindPayloadRequestPack = {
  handle: bigint;
  offset: number;
  payload: Payload;
};

export class DataSender<
  Prefingerprint,
  Fingerprint,
  AuthorisationToken,
  DynamicToken,
  NamespaceId,
  SubspaceId,
  PayloadDigest,
  AuthorisationOpts,
> {
  constructor(
    readonly opts: {
      handlesPayloadRequestsTheirs: HandleStore<{
        offset: bigint;
        entry: Entry<NamespaceId, SubspaceId, PayloadDigest>;
      }>;
      getStore: GetStoreFn<
        Prefingerprint,
        Fingerprint,
        AuthorisationToken,
        AuthorisationOpts,
        NamespaceId,
        SubspaceId,
        PayloadDigest
      >;
      transformPayload: (chunk: Uint8Array) => Uint8Array;
    },
  ) {
  }

  private internalQueue = new FIFO<
    | DataSendEntryPack<
      DynamicToken,
      NamespaceId,
      SubspaceId,
      PayloadDigest
    >
    | DataBindPayloadRequestPack
  >();

  async queueEntry(
    entry: Entry<NamespaceId, SubspaceId, PayloadDigest>,
    staticTokenHandle: bigint,
    dynamicToken: DynamicToken,
    offset: number,
  ) {
    const store = await this.opts.getStore(entry.namespaceId);

    const payload = await store.getPayload(entry);

    if (payload === undefined) {
      throw new WillowError(
        "Tried to queue data sending for entry which we do not have the payload for.",
      );
    }

    this.internalQueue.push({
      entry,
      dynamicToken,
      staticTokenHandle,
      payload,
      offset,
    });
  }

  async queuePayloadRequest(handle: bigint) {
    const payloadRequest = await this.opts.handlesPayloadRequestsTheirs
      .getEventually(handle);

    const store = await this.opts.getStore(payloadRequest.entry.namespaceId);

    const payload = await store.getPayload(payloadRequest.entry);

    if (payload === undefined) {
      throw new WillowError(
        "Tried to queue data sending for entry which we do not have the payload for.",
      );
    }

    this.internalQueue.push({
      handle,
      offset: Number(payloadRequest.offset),
      payload,
    });
  }

  async *messages(): AsyncIterable<
    | MsgDataSendEntry<DynamicToken, NamespaceId, SubspaceId, PayloadDigest>
    | MsgDataReplyPayload
    | MsgDataSendPayload
  > {
    for await (const pack of this.internalQueue) {
      if ("entry" in pack) {
        yield {
          kind: MsgKind.DataSendEntry,
          entry: pack.entry,
          offset: BigInt(pack.offset),
          dynamicToken: pack.dynamicToken,
          staticTokenHandle: pack.staticTokenHandle,
        };
      } else {
        yield {
          kind: MsgKind.DataReplyPayload,
          handle: pack.handle,
        };
      }

      const payloadIterator = await pack.payload.stream(pack.offset);

      for await (const chunk of payloadIterator) {
        const transformed = this.opts.transformPayload(chunk);

        yield {
          kind: MsgKind.DataSendPayload,
          amount: BigInt(transformed.byteLength),
          bytes: transformed,
        };
      }
    }
  }
}
