import { Entry, FIFO } from "../../../deps.ts";
import { HandleStore } from "../handle_store.ts";
import { WillowError } from "../../errors.ts";
import { Payload } from "../../store/types.ts";
import {
  MSG_DATA_REPLY_PAYLOAD,
  MSG_DATA_SEND_ENTRY,
  MSG_DATA_SEND_PAYLOAD,
  MsgDataReplyPayload,
  MsgDataSendEntry,
  MsgDataSendPayload,
} from "../types.ts";
import { GetStoreFn } from "../wgps_messenger.ts";

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
    const store = this.opts.getStore(entry.namespaceId);

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

    const store = this.opts.getStore(payloadRequest.entry.namespaceId);

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
          kind: MSG_DATA_SEND_ENTRY,
          entry: pack.entry,
          offset: BigInt(pack.offset),
          dynamicToken: pack.dynamicToken,
          staticTokenHandle: pack.staticTokenHandle,
        };
      } else {
        yield {
          kind: MSG_DATA_REPLY_PAYLOAD,
          handle: pack.handle,
        };
      }

      const payloadIterator = await pack.payload.stream(pack.offset);

      for await (const chunk of payloadIterator) {
        yield {
          kind: MSG_DATA_SEND_PAYLOAD,
          amount: BigInt(chunk.byteLength),
          bytes: chunk,
        };
      }
    }
  }
}
