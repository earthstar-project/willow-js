import { concat } from "@std/bytes";
import type { Entry } from "@earthstar/willow-utils";
import { FIFO } from "@korkje/fifo";
import { WgpsMessageValidationError } from "../../errors.ts";
import { onAsyncIterate } from "../util.ts";
import type { GetStoreFn } from "../wgps_messenger.ts";

const CANCELLATION = Symbol("cancellation");

// This class can handle both the payload sending procedures for payloads sent via reconciliation AND data channels. It would probably be better to split them up.
export class PayloadIngester<
  Prefingerprint,
  Fingerprint,
  AuthorisationToken,
  NamespaceId,
  SubspaceId,
  PayloadDigest,
  AuthorisationOpts,
> {
  private currentIngestion: {
    kind: "active" | "cancelled";
    fifo: FIFO<Uint8Array | typeof CANCELLATION>;
    receivedLength: bigint;
    entry: Entry<NamespaceId, SubspaceId, PayloadDigest>;
  } | {
    kind: "pending";
    entry: Entry<NamespaceId, SubspaceId, PayloadDigest>;
  } | {
    kind: "uninitialised";
  } = {
    kind: "uninitialised",
  };

  private events = new FIFO<
    Uint8Array | {
      entry: Entry<NamespaceId, SubspaceId, PayloadDigest>;
    } | typeof CANCELLATION
  >();
  private processReceivedPayload: (
    bytes: Uint8Array,
    entryLength: bigint,
  ) => Uint8Array;
  private entryToRequestPayloadFor:
    | Entry<NamespaceId, SubspaceId, PayloadDigest>
    | null = null;

  private id: string;

  constructor(opts: {
    getStore: GetStoreFn<
      Prefingerprint,
      Fingerprint,
      AuthorisationToken,
      AuthorisationOpts,
      NamespaceId,
      SubspaceId,
      PayloadDigest
    >;
    processReceivedPayload: (
      bytes: Uint8Array,
      entryLength: bigint,
    ) => Uint8Array;
    id: string;
  }) {
    this.processReceivedPayload = opts.processReceivedPayload;

    this.id = opts.id;

    onAsyncIterate(this.events, async (event) => {
      if (event === CANCELLATION) {
        if (this.currentIngestion.kind === "active") {
          this.currentIngestion.fifo.push(CANCELLATION);
          this.currentIngestion.kind = "cancelled";
        }
      } else if ("entry" in event) {
        if (this.currentIngestion.kind === "active") {
          this.currentIngestion.fifo.push(CANCELLATION);
        }

        this.currentIngestion = {
          kind: "pending",
          entry: event.entry,
        };
      } else {
        if (this.currentIngestion.kind === "active") {
          const transformed = this.processReceivedPayload(
            event,
            this.currentIngestion.entry.payloadLength,
          );

          this.currentIngestion.receivedLength += BigInt(
            transformed.byteLength,
          );
          this.currentIngestion.fifo.push(transformed);
        } else if (this.currentIngestion.kind === "pending") {
          const { entry } = this.currentIngestion;

          const store = await opts.getStore(entry.namespaceId);

          const fifo = new FIFO<Uint8Array | typeof CANCELLATION>();

          const transformed = this.processReceivedPayload(
            event,
            this.currentIngestion.entry.payloadLength,
          );

          fifo.push(transformed);

          store.ingestPayload({
            path: entry.path,
            subspace: entry.subspaceId,
            timestamp: entry.timestamp,
          }, new CancellableIngestion(fifo), false, 0, this.id);

          this.currentIngestion = {
            kind: "active",
            receivedLength: BigInt(transformed.byteLength),
            entry,
            fifo,
          };
        }
      }
    });
  }

  target(
    entry: Entry<NamespaceId, SubspaceId, PayloadDigest>,
    requestIfImmediatelyTerminated?: boolean,
  ) {
    this.events.push({ entry });

    if (requestIfImmediatelyTerminated) {
      this.entryToRequestPayloadFor = entry;
    }
  }

  push(bytes: Uint8Array, end: boolean) {
    this.events.push(bytes);

    if (end) {
      this.events.push(CANCELLATION);
    }

    this.entryToRequestPayloadFor = null;
  }

  // Returns the entry to request a payload for or null
  terminate(): Entry<NamespaceId, SubspaceId, PayloadDigest> | null {
    this.events.push(CANCELLATION);

    return this.entryToRequestPayloadFor;
  }
}

class CancellableIngestion {
  constructor(
    readonly iterable: AsyncIterable<Uint8Array | typeof CANCELLATION>,
  ) {}

  async *[Symbol.asyncIterator]() {
    for await (const event of this.iterable) {
      let bytes = new Uint8Array();

      if (event === CANCELLATION) {
        break;
      } else {
        bytes = concat([bytes, event]);

        yield event;
      }
    }
  }
}
