import { concat } from "@std/bytes";
import type { Entry } from "@earthstar/willow-utils";
import { FIFO } from "fifo";
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
  private currentIngestion = new FIFO<Uint8Array | typeof CANCELLATION>();
  private currentEntry:
    | Entry<NamespaceId, SubspaceId, PayloadDigest>
    | undefined;
  private currentlyReceivedLength = 0n;

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
  }) {
    this.processReceivedPayload = opts.processReceivedPayload;

    onAsyncIterate(this.events, async (event) => {
      if (event === CANCELLATION) {
        this.currentIngestion.push(CANCELLATION);
      } else if ("entry" in event) {
        this.currentIngestion.push(CANCELLATION);
        this.currentIngestion = new FIFO();

        const store = await opts.getStore(event.entry.namespaceId);

        this.currentEntry = event.entry;
        this.currentlyReceivedLength = 0n;

        store.ingestPayload({
          path: event.entry.path,
          subspace: event.entry.subspaceId,
          timestamp: event.entry.timestamp,
        }, new CancellableIngestion(this.currentIngestion)).then(
          (ingestEvent) => {
            if (
              ingestEvent.kind === "failure" &&
              this.currentlyReceivedLength === event.entry.payloadLength
            ) {
              throw new WgpsMessageValidationError(
                "Ingestion failed: " + ingestEvent.reason,
              );
            }
          },
        );
      } else {
        const transformed = this.processReceivedPayload(
          event,
          this.currentEntry!.payloadLength,
        );

        this.currentlyReceivedLength += BigInt(transformed.byteLength);

        this.currentIngestion.push(transformed);
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
