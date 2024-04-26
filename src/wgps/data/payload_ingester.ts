import { Entry, FIFO } from "../../../deps.ts";
import { WgpsMessageValidationError } from "../../errors.ts";
import { onAsyncIterate } from "../util.ts";
import { GetStoreFn } from "../wgps_messenger.ts";

const CANCELLATION = Symbol("cancellation");

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
  private events = new FIFO<
    Uint8Array | {
      entry: Entry<NamespaceId, SubspaceId, PayloadDigest>;
    } | typeof CANCELLATION
  >();

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
  }) {
    onAsyncIterate(this.events, (event) => {
      if (event === CANCELLATION) {
        this.currentIngestion.push(CANCELLATION);
      } else if ("entry" in event) {
        this.currentIngestion.push(CANCELLATION);
        this.currentIngestion = new FIFO();

        const store = opts.getStore(event.entry.namespaceId);

        store.ingestPayload({
          path: event.entry.path,
          subspace: event.entry.subspaceId,
          timestamp: event.entry.timestamp,
        }, new CancellableIngestion(this.currentIngestion)).then(
          (ingestEvent) => {
            if (ingestEvent.kind === "failure") {
              throw new WgpsMessageValidationError(
                "Ingestion failed: " + ingestEvent.reason,
              );
            }
          },
        );
      } else {
        this.currentIngestion.push(event);
      }
    });
  }

  target(entry: Entry<NamespaceId, SubspaceId, PayloadDigest>) {
    this.events.push({ entry });
  }

  push(bytes: Uint8Array, end: boolean) {
    this.events.push(bytes);

    if (end) {
      this.events.push(CANCELLATION);
    }
  }
}

class CancellableIngestion {
  constructor(
    readonly iterable: AsyncIterable<Uint8Array | typeof CANCELLATION>,
  ) {}

  async *[Symbol.asyncIterator]() {
    for await (const event of this.iterable) {
      if (event === CANCELLATION) {
        break;
      } else {
        yield event;
      }
    }
  }
}
