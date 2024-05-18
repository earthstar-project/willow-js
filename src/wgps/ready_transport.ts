import { concat } from "@std/bytes";
import type { SyncRole, Transport } from "./types.ts";

/** A transport which only emits encoded messages, following the initial max payload size and commitment.
 *
 * _Doesn't_ send our own max payload size and commitment.
 */
export class ReadyTransport implements Transport {
  private transport: Transport;
  private challengeHashLength: number;

  role: SyncRole;

  private _maximumPayloadSize = Promise.withResolvers<bigint>();
  private _receivedCommitment = Promise.withResolvers<Uint8Array>();

  /** The maximum payload size derived from the first byte sent over the transport. */
  get maximumPayloadSize() {
    return this._maximumPayloadSize.promise;
  }

  /** The received commitment sent after the first byte over the transport */
  get receivedCommitment() {
    return this._receivedCommitment.promise;
  }

  private isMaximumPayloadSizeFulfilled = false;
  private isReceivedCommitmentFulfilled = false;

  constructor(opts: {
    transport: Transport;
    challengeHashLength: number;
  }) {
    this.role = opts.transport.role;
    this.transport = opts.transport;
    this.challengeHashLength = opts.challengeHashLength;

    this._maximumPayloadSize.promise.then(() => {
      this.isMaximumPayloadSizeFulfilled = true;
    });

    this._receivedCommitment.promise.then(() => {
      this.isReceivedCommitmentFulfilled = true;
    });
  }

  send(bytes: Uint8Array): Promise<void> {
    return this.transport.send(bytes);
  }

  close(): void {
    return this.transport.close();
  }

  get isClosed() {
    return this.transport.isClosed;
  }

  private commitmentAcc: Uint8Array = new Uint8Array();

  async *[Symbol.asyncIterator]() {
    for await (const bytes of this.transport) {
      if (
        this.isMaximumPayloadSizeFulfilled &&
        this.isReceivedCommitmentFulfilled
      ) {
        yield bytes;
      }

      if (!this.isMaximumPayloadSizeFulfilled) {
        const view = new DataView(bytes.buffer);

        const power = view.getUint8(0);

        this._maximumPayloadSize.resolve(BigInt(2) ** BigInt(power));

        const rest = bytes.slice(1);

        if (rest.byteLength < this.challengeHashLength) {
          this.commitmentAcc = rest;
        } else if (rest.byteLength === this.challengeHashLength) {
          this._receivedCommitment.resolve(rest);
        } else {
          this._receivedCommitment.resolve(
            rest.slice(0, this.challengeHashLength),
          );
          yield rest.slice(this.challengeHashLength);
        }

        continue;
      }

      if (!this.isReceivedCommitmentFulfilled) {
        const combined = concat([this.commitmentAcc, bytes]);

        if (combined.byteLength === this.challengeHashLength) {
          this._receivedCommitment.resolve(combined);
        } else if (combined.byteLength < this.challengeHashLength) {
          this.commitmentAcc = combined;
        } else {
          this._receivedCommitment.resolve(
            combined.slice(0, this.challengeHashLength),
          );

          yield combined.slice(this.challengeHashLength);
        }
      }
    }
  }
}
