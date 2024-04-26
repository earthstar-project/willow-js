import { concat, deferred } from "../../deps.ts";
import { SyncRole, Transport } from "./types.ts";

/** A transport which only emits encoded messages, following the initial max payload size and commitment.
 *
 * _Doesn't_ send our own max payload size and commitment.
 */
export class ReadyTransport implements Transport {
  private transport: Transport;
  private challengeHashLength: number;

  role: SyncRole;

  /** The maximum payload size derived from the first byte sent over the transport. */
  maximumPayloadSize = deferred<bigint>();
  /** The received commitment sent after the first byte over the transport */
  receivedCommitment = deferred<Uint8Array>();

  constructor(opts: {
    transport: Transport;
    challengeHashLength: number;
  }) {
    this.role = opts.transport.role;
    this.transport = opts.transport;
    this.challengeHashLength = opts.challengeHashLength;
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
        this.maximumPayloadSize.state === "fulfilled" &&
        this.receivedCommitment.state === "fulfilled"
      ) {
        yield bytes;
      }

      if (this.maximumPayloadSize.state === "pending") {
        const view = new DataView(bytes.buffer);

        const power = view.getUint8(0);

        this.maximumPayloadSize.resolve(BigInt(2) ** BigInt(power));

        const rest = bytes.slice(1);

        if (rest.byteLength < this.challengeHashLength) {
          this.commitmentAcc = rest;
        } else if (rest.byteLength === this.challengeHashLength) {
          this.receivedCommitment.resolve(rest);
        } else {
          this.receivedCommitment.resolve(
            rest.slice(0, this.challengeHashLength),
          );
          yield rest.slice(this.challengeHashLength);
        }

        continue;
      }

      if (this.receivedCommitment.state === "pending") {
        const combined = concat(this.commitmentAcc, bytes);

        if (combined.byteLength === this.challengeHashLength) {
          this.receivedCommitment.resolve(combined);
        } else if (combined.byteLength < this.challengeHashLength) {
          this.commitmentAcc = combined;
        } else {
          this.receivedCommitment.resolve(
            combined.slice(0, this.challengeHashLength),
          );

          yield combined.slice(this.challengeHashLength);
        }
      }
    }
  }
}
