import { concat, deferred } from "../../deps.ts";
import { SyncRole, Transport } from "./types.ts";

/** A transport which only emits encoded messages, following the initial max payload size and commitment.
 *
 * _Doesn't_ send our own max payload size and commitment.
 */
export class ReadyTransport implements Transport {
  private transport: Transport;
  private challengeLength: number;

  role: SyncRole;

  /** The maximum payload size derived from the first byte sent over the transport. */
  maximumPayloadSize = deferred<bigint>();
  /** The received commitment sent after the first byte over the transport */
  receivedCommitment = deferred<Uint8Array>();

  constructor(opts: {
    transport: Transport;
    challengeLength: 1 | 2 | 4 | 8;
  }) {
    this.role = opts.transport.role;
    this.transport = opts.transport;
    this.challengeLength = opts.challengeLength;
  }

  send(bytes: Uint8Array): Promise<void> {
    return this.transport.send(bytes);
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

        if (rest.byteLength < this.challengeLength) {
          this.commitmentAcc = rest;
        } else if (rest.byteLength === this.challengeLength) {
          this.receivedCommitment.resolve(rest);
        } else {
          this.receivedCommitment.resolve(rest.slice(0, this.challengeLength));
          yield rest.slice(this.challengeLength);
        }

        continue;
      }

      if (this.receivedCommitment.state === "pending") {
        const combined = concat(this.commitmentAcc, bytes);

        if (combined.byteLength === this.challengeLength) {
          this.receivedCommitment.resolve(combined);
        } else if (combined.byteLength < this.challengeLength) {
          this.commitmentAcc = combined;
        } else {
          this.receivedCommitment.resolve(
            combined.slice(0, this.challengeLength),
          );

          yield combined.slice(this.challengeLength);
        }
      }
    }
  }
}
