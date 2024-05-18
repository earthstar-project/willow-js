import { FIFO } from "fifo";

/** A FIFO queue which does not emit data until it has received guarantees that the data will be accepted.
 *
 * https://willowprotocol.org/specs/resource-control/index.html#resource_control_overview
 */
export class GuaranteedQueue {
  /** The guarantees received from the server. */
  guarantees = BigInt(0);

  /** Bytes awaiting guarantees before being sent. */
  private queue = new FIFO<Uint8Array>();

  /** Bytes which have been guaranteed to be accepted by the server. */
  private outgoingBytes = new FIFO<Uint8Array>();

  /** Add some bytes to the queue. */
  push(bytes: Uint8Array) {
    this.queue.push(bytes);

    this.useGuarantees();
  }

  /** Add guarantees received from the server. */
  addGuarantees(bytes: bigint) {
    this.guarantees += bytes;

    this.useGuarantees();
  }

  /** Received a plea from the server to shrink the buffer to a certain size.
   *
   * This implementation always absolves them.
   */
  plead(targetSize: bigint): bigint {
    const absolveAmount = this.guarantees - targetSize;

    this.guarantees -= absolveAmount;

    return absolveAmount;
  }

  /** Use available guarantees to send bytes. */
  private useGuarantees() {
    while (this.queue.length > 0) {
      const peekedHead = this.queue.peek();

      // Check if we have enough budget to send the current message.
      if (!peekedHead || peekedHead.byteLength > this.guarantees) {
        return;
      }

      // If so, send it out.
      const head = this.queue.shift()!;

      this.outgoingBytes.push(head);

      this.guarantees -= BigInt(head.byteLength);
    }
  }

  async *[Symbol.asyncIterator]() {
    for await (const bytes of this.outgoingBytes) {
      yield bytes;
    }
  }
}
