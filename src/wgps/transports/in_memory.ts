import { FIFO } from "../../../deps.ts";
import { IS_ALFIE, IS_BETTY, SyncRole, Transport } from "../types.ts";

export class TransportInMemory implements Transport {
  private incoming: FIFO<Uint8Array>;
  private outgoing: FIFO<Uint8Array>;

  role: SyncRole;

  constructor(
    syncRole: SyncRole,
    incoming: FIFO<Uint8Array>,
    outgoing: FIFO<Uint8Array>,
  ) {
    this.role = syncRole;
    this.incoming = incoming;
    this.outgoing = outgoing;
  }

  send(bytes: Uint8Array): Promise<void> {
    this.outgoing.push(bytes);

    return Promise.resolve();
  }

  async *[Symbol.asyncIterator]() {
    for await (const bytes of this.incoming) {
      yield bytes;
    }
  }
}

export function transportPairInMemory(): [
  TransportInMemory,
  TransportInMemory,
] {
  const alfie = new FIFO<Uint8Array>();
  const betty = new FIFO<Uint8Array>();

  const alfieTransport = new TransportInMemory(IS_ALFIE, alfie, betty);
  const bettyTransport = new TransportInMemory(IS_BETTY, betty, alfie);

  return [alfieTransport, bettyTransport];
}
