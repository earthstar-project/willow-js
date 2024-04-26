import { deferred, FIFO } from "../../../deps.ts";
import { SyncRole, Transport } from "../types.ts";

const SOCKET_CLOSED = Symbol("socket_close");

export class TransportWebsocket implements Transport {
  private socketOpened = deferred<true>();
  private received = new FIFO<ArrayBuffer | typeof SOCKET_CLOSED>();
  private closed = false;

  constructor(readonly role: SyncRole, private readonly socket: WebSocket) {
    socket.binaryType = "arraybuffer";

    if (socket.readyState === socket.OPEN) {
      this.socketOpened.resolve();
    }

    socket.onopen = () => {
      this.socketOpened.resolve();
    };

    this.socket.onmessage = (event) => {
      if (event.data instanceof ArrayBuffer) {
        this.received.push(event.data);
      }
    };

    this.socket.onclose = () => {
      this.received.push(SOCKET_CLOSED);
    };
  }

  async send(bytes: Uint8Array): Promise<void> {
    await this.socketOpened;

    if (this.socket.readyState === this.socket.OPEN) {
      this.socket.send(bytes);
    }
  }

  async *[Symbol.asyncIterator]() {
    for await (const msg of this.received) {
      if (this.closed || msg === SOCKET_CLOSED) {
        break;
      }

      yield new Uint8Array(msg);
    }
  }

  close() {
    this.closed = true;
    this.socket.close();
  }

  get isClosed() {
    return this.closed;
  }
}
