import { encodeBase64 } from "@std/encoding/base64";
import { HandleStore } from "./handle_store.ts";
import { WillowError } from "../errors.ts";
import FIFO from "@korkje/fifo";

/** Maps static tokens to handles and vice versa. `boundStaticTokens` needs to be sent to the peer. */
export class StaticTokenStore<StaticToken> {
  private byHandle = new HandleStore<StaticToken>();
  private byValue = new Map<string, bigint>();

  private valueEncoder: (value: StaticToken) => Uint8Array;
  private boundTokensQueue = new FIFO<StaticToken>();

  constructor(valueEncoder: (value: StaticToken) => Uint8Array) {
    this.valueEncoder = valueEncoder;
  }

  getByValue(
    value: StaticToken,
  ): bigint {
    const encoded = this.valueEncoder(value);
    const base64 = encodeBase64(encoded);

    const existingHandle = this.byValue.get(base64);

    if (existingHandle !== undefined) {
      const canUse = this.byHandle.canUse(existingHandle);

      if (!canUse) {
        throw new WillowError("Could not use a static token handle");
      }

      return existingHandle;
    }

    const newHandle = this.byHandle.bind(value);
    this.byValue.set(base64, newHandle);

    this.boundTokensQueue.push(value);

    return newHandle;
  }

  *boundStaticTokens() {
    for (const boundToken of this.boundTokensQueue) {
      yield boundToken;
    }
  }
}
