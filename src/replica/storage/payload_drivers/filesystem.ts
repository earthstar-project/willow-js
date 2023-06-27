import { ValidationError, WillowError } from "../../../errors.ts";
import { Payload, ProtocolParameters } from "../../types.ts";
import { PayloadDriver } from "../types.ts";
import { join } from "https://deno.land/std@0.188.0/path/mod.ts";
import { ensureDir } from "https://deno.land/std@0.188.0/fs/ensure_dir.ts";
import { move } from "https://deno.land/std@0.188.0/fs/move.ts";
import { encodeBase32 } from "../../../../deps.ts";

/** Stores and retrieves payloads from the filesystem. */
export class PayloadDriverFilesystem<KeypairType> implements PayloadDriver {
  constructor(
    readonly path: string,
    readonly protocolParameters: ProtocolParameters<KeypairType>,
  ) {
  }

  async get(
    payloadHash: Uint8Array,
    opts?: { startOffset?: number | undefined } | undefined,
  ): Promise<Payload | undefined> {
    const base32Hash = encodeBase32(payloadHash);

    const filePath = join(this.path, base32Hash);

    try {
      await Deno.lstat(filePath);
    } catch {
      return undefined;
    }

    if (opts?.startOffset) {
      return {
        bytes: async () => {
          const stats = await Deno.stat(filePath);

          const bytes = new Uint8Array(stats.size - opts.startOffset!);

          const file = await Deno.open(
            filePath,
            { read: true },
          );

          await Deno.seek(
            file.rid,
            opts.startOffset!,
            Deno.SeekMode.Start,
          );

          await file.read(bytes);

          file.close();

          return bytes;
        },
        stream: new DenoFileReadable(filePath, opts?.startOffset),
      };
    }

    return {
      bytes: () => Deno.readFile(filePath),
      stream: new DenoFileReadable(filePath),
    };
  }

  async erase(payloadHash: Uint8Array): Promise<true | ValidationError> {
    const base32Hash = encodeBase32(payloadHash);
    const filePath = join(this.path, base32Hash);

    try {
      await Deno.remove(filePath);
      return true;
    } catch {
      return new ValidationError(
        `Attachment not found`,
      );
    }
  }

  async stage(
    payload: Uint8Array | ReadableStream<Uint8Array>,
  ): Promise<
    {
      hash: Uint8Array;
      length: number;
      commit: () => Promise<void>;
      reject: () => Promise<void>;
    }
  > {
    await this.ensureDir("staging");

    const tempKeyBuf = new Uint8Array(32);
    crypto.getRandomValues(tempKeyBuf);
    const tempKey = encodeBase32(tempKeyBuf);

    const stagingPath = join(this.path, "staging", tempKey);

    if (payload instanceof Uint8Array) {
      await Deno.writeFile(stagingPath, payload, { create: true });
      const hash = await this.protocolParameters.hash(payload);

      return {
        hash,
        length: payload.byteLength,
        commit: async () => {
          await this.ensureDir();
          const base32Hash = encodeBase32(hash);
          return move(stagingPath, join(this.path, base32Hash), {
            overwrite: true,
          });
        },
        reject: () => {
          return Deno.remove(stagingPath);
        },
      };
    }

    try {
      await Deno.truncate(stagingPath);
    } catch {
      // It's fine.
    }

    let hash = new Uint8Array();
    let length = 0;

    try {
      const file = await Deno.open(stagingPath, {
        createNew: true,
        write: true,
      });

      const [forHash, forStorage] = payload.tee();

      await Promise.all(
        [
          forStorage.pipeTo(file.writable),
          async () => {
            hash = await this.protocolParameters.hash(forHash);
          },
        ],
      );

      const stats = await file.stat();

      length = stats.size;
    } catch {
      throw new WillowError("Couldn't write data to the staging path.");
    }

    return {
      hash,
      length,
      commit: async () => {
        await this.ensureDir();

        const base32Hash = encodeBase32(hash);

        return move(stagingPath, join(this.path, base32Hash), {
          overwrite: true,
        });
      },
      reject: async () => {
        try {
          // We may have gotten an empty stream, in which case no file would have been written.
          await Deno.lstat(stagingPath);
          return Deno.remove(stagingPath);
        } catch {
          return Promise.resolve();
        }
      },
    };
  }

  private ensureDir(...args: string[]) {
    return ensureDir(join(this.path, ...args));
  }
}

class DenoFileReadable implements ReadableStream<Uint8Array> {
  private path: string;
  private offset: number | undefined;
  private stream: ReadableStream<Uint8Array> | undefined;

  constructor(path: string, offset?: number) {
    this.path = path;
    this.offset = offset;
  }

  private initiateStream() {
    const file = Deno.openSync(this.path);

    if (this.offset) {
      file.seek(this.offset, Deno.SeekMode.Start);
    }

    this.stream = file.readable;
  }

  get locked() {
    if (this.stream) {
      return this.stream.locked;
    }

    return false;
  }

  cancel() {
    if (this.stream) {
      return this.stream.cancel();
    }

    return Promise.resolve();
  }

  getReader(options: { mode: "byob" }): ReadableStreamBYOBReader;
  getReader(
    options?: { mode?: undefined } | undefined,
  ): ReadableStreamDefaultReader<Uint8Array>;
  getReader(
    options?: unknown,
  ): ReadableStreamBYOBReader | ReadableStreamDefaultReader<Uint8Array> {
    if (!this.stream) {
      this.initiateStream();
    }

    /* @ts-ignore */
    return this.stream!.getReader(options);
  }

  pipeThrough<T>(
    transform: {
      writable: WritableStream<Uint8Array>;
      readable: ReadableStream<T>;
    },
    options?: PipeOptions | undefined,
  ): ReadableStream<T> {
    if (!this.stream) {
      this.initiateStream();
    }

    return this.stream!.pipeThrough(transform, options);
  }

  pipeTo(
    dest: WritableStream<Uint8Array>,
    options?: PipeOptions | undefined,
  ): Promise<void> {
    if (!this.stream) {
      this.initiateStream();
    }

    return this.stream!.pipeTo(dest, options);
  }

  tee(): [ReadableStream<Uint8Array>, ReadableStream<Uint8Array>] {
    if (!this.stream) {
      this.initiateStream();
    }

    return this.stream!.tee();
  }

  [Symbol.asyncIterator]() {
    if (!this.stream) {
      this.initiateStream();
    }

    return this.stream![Symbol.asyncIterator]();
  }
}
