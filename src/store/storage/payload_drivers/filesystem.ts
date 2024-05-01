import { ValidationError, WillowError } from "../../../errors.ts";
import { Payload, PayloadScheme } from "../../types.ts";
import { PayloadDriver } from "../types.ts";
import { join, resolve } from "https://deno.land/std@0.188.0/path/mod.ts";
import { ensureDir } from "https://deno.land/std@0.188.0/fs/ensure_dir.ts";
import { move } from "https://deno.land/std@0.188.0/fs/move.ts";
import { encodeBase32 } from "../../../../deps.ts";

/** Stores and retrieves payloads from the filesystem. */
export class PayloadDriverFilesystem<PayloadDigest>
  implements PayloadDriver<PayloadDigest> {
  constructor(
    readonly path: string,
    readonly payloadScheme: PayloadScheme<PayloadDigest>,
  ) {
  }

  private getKey(hash: PayloadDigest): string {
    const encoded = this.payloadScheme.encode(hash);
    return encodeBase32(encoded);
  }

  private getPayload(filePath: string): Payload {
    return {
      bytes: async (offset) => {
        if (!offset) {
          return Deno.readFile(filePath);
        }

        const stats = await Deno.stat(filePath);
        const bytes = new Uint8Array(stats.size - offset!);

        const file = await Deno.open(
          filePath,
          { read: true },
        );

        file.seek(offset!, Deno.SeekMode.Start);

        await file.read(bytes);

        file.close();

        return bytes;
      },
      stream: async (offset) => {
        const file = await Deno.open(
          filePath,
          { read: true },
        );

        if (offset) {
          await file.seek(offset, Deno.SeekMode.Start);
        }

        return file.readable;
      },
      length: async () => {
        const stats = await Deno.stat(filePath);
        return BigInt(stats.size);
      },
    };
  }

  async get(
    payloadHash: PayloadDigest,
  ): Promise<Payload | undefined> {
    const filePath = join(this.path, this.getKey(payloadHash));

    try {
      await Deno.lstat(filePath);
    } catch {
      return undefined;
    }

    return this.getPayload(filePath);
  }

  async erase(payloadHash: PayloadDigest): Promise<true | ValidationError> {
    const filePath = join(this.path, this.getKey(payloadHash));

    try {
      await Deno.remove(filePath);
      return true;
    } catch {
      return new ValidationError(
        `Attachment not found`,
      );
    }
  }

  async set(
    payload: Uint8Array | AsyncIterable<Uint8Array>,
  ): Promise<{ digest: PayloadDigest; payload: Payload; length: bigint }> {
    if (payload instanceof Uint8Array) {
      const digest = await this.payloadScheme.fromBytes(payload);

      await this.ensureDir();

      const filePath = join(this.path, this.getKey(digest));

      await Deno.writeFile(filePath, payload, { create: true });

      return {
        digest: digest,
        length: BigInt(payload.byteLength),
        payload: this.getPayload(filePath),
      };
    }

    try {
      await this.ensureDir("staging");

      const tempKeyBuf = new Uint8Array(32);
      crypto.getRandomValues(tempKeyBuf);
      const tempKey = encodeBase32(tempKeyBuf);

      const stagingPath = join(this.path, "staging", tempKey);

      try {
        await Deno.truncate(stagingPath);
      } catch {
        // It's fine.
      }

      const file = await Deno.open(stagingPath, {
        createNew: true,
        write: true,
      });

      const writer = file.writable.getWriter();

      for await (const chunk of payload) {
        await writer.write(chunk);
      }

      await file.seek(0, Deno.SeekMode.Start);

      const digest = await this.payloadScheme.fromBytes(file.readable);

      const stats = await file.stat();

      await this.ensureDir();

      const filePath = join(this.path, this.getKey(digest));
      await move(stagingPath, filePath, {
        overwrite: true,
      });

      return {
        digest,
        length: BigInt(stats.size),
        payload: this.getPayload(filePath),
      };
    } catch {
      throw new WillowError("Couldn't write data to the staging path.");
    }
  }

  async receive(
    opts: {
      payload: AsyncIterable<Uint8Array> | Uint8Array;
      offset: number;
      knownLength: bigint;
      knownDigest: PayloadDigest;
    },
  ): Promise<{ digest: PayloadDigest; length: bigint }> {
    try {
      await this.ensureDir();

      const key = this.getKey(opts.knownDigest);

      const filePath = join(this.path, key);

      const file = await Deno.open(
        filePath,
        opts.offset === 0
          ? {
            create: true,
            write: true,
            truncate: true,
            read: true,
          }
          : {
            create: true,
            append: true,
            read: true,
          },
      );

      if (opts.offset > 0) {
        await file.truncate(opts.offset);
        await file.seek(opts.offset, Deno.SeekMode.Start);
      }

      const writer = file.writable.getWriter();

      let receivedLength = BigInt(opts.offset);

      if (opts.payload instanceof Uint8Array) {
        receivedLength += BigInt(opts.payload.byteLength);

        await writer.write(opts.payload);
      } else {
        for await (const chunk of opts.payload) {
          await writer.write(chunk);

          receivedLength += BigInt(chunk.byteLength);

          if (receivedLength >= opts.knownLength) {
            break;
          }
        }
      }

      await file.seek(0, Deno.SeekMode.Start);

      const digest = await this.payloadScheme.fromBytes(file.readable);

      return {
        digest,
        length: BigInt(receivedLength),
      };
    } catch (err) {
      throw new WillowError("Payload driver error: " + err);
    }
  }

  async length(payloadHash: PayloadDigest): Promise<bigint> {
    const filePath = join(this.path, this.getKey(payloadHash));

    try {
      const stats = await Deno.lstat(filePath);
      return BigInt(stats.size);
    } catch {
      return BigInt(0);
    }
  }

  private ensureDir(...args: string[]) {
    return ensureDir(join(this.path, ...args));
  }
}
