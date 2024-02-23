import { deferred, orderBytes } from "../../deps.ts";
import { ValidationError, WgpsMessageValidationError } from "../errors.ts";
import { decodeMessages } from "./decoding/decode_messages.ts";
import { MessageEncoder } from "./encoding/message_encoder.ts";
import { ReadyTransport } from "./ready_transport.ts";
import { HandleStore } from "./handle_store.ts";
import { PaiFinder } from "./pai/pai_finder.ts";
import {
  IS_ALFIE,
  MSG_COMMITMENT_REVEAL,
  SyncEncodings,
  SyncMessage,
  Transport,
} from "./types.ts";
import { Intersection, PaiScheme } from "./pai/types.ts";

export type WgpsMessengerOpts<
  NamespaceId,
  SubspaceId,
  PsiGroup,
  Scalar,
  SubspaceCapability,
  SyncSubspaceSignature,
  ReadCapability,
> = {
  transport: Transport;

  /** Sets the maximum payload size for this peer, which is 2 to the power of the given number.
   *
   * The given power must be a natural number lesser than or equal to 64. */
  maxPayloadSizePower: number;

  challengeLength: number;
  challengeHashLength: number;

  challengeHash: (bytes: Uint8Array) => Promise<Uint8Array>;

  paiScheme: PaiScheme<
    NamespaceId,
    SubspaceId,
    PsiGroup,
    Scalar,
    ReadCapability
  >;

  encodings: SyncEncodings<PsiGroup, SubspaceCapability, SyncSubspaceSignature>;
};

/** Coordinates a complete WGPS synchronisation session. */
export class WgpsMessenger<
  NamespaceId,
  SubspaceId,
  PsiGroup,
  Scalar,
  SubspaceCapability,
  SyncSubspaceSignature,
  ReadCapability,
  SyncSignature,
> {
  private transport: ReadyTransport;
  private encoder: MessageEncoder<
    PsiGroup,
    SubspaceCapability,
    SyncSubspaceSignature
  >;

  // Commitment scheme
  private maxPayloadSizePower: number;

  private challengeHash: (bytes: Uint8Array) => Promise<Uint8Array>;
  private nonce: Uint8Array;
  private ourChallenge = deferred<Uint8Array>();
  private theirChallenge = deferred<Uint8Array>();

  // Private area intersection
  private intersectionHandlesOurs = new HandleStore<Intersection<PsiGroup>>();
  private intersectionHandlesTheirs = new HandleStore<Intersection<PsiGroup>>();
  private paiFinder: PaiFinder<
    NamespaceId,
    SubspaceId,
    PsiGroup,
    Scalar,
    ReadCapability,
    SubspaceCapability,
    SyncSignature,
    SyncSubspaceSignature
  >;

  constructor(
    opts: WgpsMessengerOpts<
      NamespaceId,
      SubspaceId,
      PsiGroup,
      Scalar,
      SubspaceCapability,
      SyncSubspaceSignature,
      ReadCapability
    >,
  ) {
    if (opts.maxPayloadSizePower < 0 || opts.maxPayloadSizePower > 64) {
      throw new ValidationError(
        "maxPayloadSizePower must be a natural number less than or equal to 64",
      );
    }

    this.nonce = crypto.getRandomValues(new Uint8Array(opts.challengeLength));

    this.transport = new ReadyTransport({
      transport: opts.transport,
      challengeHashLength: opts.challengeHashLength,
    });

    // Setup Private Area Intersection
    this.paiFinder = new PaiFinder({
      paiScheme: opts.paiScheme,
      intersectionHandlesOurs: this.intersectionHandlesOurs,
      intersectionHandlesTheirs: this.intersectionHandlesTheirs,
    });

    // Plug our transport into a new encoder.
    this.encoder = new MessageEncoder(this.transport, opts.encodings);

    // Start decoding incoming messages.
    const decodedMessages = decodeMessages({
      transport: this.transport,
      challengeLength: opts.challengeLength,
    });

    // Begin handling decoded messages
    (async () => {
      for await (const message of decodedMessages) {
        this.handleMessage(message);
      }
    })();

    // Set private variables for commitment scheme
    this.maxPayloadSizePower = opts.maxPayloadSizePower;
    this.challengeHash = opts.challengeHash;

    // Initiate commitment scheme.
    this.initiate();
  }

  async initiate() {
    // Send our max payload size.
    await this.transport.send(new Uint8Array([this.maxPayloadSizePower]));

    // Hash the nonce with the challenge-hashing function.
    const commitment = await this.challengeHash(this.nonce);

    // Send the digest of the nonce to the other peer.
    await this.transport.send(commitment);

    // Wait until we have the received commitment.
    await this.transport.receivedCommitment;

    // Now safe to send commitment reveal message.

    this.encoder.send({
      kind: MSG_COMMITMENT_REVEAL,
      nonce: this.nonce,
    });
  }

  async handleMessage(
    message: SyncMessage<
      PsiGroup,
      SubspaceCapability,
      SyncSubspaceSignature
    >,
  ) {
    switch (message.kind) {
      case MSG_COMMITMENT_REVEAL: {
        // Determine challenges.
        const receivedCommitment = await this.transport.receivedCommitment;
        const digest = await this.challengeHash(message.nonce);

        if (orderBytes(receivedCommitment, digest) !== 0) {
          throw new WgpsMessageValidationError(
            "Digest of revealed commitment did not match other side's received commitment.",
          );
        }

        const xor = (a: Uint8Array, b: Uint8Array, complementary = false) => {
          const xored = new Uint8Array(a.byteLength);

          for (let i = 0; i < a.byteLength; i++) {
            xored.set([
              complementary ? ~(a[i] ^ b[i]) : (a[i] ^ b[i]),
            ], i);
          }

          return xored;
        };

        // If alfie, bitwise XOR of our nonce and received nonce.
        if (this.transport.role === IS_ALFIE) {
          this.ourChallenge.resolve(xor(this.nonce, message.nonce));
          this.theirChallenge.resolve(xor(this.nonce, message.nonce, true));
        } else {
          // If betty, bitwise complement of XOR of our nonce and received nonce.
          this.ourChallenge.resolve(xor(this.nonce, message.nonce, true));
          this.theirChallenge.resolve(xor(this.nonce, message.nonce));
        }
      }
    }
  }
}
