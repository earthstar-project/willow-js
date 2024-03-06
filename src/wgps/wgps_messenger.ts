import { deferred, orderBytes } from "../../deps.ts";
import { ValidationError, WgpsMessageValidationError } from "../errors.ts";
import { decodeMessages } from "./decoding/decode_messages.ts";
import { MessageEncoder } from "./encoding/message_encoder.ts";
import { ReadyTransport } from "./ready_transport.ts";
import { HandleStore } from "./handle_store.ts";
import { PaiFinder } from "./pai/pai_finder.ts";
import {
  HandleType,
  IS_ALFIE,
  LogicalChannel,
  MSG_COMMITMENT_REVEAL,
  MSG_CONTROL_ABSOLVE,
  MSG_CONTROL_ANNOUNCE_DROPPING,
  MSG_CONTROL_APOLOGISE,
  MSG_CONTROL_FREE,
  MSG_CONTROL_ISSUE_GUARANTEE,
  MSG_CONTROL_PLEAD,
  MSG_PAI_BIND_FRAGMENT,
  MSG_PAI_REPLY_FRAGMENT,
  MSG_PAI_REPLY_SUBSPACE_CAPABILITY,
  MSG_PAI_REQUEST_SUBSPACE_CAPABILITY,
  MSG_SETUP_BIND_READ_CAPABILITY,
  ReadAuthorisation,
  SyncEncodings,
  SyncMessage,
  SyncSchemes,
  Transport,
} from "./types.ts";
import { Intersection } from "./pai/types.ts";
import { onAsyncIterate } from "./util.ts";
import { WillowError } from "../../mod.universal.ts";
import { GuaranteedQueue } from "./guaranteed_queue.ts";

export type WgpsMessengerOpts<
  ReadCapability,
  ReadCapabilityPartial,
  Receiver,
  SyncSignature,
  ReceiverSecretKey,
  PsiGroup,
  PsiScalar,
  SubspaceCapability,
  SubspaceReceiver,
  SyncSubspaceSignature,
  SubspaceSecretKey,
  NamespaceId,
  SubspaceId,
> = {
  transport: Transport;

  /** Sets the maximum payload size for this peer, which is 2 to the power of the given number.
   *
   * The given power must be a natural number lesser than or equal to 64. */
  maxPayloadSizePower: number;

  challengeLength: number;
  challengeHashLength: number;

  challengeHash: (bytes: Uint8Array) => Promise<Uint8Array>;

  schemes: SyncSchemes<
    ReadCapability,
    ReadCapabilityPartial,
    Receiver,
    SyncSignature,
    ReceiverSecretKey,
    PsiGroup,
    PsiScalar,
    SubspaceCapability,
    SubspaceReceiver,
    SyncSubspaceSignature,
    SubspaceSecretKey,
    NamespaceId,
    SubspaceId
  >;

  readAuthorisations: ReadAuthorisation<
    ReadCapability,
    SubspaceCapability,
    SyncSignature,
    SyncSubspaceSignature
  >[];
};

/** Coordinates a complete WGPS synchronisation session. */
export class WgpsMessenger<
  ReadCapability,
  ReadCapabilityPartial,
  Receiver,
  SyncSignature,
  ReceiverSecretKey,
  PsiGroup,
  PsiScalar,
  SubspaceCapability,
  SubspaceReceiver,
  SyncSubspaceSignature,
  SubspaceSecretKey,
  NamespaceId,
  SubspaceId,
> {
  private transport: ReadyTransport;
  private encoder: MessageEncoder<
    ReadCapabilityPartial,
    SyncSignature,
    PsiGroup,
    SubspaceCapability,
    SyncSubspaceSignature
  >;
  private intersectionChannel = new GuaranteedQueue();
  private capabilityChannel = new GuaranteedQueue();

  // Commitment scheme
  private maxPayloadSizePower: number;

  private challengeHash: (bytes: Uint8Array) => Promise<Uint8Array>;
  private nonce: Uint8Array;
  private ourChallenge = deferred<Uint8Array>();
  private theirChallenge = deferred<Uint8Array>();

  private schemes: SyncSchemes<
    ReadCapability,
    ReadCapabilityPartial,
    Receiver,
    SyncSignature,
    ReceiverSecretKey,
    PsiGroup,
    PsiScalar,
    SubspaceCapability,
    SubspaceReceiver,
    SyncSubspaceSignature,
    SubspaceSecretKey,
    NamespaceId,
    SubspaceId
  >;

  // Private area intersection
  private intersectionHandlesOurs = new HandleStore<Intersection<PsiGroup>>();
  private intersectionHandlesTheirs = new HandleStore<Intersection<PsiGroup>>();
  private paiFinder: PaiFinder<
    ReadCapability,
    SyncSignature,
    PsiGroup,
    PsiScalar,
    SubspaceCapability,
    SyncSubspaceSignature,
    NamespaceId,
    SubspaceId
  >;

  // Setup
  private handlesCapsOurs = new HandleStore<ReadCapability>();
  private handlesCapsTheirs = new HandleStore<ReadCapability>();

  constructor(
    opts: WgpsMessengerOpts<
      ReadCapability,
      ReadCapabilityPartial,
      Receiver,
      SyncSignature,
      ReceiverSecretKey,
      PsiGroup,
      PsiScalar,
      SubspaceCapability,
      SubspaceReceiver,
      SyncSubspaceSignature,
      SubspaceSecretKey,
      NamespaceId,
      SubspaceId
    >,
  ) {
    if (opts.maxPayloadSizePower < 0 || opts.maxPayloadSizePower > 64) {
      throw new ValidationError(
        "maxPayloadSizePower must be a natural number less than or equal to 64",
      );
    }

    this.schemes = opts.schemes;

    this.nonce = crypto.getRandomValues(new Uint8Array(opts.challengeLength));

    const transport = new ReadyTransport({
      transport: opts.transport,
      challengeHashLength: opts.challengeHashLength,
    });

    this.transport = transport;

    // Setup Private Area Intersection
    this.paiFinder = new PaiFinder({
      namespaceScheme: opts.schemes.namespace,
      paiScheme: opts.schemes.pai,
      intersectionHandlesOurs: this.intersectionHandlesOurs,
      intersectionHandlesTheirs: this.intersectionHandlesTheirs,
    });

    const encodings: SyncEncodings<
      ReadCapabilityPartial,
      SyncSignature,
      PsiGroup,
      SubspaceCapability,
      SyncSubspaceSignature
    > = {
      readCapabilityPartial:
        opts.schemes.accessControl.encodings.readCapabilityPartial,
      syncSignature: opts.schemes.accessControl.encodings.syncSignature,
      groupMember: opts.schemes.pai.groupMemberEncoding,
      subspaceCapability: opts.schemes.subspaceCap.encodings.subspaceCapability,
      syncSubspaceSignature:
        opts.schemes.subspaceCap.encodings.syncSubspaceSignature,
    };

    // Send encoded messages
    this.encoder = new MessageEncoder(encodings);

    onAsyncIterate(this.intersectionChannel, async (message) => {
      await this.transport.send(message);
    });

    onAsyncIterate(this.capabilityChannel, async (message) => {
      await this.transport.send(message);
    });

    onAsyncIterate(this.encoder, async ({ channel, message }) => {
      switch (channel) {
        case LogicalChannel.IntersectionChannel: {
          this.intersectionChannel.push(message);
          break;
        }
        case LogicalChannel.CapabilityChannel: {
          this.capabilityChannel.push(message);
          break;
        }
        case null:
          await this.transport.send(message);
      }
    });

    // Start decoding incoming messages.
    const decodedMessages = decodeMessages({
      transport: this.transport,
      challengeLength: opts.challengeLength,
      encodings: encodings,
    });

    // Begin handling decoded messages
    onAsyncIterate(decodedMessages, (message) => {
      this.handleMessage(message);
    });

    // Set private variables for commitment scheme
    this.maxPayloadSizePower = opts.maxPayloadSizePower;
    this.challengeHash = opts.challengeHash;

    // Initiate commitment scheme.
    this.initiate();
    this.setupPai(opts.readAuthorisations);
  }

  private async initiate() {
    // Send our max payload size.
    await this.transport.send(new Uint8Array([this.maxPayloadSizePower]));

    // Hash the nonce with the challenge-hashing function.
    const commitment = await this.challengeHash(this.nonce);

    // Send the digest of the nonce to the other peer.
    await this.transport.send(commitment);

    // Wait until we have the received commitment.
    await this.transport.receivedCommitment;

    // Now safe to send commitment reveal message.

    this.encoder.encode({
      kind: MSG_COMMITMENT_REVEAL,
      nonce: this.nonce,
    });

    this.encoder.encode({
      kind: MSG_CONTROL_ISSUE_GUARANTEE,
      channel: LogicalChannel.IntersectionChannel,
      amount: BigInt(Number.MAX_SAFE_INTEGER),
    });

    this.encoder.encode({
      kind: MSG_CONTROL_ISSUE_GUARANTEE,
      channel: LogicalChannel.CapabilityChannel,
      amount: BigInt(Number.MAX_SAFE_INTEGER),
    });
  }

  private setupPai(authorisations: ReadAuthorisation<
    ReadCapability,
    SubspaceCapability,
    SyncSignature,
    SyncSubspaceSignature
  >[]) {
    // Hook up the PAI finder
    onAsyncIterate(this.paiFinder.fragmentBinds(), (bind) => {
      this.encoder.encode({
        kind: MSG_PAI_BIND_FRAGMENT,
        groupMember: bind.group,
        isSecondary: bind.isSecondary,
      });
    });

    onAsyncIterate(this.paiFinder.fragmentReplies(), (reply) => {
      this.encoder.encode({
        kind: MSG_PAI_REPLY_FRAGMENT,
        handle: reply.handle,
        groupMember: reply.groupMember,
      });
    });

    onAsyncIterate(this.paiFinder.subspaceCapRequests(), (handle) => {
      this.encoder.encode({
        kind: MSG_PAI_REQUEST_SUBSPACE_CAPABILITY,
        handle,
      });
    });

    onAsyncIterate(this.paiFinder.subspaceCapReplies(), async (reply) => {
      const receiver = this.schemes.subspaceCap.getReceiver(reply.subspaceCap);
      const secretKey = this.schemes.subspaceCap.getSecretKey(receiver);

      if (!secretKey) {
        throw new WillowError(
          "Tried to reply to a subspace cap request with a subspace cap we do not have a secret key for",
        );
      }

      const signature = await this.schemes.subspaceCap.signatures.sign(
        secretKey,
        await this.ourChallenge,
      );

      this.encoder.encode({
        kind: MSG_PAI_REPLY_SUBSPACE_CAPABILITY,
        handle: reply.handle,
        capability: reply.subspaceCap,
        signature,
      });
    });

    onAsyncIterate(
      this.paiFinder.intersections(),
      async ({ authorisation, handle, outer }) => {
        // Encode and sign the challenge using the receiver of the read cap.
        const receiver = this.schemes.accessControl.getReceiver(
          authorisation.capability,
        );

        const receiverSecretKey = this.schemes.accessControl.getSecretKey(
          receiver,
        );

        const signature = await this.schemes.accessControl.signatures.sign(
          receiverSecretKey,
          await this.ourChallenge,
        );

        // Create a partial capability.
        const partial = this.schemes.accessControl.getPartial(
          authorisation.capability,
          outer,
        );

        this.handlesCapsOurs.bind(authorisation.capability);

        // Send.
        this.encoder.encode({
          kind: MSG_SETUP_BIND_READ_CAPABILITY,
          capability: partial,
          handle,
          signature,
        });
      },
    );

    for (const auth of authorisations) {
      this.paiFinder.submitAuthorisation(auth);
    }
  }

  private async handleMessage(
    message: SyncMessage<
      ReadCapabilityPartial,
      SyncSignature,
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

        break;
      }
      case MSG_CONTROL_ISSUE_GUARANTEE: {
        switch (message.channel) {
          case LogicalChannel.IntersectionChannel:
            this.intersectionChannel.addGuarantees(message.amount);
            break;
          case LogicalChannel.CapabilityChannel:
            this.capabilityChannel.addGuarantees(message.amount);
            break;
        }

        break;
      }
      case MSG_CONTROL_ABSOLVE: {
        // Silently ignore.
        break;
      }
      case MSG_CONTROL_PLEAD: {
        const absolved = this.intersectionChannel.plead(message.target);

        this.encoder.encode({
          kind: MSG_CONTROL_ABSOLVE,
          channel: message.channel,
          amount: absolved,
        });

        break;
      }
      case MSG_CONTROL_ANNOUNCE_DROPPING: {
        // Should not happen
        throw new WgpsMessageValidationError(
          "Partner announced dropping — but we never optimistically send.",
        );
      }
      case MSG_CONTROL_APOLOGISE: {
        // Should not happen
        throw new WgpsMessageValidationError(
          "Partner apologised — but we never drop messages.",
        );
      }
      case MSG_CONTROL_FREE: {
        switch (message.handleType) {
          case HandleType.IntersectionHandle: {
            // Remember: 'mine' is from the perspective of the sender.
            if (message.mine) {
              this.intersectionHandlesTheirs.markForFreeing(message.handle);
            } else {
              this.intersectionHandlesOurs.markForFreeing(message.handle);

              this.encoder.encode({
                kind: MSG_CONTROL_FREE,
                handle: message.handle,
                handleType: message.handleType,
                mine: true,
              });
            }
          }
        }
        break;
      }

      // PAI
      case MSG_PAI_BIND_FRAGMENT: {
        this.paiFinder.receivedBind(message.groupMember, message.isSecondary);

        break;
      }
      case MSG_PAI_REPLY_FRAGMENT: {
        this.intersectionHandlesOurs.incrementHandleReference(message.handle);
        this.paiFinder.receivedReply(message.handle, message.groupMember);
        this.intersectionHandlesOurs.decrementHandleReference(message.handle);
        break;
      }
      case MSG_PAI_REQUEST_SUBSPACE_CAPABILITY: {
        this.intersectionHandlesTheirs.incrementHandleReference(message.handle);
        this.paiFinder.receivedSubspaceCapRequest(message.handle);
        this.intersectionHandlesTheirs.decrementHandleReference(message.handle);
        break;
      }
      case MSG_PAI_REPLY_SUBSPACE_CAPABILITY: {
        this.intersectionHandlesOurs.incrementHandleReference(message.handle);
        const isValid = await this.schemes.subspaceCap.signatures.verify(
          this.schemes.subspaceCap.getReceiver(message.capability),
          message.signature,
          await this.theirChallenge,
        );

        if (!isValid) {
          throw new WgpsMessageValidationError(
            "PAI: Partner sent invalid signature with subspace capability reply",
          );
        }

        const namespace = this.schemes.subspaceCap.getNamespace(
          message.capability,
        );

        this.paiFinder.receivedVerifiedSubspaceCapReply(
          message.handle,
          namespace,
        );
        this.intersectionHandlesOurs.decrementHandleReference(message.handle);
        break;
      }

      // Setup
      case MSG_SETUP_BIND_READ_CAPABILITY: {
        // Rehydrate the capability.
        const privy = this.paiFinder.getIntersectionPrivy(message.handle);

        const capability = this.schemes.accessControl.getCap(
          message.capability,
          privy.namespace,
          privy.outer,
        );

        const isAuthentic = await this.schemes.accessControl.signatures.verify(
          this.schemes.accessControl.getReceiver(capability),
          message.signature,
          await this.theirChallenge,
        );

        if (!isAuthentic) {
          throw new WgpsMessageValidationError(
            "Received SetupBindReadCapability with bad signature",
          );
        }

        this.handlesCapsTheirs.bind(capability);

        break;
      }

      default:
        throw new WgpsMessageValidationError("Unhandled message type");
    }
  }
}
