import {
  areaIsIncluded,
  AreaOfInterest,
  deferred,
  orderBytes,
} from "../../deps.ts";
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
  MSG_SETUP_BIND_AREA_OF_INTEREST,
  MSG_SETUP_BIND_READ_CAPABILITY,
  MSG_SETUP_BIND_STATIC_TOKEN,
  ReadAuthorisation,
  SyncMessage,
  SyncSchemes,
  Transport,
} from "./types.ts";
import { Intersection } from "./pai/types.ts";
import { onAsyncIterate } from "./util.ts";
import { WillowError } from "../../mod.universal.ts";
import { GuaranteedQueue } from "./guaranteed_queue.ts";
import { AoiIntersectionFinder } from "./reconciliation/aoi_intersection_finder.ts";
import { StoreDriverCallback, StoreMap } from "./store_map.ts";
import { Reconciler } from "./reconciliation/reconciler.ts";

export type WgpsMessengerOpts<
  ReadCapability,
  Receiver,
  SyncSignature,
  ReceiverSecretKey,
  PsiGroup,
  PsiScalar,
  SubspaceCapability,
  SubspaceReceiver,
  SyncSubspaceSignature,
  SubspaceSecretKey,
  Fingerprint,
  AuthorisationToken,
  StaticToken,
  NamespaceId,
  SubspaceId,
  PayloadDigest,
  AuthorisationOpts,
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
    Receiver,
    SyncSignature,
    ReceiverSecretKey,
    PsiGroup,
    PsiScalar,
    SubspaceCapability,
    SubspaceReceiver,
    SyncSubspaceSignature,
    SubspaceSecretKey,
    Fingerprint,
    AuthorisationToken,
    StaticToken,
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    AuthorisationOpts
  >;

  interests: Map<
    ReadAuthorisation<
      ReadCapability,
      SubspaceCapability
    >,
    AreaOfInterest<SubspaceId>[]
  >;

  getStoreDrivers: StoreDriverCallback<
    Fingerprint,
    NamespaceId,
    SubspaceId,
    PayloadDigest
  >;
};

/** Coordinates a complete WGPS synchronisation session. */
export class WgpsMessenger<
  ReadCapability,
  Receiver,
  SyncSignature,
  ReceiverSecretKey,
  PsiGroup,
  PsiScalar,
  SubspaceCapability,
  SubspaceReceiver,
  SyncSubspaceSignature,
  SubspaceSecretKey,
  Fingerprint,
  AuthorisationToken,
  StaticToken,
  NamespaceId,
  SubspaceId,
  PayloadDigest,
  AuthorisationOpts,
> {
  private interests: Map<
    ReadAuthorisation<
      ReadCapability,
      SubspaceCapability
    >,
    AreaOfInterest<SubspaceId>[]
  >;

  private transport: ReadyTransport;
  private encoder: MessageEncoder<
    ReadCapability,
    Receiver,
    SyncSignature,
    ReceiverSecretKey,
    PsiGroup,
    PsiScalar,
    SubspaceCapability,
    SubspaceReceiver,
    SyncSubspaceSignature,
    SubspaceSecretKey,
    StaticToken,
    NamespaceId,
    SubspaceId
  >;
  private intersectionChannel = new GuaranteedQueue();
  private capabilityChannel = new GuaranteedQueue();
  private areaOfInterestChannel = new GuaranteedQueue();

  // Commitment scheme
  private maxPayloadSizePower: number;

  private challengeHash: (bytes: Uint8Array) => Promise<Uint8Array>;
  private nonce: Uint8Array;
  private ourChallenge = deferred<Uint8Array>();
  private theirChallenge = deferred<Uint8Array>();

  private schemes: SyncSchemes<
    ReadCapability,
    Receiver,
    SyncSignature,
    ReceiverSecretKey,
    PsiGroup,
    PsiScalar,
    SubspaceCapability,
    SubspaceReceiver,
    SyncSubspaceSignature,
    SubspaceSecretKey,
    Fingerprint,
    AuthorisationToken,
    StaticToken,
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    AuthorisationOpts
  >;

  // Private area intersection
  private handlesIntersectionsOurs = new HandleStore<Intersection<PsiGroup>>();
  private handlesIntersectionsTheirs = new HandleStore<
    Intersection<PsiGroup>
  >();
  private paiFinder: PaiFinder<
    ReadCapability,
    PsiGroup,
    PsiScalar,
    SubspaceCapability,
    NamespaceId,
    SubspaceId
  >;

  // Setup
  private handlesCapsOurs = new HandleStore<ReadCapability>();
  private handlesCapsTheirs = new HandleStore<ReadCapability>();

  private handlesAoisOurs = new HandleStore<AreaOfInterest<SubspaceId>>();
  private handlesAoisTheirs = new HandleStore<AreaOfInterest<SubspaceId>>();

  private handlesStaticTokensOurs = new HandleStore<StaticToken>();
  private handlesStaticTokensTheirs = new HandleStore<StaticToken>();

  // Reconciliation

  private storeMap: StoreMap<
    Fingerprint,
    AuthorisationToken,
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    AuthorisationOpts
  >;

  private aoiIntersectionFinder: AoiIntersectionFinder<NamespaceId, SubspaceId>;

  constructor(
    opts: WgpsMessengerOpts<
      ReadCapability,
      Receiver,
      SyncSignature,
      ReceiverSecretKey,
      PsiGroup,
      PsiScalar,
      SubspaceCapability,
      SubspaceReceiver,
      SyncSubspaceSignature,
      SubspaceSecretKey,
      Fingerprint,
      AuthorisationToken,
      StaticToken,
      NamespaceId,
      SubspaceId,
      PayloadDigest,
      AuthorisationOpts
    >,
  ) {
    if (opts.maxPayloadSizePower < 0 || opts.maxPayloadSizePower > 64) {
      throw new ValidationError(
        "maxPayloadSizePower must be a natural number less than or equal to 64",
      );
    }

    for (const [authorisation, areas] of opts.interests) {
      if (areas.length === 0) {
        throw new WillowError("No Areas of Interest given for authorisation");
      }

      // Get granted area of authorisation
      const grantedArea = opts.schemes.accessControl.getGrantedArea(
        authorisation.capability,
      );

      for (const aoi of areas) {
        // Check that granted area is in granted area.
        const isWithin = areaIsIncluded(
          opts.schemes.subspace.order,
          aoi.area,
          grantedArea,
        );

        if (!isWithin) {
          throw new WillowError(
            "Given authorisation is not within authorisation's granted area",
          );
        }
      }
    }

    this.interests = opts.interests;

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
      intersectionHandlesOurs: this.handlesIntersectionsOurs,
      intersectionHandlesTheirs: this.handlesIntersectionsTheirs,
    });

    // Reconciliation helpers

    this.storeMap = new StoreMap({
      getStoreDrivers: opts.getStoreDrivers,
      schemes: opts.schemes,
    });

    this.aoiIntersectionFinder = new AoiIntersectionFinder({
      namespaceScheme: this.schemes.namespace,
      subspaceScheme: this.schemes.subspace,
      handlesOurs: this.handlesAoisOurs,
      handlesTheirs: this.handlesAoisTheirs,
    });

    // Send encoded messages

    this.encoder = new MessageEncoder(opts.schemes, {
      getIntersectionPrivy: (handle) => {
        return this.paiFinder.getIntersectionPrivy(handle);
      },
      getCap: (handle) => {
        const cap = this.handlesCapsOurs.get(handle);

        if (!cap) {
          throw new WillowError("Tried to get a cap with an unknown handle.");
        }

        return cap;
      },
    });

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
        case LogicalChannel.AreaOfInterestChannel: {
          this.areaOfInterestChannel.push(message);
          break;
        }
        case null:
          await this.transport.send(message);
          break;
        default:
          throw new WillowError("Didn't handle an encoded message");
      }
    });

    // Start decoding incoming messages.
    const decodedMessages = decodeMessages({
      transport: this.transport,
      challengeLength: opts.challengeLength,
      encodings: encodings,
      schemes: this.schemes,
      getCap: (handle) => {
        return this.handlesCapsOurs.getEventually(handle);
      },
      getIntersectionPrivy: (handle) => {
        return this.paiFinder.getIntersectionPrivy(handle);
      },
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
    this.setupPai(Array.from(opts.interests.keys()));
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
    SubspaceCapability
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
      async ({ namespace, authorisation, handle }) => {
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

        const capHandle = this.handlesCapsOurs.bind(authorisation.capability);

        // Send capability
        this.encoder.encode({
          kind: MSG_SETUP_BIND_READ_CAPABILITY,
          capability: authorisation.capability,
          handle,
          signature,
        });

        const aois = this.interests.get(authorisation);

        if (!aois) {
          throw new WillowError("No interests known for a given authorisation");
        }

        // And areas of interest.
        for (const aoi of aois) {
          const handle = this.handlesAoisOurs.bind(aoi);

          this.aoiIntersectionFinder.addAoiHandleForNamespace(
            handle,
            namespace,
            true,
          );

          this.encoder.encode({
            kind: MSG_SETUP_BIND_AREA_OF_INTEREST,
            areaOfInterest: aoi,
            authorisation: capHandle,
          });
        }
      },
    );

    for (const auth of authorisations) {
      this.paiFinder.submitAuthorisation(auth);
    }
  }

  private setupReconciliation() {
    onAsyncIterate(
      this.aoiIntersectionFinder.intersections(),
      (intersection) => {
        // Create a new RangeReconciler.

        // It has to be mapped to the intersection handles so we can route messages properly.
        // What's a good id that's a unique product of two bigints?
        // Or a decent data structure...
        const store = this.storeMap.get(intersection.namespace);

        const aoiOurs = this.handlesAoisOurs.get(intersection.ours);

        if (!aoiOurs) {
          throw new WillowError("Couldn't dereference AOI handle");
        }

        const aoiTheirs = this.handlesAoisTheirs.get(intersection.theirs);

        if (!aoiTheirs) {
          throw new WillowError("Couldn't dereference AOI handle");
        }

        const reconciler = new Reconciler({
          namespace: intersection.namespace,
          aoiOurs,
          aoiTheirs,
          role: this.transport.role,
          store,
          subspaceScheme: this.schemes.subspace,
          fingerprintScheme: this.schemes.fingerprint,
        });

        onAsyncIterate(reconciler.fingerprints(), ({ fingerprint, range }) => {
          // Send a ReconciliationSendFingerprint
        });

        onAsyncIterate(reconciler.entryAnnouncements(), (announcement) => {
          // QUEUE a ReconciliationAnnounceEntries with our singleton announceentries thing
        });
      },
    );
  }

  private async handleMessage(
    message: SyncMessage<
      ReadCapability,
      SyncSignature,
      PsiGroup,
      SubspaceCapability,
      SyncSubspaceSignature,
      StaticToken,
      SubspaceId
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
              this.handlesIntersectionsTheirs.markForFreeing(message.handle);
            } else {
              this.handlesIntersectionsOurs.markForFreeing(message.handle);

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
        this.handlesIntersectionsOurs.incrementHandleReference(message.handle);
        this.paiFinder.receivedReply(message.handle, message.groupMember);
        this.handlesIntersectionsOurs.decrementHandleReference(message.handle);
        break;
      }
      case MSG_PAI_REQUEST_SUBSPACE_CAPABILITY: {
        this.handlesIntersectionsTheirs.incrementHandleReference(
          message.handle,
        );
        this.paiFinder.receivedSubspaceCapRequest(message.handle);
        this.handlesIntersectionsTheirs.decrementHandleReference(
          message.handle,
        );
        break;
      }
      case MSG_PAI_REPLY_SUBSPACE_CAPABILITY: {
        const isSubspaceCapValid = await this.schemes.subspaceCap.isValidCap(
          message.capability,
        );

        if (!isSubspaceCapValid) {
          throw new WgpsMessageValidationError(
            "PAI: Partner sent invalid cap",
          );
        }

        this.handlesIntersectionsOurs.incrementHandleReference(message.handle);

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
        this.handlesIntersectionsOurs.decrementHandleReference(message.handle);
        break;
      }

      // Setup
      case MSG_SETUP_BIND_READ_CAPABILITY: {
        const isValidCap = await this.schemes.accessControl.isValidCap(
          message.capability,
        );

        if (!isValidCap) {
          throw new WgpsMessageValidationError(
            "Received SetupBindReadCapability with invalid capability",
          );
        }

        const isAuthentic = await this.schemes.accessControl.signatures.verify(
          this.schemes.accessControl.getReceiver(message.capability),
          message.signature,
          await this.theirChallenge,
        );

        if (!isAuthentic) {
          throw new WgpsMessageValidationError(
            "Received SetupBindReadCapability with bad signature",
          );
        }

        this.handlesCapsTheirs.bind(message.capability);

        break;
      }

      case MSG_SETUP_BIND_AREA_OF_INTEREST: {
        const cap = this.handlesCapsTheirs.get(message.authorisation);

        if (!cap) {
          throw new WgpsMessageValidationError(
            "Received SetupBindAreaOfInterest referring to non-existent handle.",
          );
        }

        const grantedArea = this.schemes.accessControl.getGrantedArea(cap);

        const isContained = areaIsIncluded(
          this.schemes.subspace.order,
          message.areaOfInterest.area,
          grantedArea,
        );

        if (!isContained) {
          throw new WgpsMessageValidationError(
            "Received SetupBindAreaOfInterest with AOI outside the read cap it is for.",
          );
        }

        const grantedNamespace = this.schemes.accessControl.getGrantedNamespace(
          cap,
        );

        const handle = this.handlesAoisTheirs.bind(message.areaOfInterest);

        this.aoiIntersectionFinder.addAoiHandleForNamespace(
          handle,
          grantedNamespace,
          false,
        );

        break;
      }

      case MSG_SETUP_BIND_STATIC_TOKEN: {
        this.handlesStaticTokensTheirs.bind(message.staticToken);

        break;
      }

      default:
        throw new WgpsMessageValidationError("Unhandled message type");
    }
  }
}
