import {
  areaIsIncluded,
  AreaOfInterest,
  defaultEntry,
  deferred,
  Entry,
  entryPosition,
  intersectArea,
  isIncluded3d,
  isIncludedArea,
  orderBytes,
  Range3d,
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
  MSG_DATA_BIND_PAYLOAD_REQUEST,
  MSG_DATA_REPLY_PAYLOAD,
  MSG_DATA_SEND_ENTRY,
  MSG_DATA_SEND_PAYLOAD,
  MSG_DATA_SET_EAGERNESS,
  MSG_PAI_BIND_FRAGMENT,
  MSG_PAI_REPLY_FRAGMENT,
  MSG_PAI_REPLY_SUBSPACE_CAPABILITY,
  MSG_PAI_REQUEST_SUBSPACE_CAPABILITY,
  MSG_RECONCILIATION_ANNOUNCE_ENTRIES,
  MSG_RECONCILIATION_SEND_ENTRY,
  MSG_RECONCILIATION_SEND_FINGERPRINT,
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
import { ReconcilerMap } from "./reconciliation/reconciler_map.ts";
import { Announcer } from "./reconciliation/announcer.ts";
import { CapFinder } from "./cap_finder.ts";
import { DataSender } from "./data/data_sender.ts";
import { PayloadIngester } from "./data/payload_ingester.ts";

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
  DynamicToken,
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
    DynamicToken,
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    AuthorisationOpts
  >;

  interests: Map<
    ReadAuthorisation<ReadCapability, SubspaceCapability>,
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
  DynamicToken,
  NamespaceId,
  SubspaceId,
  PayloadDigest,
  AuthorisationOpts,
> {
  private interests: Map<
    ReadAuthorisation<ReadCapability, SubspaceCapability>,
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
    Fingerprint,
    AuthorisationToken,
    StaticToken,
    DynamicToken,
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    AuthorisationOpts
  >;
  private reconciliationChannel = new GuaranteedQueue();
  private dataChannel = new GuaranteedQueue();
  private intersectionChannel = new GuaranteedQueue();
  private capabilityChannel = new GuaranteedQueue();
  private areaOfInterestChannel = new GuaranteedQueue();
  private payloadRequestChannel = new GuaranteedQueue();
  private staticTokenChannel = new GuaranteedQueue();

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
    DynamicToken,
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

  private reconcilerMap = new ReconcilerMap<
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    AuthorisationOpts,
    AuthorisationToken,
    Fingerprint
  >();

  private aoiIntersectionFinder: AoiIntersectionFinder<NamespaceId, SubspaceId>;

  private announcer: Announcer<
    Fingerprint,
    AuthorisationToken,
    StaticToken,
    DynamicToken,
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    AuthorisationOpts
  >;

  private currentlyReceivingEntries: {
    namespace: NamespaceId;
    range: Range3d<SubspaceId>;
    remaining: bigint;
  } | undefined;

  // Data

  private capFinder: CapFinder<
    ReadCapability,
    Receiver,
    SyncSignature,
    ReceiverSecretKey,
    NamespaceId,
    SubspaceId,
    PayloadDigest
  >;

  private currentlySentEntry: Entry<NamespaceId, SubspaceId, PayloadDigest>;
  private currentlyReceivedEntry: Entry<NamespaceId, SubspaceId, PayloadDigest>;
  private currentlyReceivedOffset = 0n;

  private handlesPayloadRequestsOurs = new HandleStore<{
    offset: bigint;
    entry: Entry<NamespaceId, SubspaceId, PayloadDigest>;
  }>();
  private handlesPayloadRequestsTheirs = new HandleStore<{
    offset: bigint;
    entry: Entry<NamespaceId, SubspaceId, PayloadDigest>;
  }>();

  private dataSender: DataSender<
    Fingerprint,
    AuthorisationToken,
    DynamicToken,
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    AuthorisationOpts
  >;

  private payloadIngester: PayloadIngester<
    Fingerprint,
    AuthorisationToken,
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    AuthorisationOpts
  >;

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
      DynamicToken,
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
        // Check that AOI area is in granted area.
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

    this.announcer = new Announcer({
      authorisationTokenScheme: this.schemes.authorisationToken,
      payloadScheme: this.schemes.payload,
      staticTokenHandleStoreOurs: this.handlesStaticTokensOurs,
    });

    // Data

    this.currentlyReceivedEntry = defaultEntry(
      this.schemes.namespace.defaultNamespaceId,
      this.schemes.subspace.minimalSubspaceId,
      this.schemes.payload.defaultDigest,
    );
    this.currentlySentEntry = defaultEntry(
      this.schemes.namespace.defaultNamespaceId,
      this.schemes.subspace.minimalSubspaceId,
      this.schemes.payload.defaultDigest,
    );

    this.capFinder = new CapFinder({
      handleStoreOurs: this.handlesCapsOurs,
      schemes: {
        accessControl: this.schemes.accessControl,
        namespace: this.schemes.namespace,
        subspace: this.schemes.subspace,
      },
    });

    this.dataSender = new DataSender({
      handlesPayloadRequestsTheirs: this.handlesPayloadRequestsTheirs,
      storeMap: this.storeMap,
    });

    this.payloadIngester = new PayloadIngester({
      storeMap: this.storeMap,
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
      defaultNamespaceId: this.schemes.namespace.defaultNamespaceId,
      defaultSubspaceId: this.schemes.subspace.minimalSubspaceId,
      defaultPayloadDigest: this.schemes.payload.defaultDigest,
      handleToNamespaceId: (handle) => {
        const res = this.aoiIntersectionFinder.handleToNamespaceId(
          handle,
          true,
        );

        if (res === undefined) {
          throw new WgpsMessageValidationError(
            "Couldn't find namespace corresponding to handle!",
          );
        }

        return res;
      },
      aoiHandlesToRange3d: (senderAoiHandle, receiverAoiHandle) => {
        const reconciler = this.reconcilerMap.getReconciler(
          senderAoiHandle,
          receiverAoiHandle,
        );

        return reconciler.range;
      },
      getCurrentlySentEntry: () => {
        return this.currentlySentEntry;
      },
    });

    onAsyncIterate(this.reconciliationChannel, async (message) => {
      await this.transport.send(message);
    });

    onAsyncIterate(this.dataChannel, async (message) => {
      await this.transport.send(message);
    });

    onAsyncIterate(this.intersectionChannel, async (message) => {
      await this.transport.send(message);
    });

    onAsyncIterate(this.capabilityChannel, async (message) => {
      await this.transport.send(message);
    });

    onAsyncIterate(this.areaOfInterestChannel, async (message) => {
      await this.transport.send(message);
    });

    onAsyncIterate(this.payloadRequestChannel, async (message) => {
      await this.transport.send(message);
    });

    onAsyncIterate(this.staticTokenChannel, async (message) => {
      await this.transport.send(message);
    });

    onAsyncIterate(this.encoder, async ({ channel, message }) => {
      switch (channel) {
        case LogicalChannel.ReconciliationChannel: {
          this.reconciliationChannel.push(message);
          break;
        }
        case LogicalChannel.DataChannel: {
          this.dataChannel.push(message);
          break;
        }
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
        case LogicalChannel.PayloadRequestChannel: {
          this.payloadRequestChannel.push(message);
          break;
        }
        case LogicalChannel.StaticTokenChannel: {
          this.staticTokenChannel.push(message);
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
      schemes: this.schemes,
      getCap: (handle) => {
        return this.handlesCapsOurs.getEventually(handle);
      },
      getIntersectionPrivy: (handle) => {
        return this.paiFinder.getIntersectionPrivy(handle);
      },
      defaultNamespaceId: this.schemes.namespace.defaultNamespaceId,
      defaultSubspaceId: this.schemes.subspace.minimalSubspaceId,
      defaultPayloadDigest: this.schemes.payload.defaultDigest,
      handleToNamespaceId: (handle) => {
        const res = this.aoiIntersectionFinder.handleToNamespaceId(
          handle,
          false,
        );

        if (res === undefined) {
          throw new WgpsMessageValidationError(
            "Couldn't find namespace corresponding to handle!",
          );
        }

        return res;
      },
      aoiHandlesToRange3d: (senderAoiHandle, receiverAoiHandle) => {
        const reconciler = this.reconcilerMap.getReconciler(
          receiverAoiHandle,
          senderAoiHandle,
        );

        return reconciler.range;
      },
      // TODO: this might need to be async and use handleStore.getEventually
      aoiHandlesToArea: (senderHandle, receiverHandle) => {
        const senderAoi = this.handlesAoisTheirs.get(senderHandle);
        const receiverAoi = this.handlesAoisOurs.get(receiverHandle);

        if (!senderAoi) {
          throw new WgpsMessageValidationError(
            "Failed to retrieve unbound sender AOI handle",
          );
        }

        if (!receiverAoi) {
          throw new WgpsMessageValidationError(
            "Failed to retrieve unbound receiver AOI handle",
          );
        }

        const intersectingArea = intersectArea(
          this.schemes.subspace.order,
          senderAoi.area,
          receiverAoi.area,
        );

        if (!intersectingArea) {
          throw new WgpsMessageValidationError(
            "No intersecting area between two AOI handles",
          );
        }

        return intersectingArea;
      },
      aoiHandlesToNamespace: (senderHandle, receiverHandle) => {
        const reconciler = this.reconcilerMap.getReconciler(
          receiverHandle,
          senderHandle,
        );

        return reconciler.store.namespace;
      },
      getCurrentlyReceivedEntry: () => {
        return this.currentlyReceivedEntry;
      },
    });

    // Begin handling decoded messages
    onAsyncIterate(decodedMessages, async (message) => {
      await this.handleMessage(message);
    });

    // Set private variables for commitment scheme
    this.maxPayloadSizePower = opts.maxPayloadSizePower;
    this.challengeHash = opts.challengeHash;

    // Get this ball rolling.
    this.initiate();
    this.setupData();
    this.setupReconciliation();
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
      channel: LogicalChannel.ReconciliationChannel,
      amount: BigInt(Number.MAX_SAFE_INTEGER),
    });

    this.encoder.encode({
      kind: MSG_CONTROL_ISSUE_GUARANTEE,
      channel: LogicalChannel.DataChannel,
      amount: BigInt(Number.MAX_SAFE_INTEGER),
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

    this.encoder.encode({
      kind: MSG_CONTROL_ISSUE_GUARANTEE,
      channel: LogicalChannel.AreaOfInterestChannel,
      amount: BigInt(Number.MAX_SAFE_INTEGER),
    });

    this.encoder.encode({
      kind: MSG_CONTROL_ISSUE_GUARANTEE,
      channel: LogicalChannel.PayloadRequestChannel,
      amount: BigInt(Number.MAX_SAFE_INTEGER),
    });

    this.encoder.encode({
      kind: MSG_CONTROL_ISSUE_GUARANTEE,
      channel: LogicalChannel.StaticTokenChannel,
      amount: BigInt(Number.MAX_SAFE_INTEGER),
    });
  }

  private setupPai(
    authorisations: ReadAuthorisation<ReadCapability, SubspaceCapability>[],
  ) {
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

        this.capFinder.addCap(capHandle);

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
    // When our announcer releases an 'announcement pack' (everything needed to announce and send some entries)...
    onAsyncIterate(this.announcer.announcementPacks(), (pack) => {
      // Bind any static tokens first.
      for (const staticToken of pack.staticTokenBinds) {
        this.encoder.encode({
          kind: MSG_SETUP_BIND_STATIC_TOKEN,
          staticToken,
        });
      }

      // Then announce the entries.
      this.encoder.encode({
        kind: MSG_RECONCILIATION_ANNOUNCE_ENTRIES,
        count: BigInt(pack.announcement.count),
        range: pack.announcement.range,
        wantResponse: pack.announcement.wantResponse,
        willSort: true,
        receiverHandle: pack.announcement.receiverHandle,
        senderHandle: pack.announcement.senderHandle,
      });

      // Then send the entries.
      for (const entry of pack.entries) {
        this.encoder.encode({
          kind: MSG_RECONCILIATION_SEND_ENTRY,
          entry: entry.lengthyEntry,
          dynamicToken: entry.dynamicToken,
          staticTokenHandle: entry.staticTokenHandle,
        });
      }
    });

    // Whenever the area of interest intersection finder finds an intersection from the setup phase...
    onAsyncIterate(
      this.aoiIntersectionFinder.intersections(),
      (intersection) => {
        const store = this.storeMap.get(intersection.namespace);

        const aoiOurs = this.handlesAoisOurs.get(intersection.ours);

        if (!aoiOurs) {
          throw new WillowError("Couldn't dereference AOI handle");
        }

        const aoiTheirs = this.handlesAoisTheirs.get(intersection.theirs);

        if (!aoiTheirs) {
          throw new WillowError("Couldn't dereference AOI handle");
        }

        // Create a new reconciler
        const reconciler = new Reconciler({
          namespace: intersection.namespace,
          aoiOurs,
          aoiTheirs,
          role: this.transport.role,
          store,
          subspaceScheme: this.schemes.subspace,
          fingerprintScheme: this.schemes.fingerprint,
        });

        this.reconcilerMap.addReconciler(
          intersection.ours,
          intersection.theirs,
          reconciler,
        );

        // Whenever the reconciler emits a fingerprint...
        onAsyncIterate(reconciler.fingerprints(), ({ fingerprint, range }) => {
          // Send a ReconciliationSendFingerprint message
          this.encoder.encode({
            kind: MSG_RECONCILIATION_SEND_FINGERPRINT,
            fingerprint,
            range,
            senderHandle: intersection.ours,
            receiverHandle: intersection.theirs,
          });
        });

        // Whenever the reconciler emits an announcement...
        onAsyncIterate(reconciler.entryAnnouncements(), (announcement) => {
          // Let the announcer figure out what to do.
          this.announcer.queueAnnounce({
            namespace: intersection.namespace,
            range: announcement.range,
            store,
            wantResponse: announcement.wantResponse,
            receiverHandle: intersection.theirs,
            senderHandle: intersection.ours,
          });
        });
      },
    );
  }

  setupData() {
    onAsyncIterate(this.dataSender.messages(), (msg) => {
      this.encoder.encode(msg);
    });
  }

  private async handleMessage(
    message: SyncMessage<
      ReadCapability,
      SyncSignature,
      PsiGroup,
      SubspaceCapability,
      SyncSubspaceSignature,
      Fingerprint,
      StaticToken,
      DynamicToken,
      NamespaceId,
      SubspaceId,
      PayloadDigest
    >,
  ) {
    console.log(
      `%c${this.transport.role === IS_ALFIE ? "Alfie" : "Betty"} got: ${
        String(message.kind)
      }`,
      `color: ${this.transport.role === IS_ALFIE ? "red" : "blue"}`,
    );

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
            xored.set([complementary ? ~(a[i] ^ b[i]) : a[i] ^ b[i]], i);
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
          case LogicalChannel.ReconciliationChannel:
            this.reconciliationChannel.addGuarantees(message.amount);
            break;
          case LogicalChannel.DataChannel:
            this.dataChannel.addGuarantees(message.amount);
            break;
          case LogicalChannel.IntersectionChannel:
            this.intersectionChannel.addGuarantees(message.amount);
            break;
          case LogicalChannel.CapabilityChannel:
            this.capabilityChannel.addGuarantees(message.amount);
            break;
          case LogicalChannel.AreaOfInterestChannel:
            this.areaOfInterestChannel.addGuarantees(message.amount);
            break;
          case LogicalChannel.PayloadRequestChannel:
            this.payloadRequestChannel.addGuarantees(message.amount);
            break;
          case LogicalChannel.StaticTokenChannel:
            this.staticTokenChannel.addGuarantees(message.amount);
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
          throw new WgpsMessageValidationError("PAI: Partner sent invalid cap");
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
        const cap = await this.handlesCapsTheirs.getEventually(
          message.authorisation,
        );

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

      // Reconciliation

      case MSG_RECONCILIATION_SEND_FINGERPRINT: {
        const reconciler = this.reconcilerMap.getReconciler(
          message.receiverHandle,
          message.senderHandle,
        );

        // TODO: DO WE AWAIT THIS???
        await reconciler.respond(message.range, message.fingerprint);

        break;
      }

      case MSG_RECONCILIATION_ANNOUNCE_ENTRIES: {
        if (
          this.currentlyReceivingEntries &&
          this.currentlyReceivingEntries.remaining > 0n
        ) {
          throw new WgpsMessageValidationError(
            "Never received the entries we were promised...",
          );
        }
        const reconciler = this.reconcilerMap.getReconciler(
          message.receiverHandle,
          message.senderHandle,
        );

        // Set the currently receiving namespace and range and expected count.
        this.currentlyReceivingEntries = {
          remaining: message.count,
          namespace: reconciler.store.namespace,
          range: message.range,
        };

        // If a response is wanted... queue up announcement
        if (message.wantResponse) {
          this.announcer.queueAnnounce({
            namespace: reconciler.store.namespace,
            store: reconciler.store,
            range: message.range,
            wantResponse: false,
            receiverHandle: message.senderHandle,
            senderHandle: message.receiverHandle,
          });
        }

        break;
      }

      case MSG_RECONCILIATION_SEND_ENTRY: {
        if (!this.currentlyReceivingEntries) {
          throw new WgpsMessageValidationError(
            "Received entry when no entries have been announced.",
          );
        }

        const entryPos = entryPosition(message.entry.entry);

        const isInRange = isIncluded3d(
          this.schemes.subspace.order,
          this.currentlyReceivingEntries.range,
          entryPos,
        );

        if (!isInRange) {
          throw new WgpsMessageValidationError(
            "Received entry which does not fall in the announced range!",
          );
        }

        if (this.currentlyReceivingEntries.remaining <= 0n) {
          throw new WgpsMessageValidationError(
            "Received entry for an announcement when we are not expecting any more!",
          );
        }

        const store = this.storeMap.get(
          this.currentlyReceivingEntries.namespace,
        );

        const staticToken = await this.handlesStaticTokensTheirs.getEventually(
          message.staticTokenHandle,
        );

        const authToken = this.schemes.authorisationToken.recomposeAuthToken(
          staticToken,
          message.dynamicToken,
        );

        const result = await store.ingestEntry(
          message.entry.entry,
          authToken,
          "TODO_DEFINE_THIS_WHEN_PLUMTREES_GROW",
        );

        if (result.kind === "failure") {
          throw new WgpsMessageValidationError(
            `Entry ingestion FAILED: ${result.message}`,
          );
        }

        this.currentlyReceivingEntries.remaining -= 1n;

        if (
          result.kind === "success" &&
          message.entry.available === message.entry.entry.payloadLength
        ) {
          // Request the payload.

          const capHandle = this.capFinder.findCapHandle(message.entry.entry);

          if (capHandle === undefined) {
            throw new WillowError(
              "Couldn't get a capability for a given entry",
            );
          }

          this.handlesPayloadRequestsOurs.bind({
            entry: message.entry.entry,
            offset: 0n,
          });

          this.encoder.encode({
            kind: MSG_DATA_BIND_PAYLOAD_REQUEST,
            entry: message.entry.entry,
            offset: 0n,
            capability: capHandle,
          });
        }

        break;
      }

      // Data

      case MSG_DATA_SEND_ENTRY: {
        const staticToken = await this.handlesStaticTokensTheirs.getEventually(
          message.staticTokenHandle,
        );

        const authToken = this.schemes.authorisationToken.recomposeAuthToken(
          staticToken,
          message.dynamicToken,
        );

        const store = this.storeMap.get(message.entry.namespaceId);

        const result = await store.ingestEntry(message.entry, authToken);

        if (result.kind === "failure") {
          throw new WgpsMessageValidationError(result.message);
        }

        this.currentlyReceivedEntry = message.entry;
        this.currentlyReceivedOffset = message.offset;

        this.payloadIngester.target(message.entry);

        break;
      }

      case MSG_DATA_SEND_PAYLOAD: {
        if (
          message.amount + this.currentlyReceivedOffset >
            this.currentlyReceivedEntry.payloadLength
        ) {
          throw new WgpsMessageValidationError("Partner sent too many bytes.");
        }

        const endHere = this.currentlyReceivedOffset ===
          this.currentlyReceivedEntry.payloadLength;

        this.payloadIngester.push(message.bytes, endHere);

        this.currentlyReceivedOffset += message.amount;

        break;
      }

      case MSG_DATA_SET_EAGERNESS: {
        // Do nothing (for now);
        break;
      }

      case MSG_DATA_BIND_PAYLOAD_REQUEST: {
        // Check cap matches entry.
        const cap = await this.handlesCapsTheirs.getEventually(
          message.capability,
        );

        const grantedNamespace = this.schemes.accessControl.getGrantedNamespace(
          cap,
        );

        if (
          !this.schemes.namespace.isEqual(
            message.entry.namespaceId,
            grantedNamespace,
          )
        ) {
          throw new WgpsMessageValidationError("Cap did not match entry");
        }

        const position = entryPosition(message.entry);

        const grantedArea = this.schemes.accessControl.getGrantedArea(cap);

        const isEntryWithinCap = isIncludedArea(
          this.schemes.subspace.order,
          grantedArea,
          position,
        );

        if (!isEntryWithinCap) {
          throw new WgpsMessageValidationError("Cap did not match entry");
        }

        const handle = this.handlesPayloadRequestsTheirs.bind({
          entry: message.entry,
          offset: message.offset,
        });

        this.dataSender.queuePayloadRequest(handle);

        break;
      }

      case MSG_DATA_REPLY_PAYLOAD: {
        const result = this.handlesPayloadRequestsOurs.get(message.handle);

        if (!result) {
          throw new WgpsMessageValidationError(
            "Could not dereference payload request handle",
          );
        }

        this.currentlyReceivedEntry = result.entry;
        this.currentlyReceivedOffset = result.offset;

        this.payloadIngester.target(result.entry);

        break;
      }

      default:
        throw new WgpsMessageValidationError("Unhandled message type");
    }
  }
}
