import {
  defaultEntry,
  defaultRange3d,
  type Entry,
  type Range3d,
} from "@earthstar/willow-utils";
import type {
  MsgReconciliationAnnounceEntries,
  MsgReconciliationSendEntry,
  MsgReconciliationSendFingerprint,
  MsgReconciliationTerminatePayload,
  ReconciliationPrivy,
} from "../types.ts";

export type ReconcileMsgTrackerOpts<NamespaceId, SubspaceId, PayloadDigest> = {
  defaultNamespaceId: NamespaceId;
  defaultSubspaceId: SubspaceId;
  defaultPayloadDigest: PayloadDigest;
  handleToNamespaceId: (aoiHandle: bigint) => NamespaceId;
  aoiHandlesToRange3d: (
    senderAoiHandle: bigint,
    receiverAoiHandle: bigint,
  ) => Promise<Range3d<SubspaceId>>;
};

export class ReconcileMsgTracker<
  Fingerprint,
  DynamicToken,
  NamespaceId,
  SubspaceId,
  PayloadDigest,
> {
  private prevRange: Range3d<SubspaceId>;
  private prevSenderHandle = 0n;
  private prevReceiverHandle = 0n;
  private prevEntry: Entry<NamespaceId, SubspaceId, PayloadDigest>;
  private prevToken = 0n;

  private announcedRange: Range3d<SubspaceId>;
  private announcedNamespace: NamespaceId;
  private announcedEntriesExpecting = false;

  private handleToNamespaceId: (aoiHandle: bigint) => NamespaceId;

  private isAwaitingTermination = false;

  constructor(
    opts: ReconcileMsgTrackerOpts<NamespaceId, SubspaceId, PayloadDigest>,
  ) {
    this.prevRange = defaultRange3d(opts.defaultSubspaceId);
    this.prevEntry = defaultEntry(
      opts.defaultNamespaceId,
      opts.defaultSubspaceId,
      opts.defaultPayloadDigest,
    );

    this.announcedRange = defaultRange3d(opts.defaultSubspaceId);
    this.announcedNamespace = opts.defaultNamespaceId;

    this.handleToNamespaceId = opts.handleToNamespaceId;
  }

  onSendFingerprint(
    msg: MsgReconciliationSendFingerprint<SubspaceId, Fingerprint>,
  ) {
    this.prevRange = msg.range;
    this.prevSenderHandle = msg.senderHandle;
    this.prevReceiverHandle = msg.receiverHandle;
  }

  onAnnounceEntries(
    msg: MsgReconciliationAnnounceEntries<SubspaceId>,
  ) {
    this.prevRange = msg.range;
    this.prevSenderHandle = msg.senderHandle;
    this.prevReceiverHandle = msg.receiverHandle;

    this.announcedRange = msg.range;
    this.announcedNamespace = this.handleToNamespaceId(msg.receiverHandle);
    this.announcedEntriesExpecting = !msg.isEmpty;
  }

  onSendEntry(
    msg: MsgReconciliationSendEntry<
      DynamicToken,
      NamespaceId,
      SubspaceId,
      PayloadDigest
    >,
  ) {
    this.prevEntry = msg.entry.entry;
    this.prevToken = msg.staticTokenHandle;

    this.isAwaitingTermination = true;
  }

  onTerminatePayload(msg: MsgReconciliationTerminatePayload) {
    this.isAwaitingTermination = false;
    if (msg.isFinal) {
      this.announcedEntriesExpecting = false;
    }
  }

  isExpectingPayloadOrTermination() {
    return this.isAwaitingTermination;
  }

  isExpectingReconciliationSendEntry() {
    return this.announcedEntriesExpecting;
  }

  getPrivy(): ReconciliationPrivy<NamespaceId, SubspaceId, PayloadDigest> {
    return {
      prevRange: this.prevRange,
      prevSenderHandle: this.prevSenderHandle,
      prevReceiverHandle: this.prevReceiverHandle,
      prevEntry: this.prevEntry,
      prevStaticTokenHandle: this.prevToken,
      announced: {
        range: this.announcedRange,
        namespace: this.announcedNamespace,
      },
    };
  }
}
