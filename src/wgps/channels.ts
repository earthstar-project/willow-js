import { LogicalChannel, MsgKind } from "./types.ts";

export const msgLogicalChannels: Record<MsgKind, LogicalChannel | null> = {
  [MsgKind.PaiBindFragment]: LogicalChannel.IntersectionChannel,
  [MsgKind.PaiReplyFragment]: LogicalChannel.IntersectionChannel,
  [MsgKind.SetupBindReadCapability]: LogicalChannel.CapabilityChannel,
  [MsgKind.SetupBindAreaOfInterest]: LogicalChannel.AreaOfInterestChannel,
  [MsgKind.SetupBindStaticToken]: LogicalChannel.StaticTokenChannel,
  [MsgKind.ReconciliationSendFingerprint]: LogicalChannel.ReconciliationChannel,
  [MsgKind.ReconciliationAnnounceEntries]: LogicalChannel.ReconciliationChannel,
  [MsgKind.ReconciliationSendEntry]: LogicalChannel.ReconciliationChannel,
  [MsgKind.ReconciliationSendPayload]: LogicalChannel.ReconciliationChannel,
  [MsgKind.ReconciliationTerminatePayload]:
    LogicalChannel.ReconciliationChannel,
  [MsgKind.DataSendEntry]: LogicalChannel.DataChannel,
  [MsgKind.DataSendPayload]: LogicalChannel.DataChannel,
  [MsgKind.DataReplyPayload]: LogicalChannel.DataChannel,
  [MsgKind.DataBindPayloadRequest]: LogicalChannel.PayloadRequestChannel,
  [MsgKind.CommitmentReveal]: null,
  [MsgKind.ControlAbsolve]: null,
  [MsgKind.ControlAnnounceDropping]: null,
  [MsgKind.ControlApologise]: null,
  [MsgKind.ControlFree]: null,
  [MsgKind.ControlPlead]: null,
  [MsgKind.ControlIssueGuarantee]: null,
  [MsgKind.PaiRequestSubspaceCapability]: null,
  [MsgKind.PaiReplySubspaceCapability]: null,
  [MsgKind.DataSetMetadata]: null,
};
