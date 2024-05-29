/**
 * [Willow](https://willowprotocol.org) is a family of protocols for peer-to-peer data stores. This module provides implementations of the [Willow Data Model](https://willowprotocol.org/specs/data-model/index.html#data_model) and the [Willow General Purpose Sync Protocol](https://willowprotocol.org/specs/sync/index.html#sync).
 *
 * This module — just like the specs it is based on — is highly parametrised. It exports many low-level primitives for others to build their own protocols with. For more information on how to configure these parameters, please see the README.
 *
 * Implementations of the following Willow specifications are available:
 * - {@linkcode Store} - [Willow Data Model](https://willowprotocol.org/specs/data-model/index.html#data_model)
 * - {@linkcode WgpsMessenger } - [Willow General Purpose Sync protocol](https://willowprotocol.org/specs/sync/index.html#sync)
 * - {@linkcode createDrop}, {@linkcode ingestDrop} - [Willow Sideloading protocol](https://willowprotocol.org/specs/sideloading/index.html#sideloading)

 * @module
 */

export type {
  AuthorisationScheme,
  FingerprintScheme,
  IngestEvent,
  IngestEventFailure,
  IngestEventNoOp,
  IngestEventSuccess,
  IngestPayloadEvent,
  IngestPayloadEventFailure,
  IngestPayloadEventNoOp,
  IngestPayloadEventSuccess,
  LengthyEntry,
  NamespaceScheme,
  Payload,
  PayloadScheme,
  SubspaceScheme,
} from "./src/store/types.ts";
export * from "./src/store/store.ts";
export * from "./src/store/events.ts";

export * from "./src/store/storage/types.ts";

export * from "./src/store/storage/entry_drivers/kv_store.ts";

export type { KvBatch, KvDriver, KvKey } from "./src/store/storage/kv/types.ts";
export * from "./src/store/storage/kv/prefixed_driver.ts";
export * from "./src/store/storage/kv/kv_driver_in_memory.ts";

export * from "./src/store/storage/payload_drivers/memory.ts";

export * from "./src/store/storage/prefix_iterators/types.ts";
export * from "./src/store/storage/prefix_iterators/simple_key_iterator.ts";
export * from "./src/store/storage/prefix_iterators/radix_tree.ts";

export type {
  RangeOfInterest,
  Storage3d,
} from "./src/store/storage/storage_3d/types.ts";

export * from "./src/store/storage/summarisable_storage/types.ts";
export {
  Skiplist,
  type SkiplistOpts,
} from "./src/store/storage/summarisable_storage/monoid_skiplist.ts";

// Encryption

export {
  decryptPath,
  decryptPathAtOffset,
  encryptPath,
  encryptPathAtOffset,
} from "./src/utils/encryption.ts";

// Willow General Purpose Sync Protocol
export { IS_ALFIE, IS_BETTY } from "./src/wgps/types.ts";
export type {
  AccessControlScheme,
  AuthorisationTokenScheme,
  ReadAuthorisation,
  SubspaceCapScheme,
  Transport,
} from "./src/wgps/types.ts";
export * from "./src/wgps/wgps_messenger.ts";
export * from "./src/wgps/transports/in_memory.ts";
export * from "./src/wgps/transports/websocket.ts";

export { type PaiScheme } from "./src/wgps/pai/types.ts";

// Sideloading

export {
  createDrop,
  DropContentsStream,
  type DropContentsStreamOpts,
  type DropOpts,
} from "./src/sideload/create_drop.ts";
export { ingestDrop, type IngestDropOpts } from "./src/sideload/ingest_drop.ts";

// Errors

export * from "./src/errors.ts";
