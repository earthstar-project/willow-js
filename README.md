# willow-js

This is a implementation of the [Willow protocol](https://willowprotocol.org)
written in TypeScript.

## API

This module is published on JSR. Please see the
[@earthstar/willow page](https://jsr.io/@earthstar/willow) for installation
instructions.

API documentation can be found [here](https://jsr.io/@earthstar/willow/doc).

## Overview

`willow-js` includes:

- A `Store` compliant with the
  [Willow Data model](https://willowprotocol.org/specs/data-model/index.html#data_model),
- A `WgpsMessenger` which syncs data between two stores via the
  [Willow General Purpose Sync Protocol](https://willowprotocol.org/specs/sync/index.html#sync),
- and `createDrop` and `ingestDrop` compliant with the
  [Willow Sideloading protocol](https://willowprotocol.org/specs/sideloading/index.html#sideloading).

This is a low-level module for people to build their own protocols on top of,
like [Earthstar](https://earthstar-project.org). It is an _extremely_ generic
and modular codebase, with many parameters to configure.

### Parameters

These parameters have been abstracted as various `Scheme` interfaces, e.g.
`NamespaceScheme`, `SubspaceScheme`, `FingerprintScheme`. At
`src/test/test_schemes.ts` you can find a full suite of schemes used to
configure willow-js during tests. Use these to experiment with willow-js. When
you want to configure these parameter schemes yourself, please see each scheme's
accompanying documentation.

Using `Store` requires these schemes:

- `NamespaceScheme`
- `SubspaceScheme`
- `PayloadScheme`
- `PathScheme`
- `AuthorisationScheme`
- `FingerprintScheme`

Using `WgpsMessenger` requires all prior schemes, as well as the following:

- `AccessControlScheme`
- `SubspaceCapScheme`
- `PaiScheme`
- `AuthorisationTokenScheme`

### Interfaces

In addition to these parameters, it's possible to configure willow-js to use
different drivers for entry and payload storage, transports during sync, or even
different data structures to write and retrieve data from.

Most of the interfaces are geared around changing the way `Store` works:

- `KvDriver` - an interface for writing and reading data from a key value store.
  We've chosen key-value stores as the lowest common denominator for data
  storage, and these drivers can be used by many different client. See our
  KvDriverMemory, KvDriverDeno, and KvDriverIndexedDB. This is the quickest way
  to adding support for new runtimes to willow-js.
- `SummarisableStorage` - a data structure capable of summarising ranges of
  stored data as a `PreFingerprint` via a `LiftingMonoid`. See `Skiplist` for an
  implementation which reads and writes data using given `KvDriver`.
- `Storage3d` - a data structure to write and read entries from a 3d data
  structure. See `TripleStore` for our only extant implementation of this
  interface, which uses three differently ordered `SummarisableStorage`.
- `EntryDriver` - An interface encompassing all of the above to be directly
  provided to `Store`.
- `PrefixIterator` - provides a means to tell if one path is prefixed by another
  path, and store a bit of arbitrary data with it e.g. the timestamp. See
  `RadixTree` for an example.

`WgpsMessenger` can communicate with peers via different transports using the
`Transport` interface. Currently we have transports for WebSockets and
in-memory.

## `willow-js`' Most Wanted

Here is a list of features we want to implement in willow-js. We welcome all
contributions.

The number of 🌶 emoji next to each item indicates a scientifically measured
estimated challenge level for each of these undertakings, three peppers being
the maximum.

### Storage

- 🌶 A LevelDB driver conforming to `KvDriver`
- 🌶🌶 A payload driver which intelligently stores small payloads in a database,
  and larger payloads on the filesystem, conforming to `PayloadDriver`
- 🌶🌶🌶 A Z-ordered skiplist conforming to `Storage3d` (CLAIMED)

### Sync

WgpsMessenger is currently compliant with the WGPS. However there are a number
of optional enhancements yet to be implemented:

- 🌶 Make WgpsMessenger send payloads below `maximum_payload_size` during
  reconciliation via `ReconciliationSendPayload`
- 🌶 Make it possible to configure an upper byte length limit over which payloads
  are not requested.
- 🌶 Make the threshold at which 3d range-based reconciliation stop comparing
  fingerprints and just return entries user-configurable.
- 🌶 A WebRTC `Transport`.
- 🌶🌶 Make WgpsMessenger track the progress of reconciliation using the `covers`
  field of `ReconciliationSendFingerprint` and `ReconciliationAnnounceEntries`
- 🌶🌶 Make `WgpsMessenger`'s resources user configurable (currently guarantees
  effectively infinite memory to the other peer).
- 🌶🌶 Add events to `WgpsMessenger` so that the progress of a sync session can be
  tracked.
- 🌶🌶 Make `WgpsMessenger` able to add and remove `ReadAuthorisation` during a
  sync session.
- 🌶🌶🌶 Post-reconciliation forwarding of messages using a push-lazy-push
  multicast tree (plumtree), `DataSetMetadata`, and `DataSendEntry`.
- 🌶🌶🌶 Make WgpsMessenger intelligently free handles no longer in use via
  `ControlFree`

## Dev

Deno is used the development runtime. Run `deno task test` to run tests.

---

Want to follow along with development, ask questions, or get involved yourself?
Come and join us on the
[Earthstar Project Discord](https://discord.gg/6NtYzQC2G4).

---

This project was funded through the NGI Assure Fund, a fund established by NLnet
with financial support from the European Commission’s Next Generation Internet
programme, under the aegis of DG Communications Networks, Content and Technology
under grant agreement № 957073.
