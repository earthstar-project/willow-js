# Requirements

Here are some non-trivial things _not_ mandated by the Willow spec which this
library should do.

## Replica drivers

Replicas should be able to use drivers which let them persist and retrieve data
using different storage technologies.

- [ ] In-memory
- [ ] Something which can be used from Deno
- [ ] IndexedDB

## Sync transport drivers

A sync session should be able to use different transport drivers, e.g. HTTP,
TCP.

- [ ] Some kind of in-process, local adapter.
- [ ] WebSocket
- [ ] TCP

## Replica events

It must be possible to subscribe to some stream of events from a replica,
primarily for record updates. This makes it possible to write interfaces which
update as the underlying data does.

## Sync events

It must be possible to subscribe to some stream of events from a sync session,
with detailed information concerning the number of records requested and
received, as well as detailed information on transfers of record payloads.
