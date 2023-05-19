# Requirements

Here are some non-trivial things _not_ mandated by the Willow spec which this
library should do.

## Drivers

Willow, like Earthstar, must be able to run in different runtimes with their own
capabilities. I think the best way to do this is with drivers.

### Replica drivers

Replicas should be able to use drivers which let them persist and retrieve data
using different storage technologies.

- [ ] In-memory
- [ ] Something which can be used from Deno
- [ ] IndexedDB

### Sync transport drivers

A sync session should be able to use different transport drivers, e.g. HTTP,
TCP.

- [ ] Some kind of in-process, local adapter.
- [ ] WebSocket
- [ ] TCP

## Events

> Earthstar used all kinds of homespun solutions for events, but maybe it's time
> to just use
> [EventTarget](https://developer.mozilla.org/en-US/docs/Web/API/EventTarget).

### Replica events

It must be possible to subscribe to some stream of events from a replica,
primarily for record updates. This makes it possible to write interfaces which
update as the underlying data does.

### Sync events

It must be possible to subscribe to some stream of events from a sync session,
with detailed information concerning the number of records requested and
received, as well as detailed information on transfers of record payloads.

# Secret "not a requirement but would be nice" area

## Streaming querying

Earthstar had a `getQueryStream` function which would return the results of a
query as a readable stream. However, this was pretty superficial as all the
results were loaded into memory first. It would be great to have a truly
streaming version where results can be read out of the underlying DB one by one.
This is really handy for building indexes on very large sets of entries.
