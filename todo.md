## Entries

- [x] encoding entry
- [x] decoding entry
- [x] signing entry
- [x] verifying entry

## Replicas

- [ ] **Replica.ingest** - put a signed entry into a replica
- [ ] **Replica.set** - create (or update) a signed entry from some parameters
      and ingest it.
- [ ] **Replica.query** - you need to be able to query subsets of entries out at
      this level, right?
- [ ] **Replica.liveQuery** - we really need to be able follow changes to the
      replica live.
- [ ] **Replica.forget** - forget some signed entries.

(is the idea that a Willow implementation leaves the storing and retrival of
entries' associated data up to the client? So everyone has to implement their
own blob store?)

## Merging

okay I'm stopping here.
