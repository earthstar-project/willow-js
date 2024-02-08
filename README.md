# willow-js

This is a reference implementation of the Willow protocol written in TypeScript.
**It is a work in progress**!

Want to follow along with development, ask questions, or get involved yourself?
Come and join us on the
[Earthstar Project Discord](https://discord.gg/6NtYzQC2G4).

Here is what has been implemented:

- `Store`
  - Insertion of entries + payloads as per the Willow Data Model
  - Querying of entries + payloads with `Area`s
  - Summarisation (fingerprint generation, used for sync) of `Area`s
  - Entry driver system
    - KV store driver
      - KV store adapter system
        - Deno.KV driver
    - In-memory driver
    - Payload driver system
      - Memory driver
      - Filesystem driver
    - 3d storage interface
      - "Triple storage" 3d storage implementation
        - Summarisable storage interface for data structures which make
          fingerprint generation more efficient
          - Monoid Red black tree (in-memory)
          - Monoid skiplist (persisted in KV storage)
  - Prefix iterator system for efficient prefix operations
    - Radix tree (in-memory)
    - Key iteration (persisted in KV storage)
- Path encryption utilities

And here is what remains:

- Sync
  - Resource control
  - Commitment scheme
  - Private area intersection
  - 3d range-based set reconciliation
  - Payload transmission
  - Post-reconciliation forwarding
  - WebSocket transport driver
  - QUIC transport driver
- IndexedDB KV driver

## Dev

Deno is used the development runtime. Run `deno task test` to run tests.
