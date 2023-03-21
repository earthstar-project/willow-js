# Soilsun Spec

A fairly minimalistic earthstar knock-off.

## Concepts

Soilsun requires usage of cryptographic signatures and hash functions. The
specification is generic in the particular choice of primitives. In the
following, we simply speak of "public keys", "secret keys", and "signatures" for
some digital signature schemes, and of "digests" for the output of some hash
function. The length of digests is denoted by "digest-length". Using the
specification requires instantiating it with a concrete digital signature scheme
and a concrete hash function.

An **author** consists of a public key and a corresponding secret key, and
possibly a sentient entity that wants to distribute some data I guess.

A **namespace** is a collaboratively maintained, mutable key-value mapping.
Every namespace is identified by a public key, the **namespace id**. Shares can
be modified concurrently by different authors, hence they only exist as an
abstraction, not as actual data structures. Those would be _replicas_.

A **replica** is a snapshot of a namespace at a particular moment in
(distributed) time, usually backed by a persistent database. More precisely, a
_replica_ is a collection of _signed entries_. A **signed entry** consists of an
_entry_ (to be defined later), a signature over the _entry_ with the private key
of the namespace, and a signature over the _entry_ with the private key of the
author.

An **entry** is a pair of a _record identifier_ and a _record_.

A **record identifier** is a tuple of:

- The **namespace id**, which is the _namespace id_ of the _namespace_ to which
  it belongs.
- The **author id**, which is the _public key_ of the author.
- The **timestamp**, which is a 64 bit integer (interpreted as microseconds
  since the Unix epoch).
- The **path**, a bitstring of length at most 2048.

A **record** is a tuple of:

- The **length**, a 64 bit integer.
- The **hash**, an integer of _digest-length_ bits.

The _length_ and _hash_ of a _record_ are intended to be the length and digest
of some bitstring. But we are not even going to pretend in the slightest that
this could be enforced.

## Merging

A **merge** takes a **namespace id** `s` and two _replicas_ `r1` and `r2`, and
deterministically maps these inputs to a _replica_ `r` as follows:

- `r` starts as the union of the _signed entries_ of `r1` and `r2`.
- Then, remove all _entries_ whose _namespace id_ is not `s`.
- Then, remove all _signed entries_ with at least one invalid signature.
- Then, for each set of _entries_ with equal _author ids_ and equal _paths_,
  remove all but those with the highest _timestamp_.
- Then, for each set of _entries_ with equal _author ids_, equal _paths_, and
  equal _timestamps_, remove all but the one whose record has the greatest
  _hash_ component.

## Cryptographic Primitives and Encodings

The encodings for signing/hashing the concepts of soilsun are as follows:

- Encode bitstrings (including record data) as plain bitstrings - no ascii, no
  length, no nothing.
- Encode fixed-width integers (including keys, signatures, digests, timestamps)
  as big-endian.
- Encode _records_ as the encoding of the _length_ followed by the encoding of
  the _hash_.
- Encode _record identifiers_ as the encoding of the _namespace id_ directly
  followed by the encoding of the _author id_ directly followed by the encoding
  of the _timestamp_ directly followed by the encoding of the _path_.
- Encode _entries_ as the encoding of the _record identifier_ directly followed
  by the encoding of the _record_.
