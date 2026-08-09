# Causal metadata: wire representation

This document is the normative, stable definition of the metadata a parsley-sent message carries (SPEC Structural 5).
It stands alone: nothing here depends on any implementation's internals. Any reader can decode a message's key and
value with the application's own codecs and ignore this metadata entirely (SPEC Safety 5); any implementation of this
format can decide deliverability from what is defined here.

## Placement

Causal metadata is carried in exactly one Kafka record header (SPEC Assumption 6):

* **Header key**: `parsley.causes` (ASCII).
* The header key prefix `parsley.` is reserved. Applications must not attach headers with this prefix; parsley
  refuses emissions that do. This is what makes the metadata distinguishable, by construction, from metadata
  applications or other systems attach (SPEC Structural 5).
* Every message a parsley process sends carries this header, even when it expresses no causes.
* A message with **no** `parsley.causes` header has no causes and is immediately deliverable (SPEC Safety 6).
* A message with **more than one** `parsley.causes` header, or one that fails the grammar below, is **undecodable**:
  the receiver must fail closed, and must not treat the metadata as absent (SPEC Safety 7).

## Value grammar

All integers are big-endian. Version 1 is the only version.

```
value      := version entryCount entry*
version    := uint8                  -- 0x01
entryCount := int32                  -- number of entries, >= 0
entry      := topicId partition position
topicId    := 16 bytes               -- Kafka topic ID (UUID): most significant 8 bytes, then least significant 8
partition  := int32                  -- >= 0
position   := int64                  -- >= 0, a Kafka offset on that topic-partition
```

Constraints, all mandatory; violation of any makes the value undecodable:

1. The version byte is `0x01`. Any other value is undecodable (readers must not guess forward compatibility).
2. `entryCount` is non-negative and the value contains exactly `entryCount` entries — no trailing bytes.
3. Entries are strictly ascending in the unsigned lexicographic order of their 20-byte channel encoding (the 16
   topic-ID bytes followed by the 4 partition bytes, compared left to right as unsigned bytes): at most one entry
   per channel, and the encoding of a given cause set is canonical (byte-for-byte unique).
4. `partition` and `position` are non-negative.

## Meaning

Each entry `(topicId, partition, position)` expresses causes on one channel (SPEC Structural 11): the channel is the
topic-partition identified by topic ID — so a topic deleted and recreated under the same name is a *different*
channel (SPEC Assumption 2) — and the entry stands for every cause of this message on that channel whose position is
at or below `position` (compression per SPEC Structural 13; expression "at or above the cause's" per SPEC
Terminology).

A message's metadata expresses **every** cause of the message whose position is at or above its channel's earliest
retained position at send time, including causes known to the sender only from the metadata of messages it had
received but not yet delivered (SPEC Structural 15). Entries never name the sending process (SPEC Structural 11) and
never name a position that had not been assigned when the message was sent (SPEC Structural 12). A message sent to a
channel its sender also receives from never carries an entry for its own channel at or above its own position
(SPEC Structural 14).

A receiver may deliver a message only when, for every entry naming a channel in the receiver's received-channel set,
every position on that channel up to and including the entry's position has either been delivered at that receiver or
will never yield a message it receives. Entries naming channels outside the receiver's received-channel set — which
includes dead incarnations of recreated topics — impose no constraint there (SPEC Liveness 4), though the receiver
must still re-express them on its own sends while they can still matter.

## Stability

The header key, the reserved prefix, and the version-1 grammar above are frozen. Any change to the grammar requires a
new version byte; readers encountering an unknown version fail closed rather than guess.
