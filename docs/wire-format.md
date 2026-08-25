# Wire format

This page is the normative definition of the causal metadata a Parsley-sent message carries.
It stands alone. Any reader can decode a message's key and value with the application's own
codecs and ignore this metadata. Any implementation of this format can decide deliverability
from what is defined here.

## Placement

Causal metadata travels in exactly one Kafka record header.

The header key is `parsley.causes`, in ASCII. The header key prefix `parsley.` is reserved.
Applications must not attach headers with this prefix, and Parsley refuses emissions that do.
This is what makes the metadata distinguishable by construction from headers attached by
applications or by other systems.

Every message a Parsley process sends carries this header, including where it expresses no
causes.

A message with no `parsley.causes` header has no causes and is immediately deliverable. A
message with more than one such header, or one failing the grammar below, is undecodable: the
receiver fails closed, and must not treat the metadata as absent.

## Grammar

All fixed-width integers are big-endian. Two versions are defined. Version 1 is the flat
grammar every writer emits today; version 2 is the grouped grammar readers also accept, and
writers adopt at the flip D98 schedules. Both express exactly the same cause sets: the same
channels, the same positions, in the same channel order. A version byte other than `0x01` or
`0x02` is undecodable, and readers must not guess forward compatibility.

### Version 1 — flat

```
value      := version entryCount entry*
version    := uint8      -- 0x01
entryCount := int32      -- number of entries, >= 0
entry      := topicId partition position
topicId    := 16 bytes   -- Kafka topic ID: 8 most significant bytes, then 8 least
partition  := int32      -- >= 0
position   := int64      -- >= 0, a Kafka offset on that topic-partition
```

Every constraint below is mandatory. Violating any one makes the value undecodable.

1. The version byte is `0x01`.
2. `entryCount` is non-negative, and the value contains exactly `entryCount` entries with no
   trailing bytes.
3. Entries ascend strictly in the unsigned lexicographic order of their 20-byte channel
   encoding, being the 16 topic-ID bytes followed by the 4 partition bytes, compared left to
   right as unsigned bytes. At most one entry per channel, so the encoding of a given cause
   set is unique byte for byte.
4. `partition` and `position` are non-negative.
5. `topicId` is not all-zero bytes. The substrate reserves the zero topic ID and never
   assigns it to a channel, so no genuine cause can name it; an entry carrying it is
   undecodable. (A reader-side tightening, not a grammar change: no conforming writer has
   ever produced such an entry, because writers only express channels the substrate named —
   D83 records the reasoning.)

### Version 2 — grouped

The same entries, with each topic ID written once over its partitions and the structural
fields carried as varints. An entry here is one `pair` under its group's `topicId`.

```
value          := version topicCount group*
version        := uint8    -- 0x02
topicCount     := varint   -- number of groups, >= 0
group          := topicId partitionCount pair*
topicId        := 16 bytes -- as in version 1
partitionCount := varint   -- number of pairs in the group, >= 1
pair           := partition position
partition      := varint   -- >= 0
position       := int64    -- >= 0, a Kafka offset on that topic-partition
```

`varint` is unsigned base-128: seven payload bits per byte, lowest bits first, the high bit
set on every byte except the last. Only the minimal spelling is valid, and the values a
varint may spell are exactly 0 to 2³¹ − 1: a terminal byte of zero after the first byte, a
varint longer than five bytes, and a fifth byte carrying anything beyond the low three bits
are each undecodable. The last of these matters even though it looks like more padding — a
32-bit reader that shifts the surplus away would decode `85 80 80 80 10` to the same value
as `05`, two spellings for one value that the padding rule alone cannot see.

Every constraint below is mandatory. Violating any one makes the value undecodable.

1. The version byte is `0x02`.
2. The value contains exactly `topicCount` groups with no trailing bytes, and each group
   exactly `partitionCount` pairs.
3. Topic IDs ascend strictly across groups in the unsigned lexicographic order of their 16
   bytes, so each topic appears at most once.
4. `partitionCount` is at least one, and partitions ascend strictly within their group.
   Together with constraint 3 this lists channels in exactly version 1's order, and with
   minimal varints the encoding of a given cause set is again unique byte for byte.
5. `position` is non-negative.
6. `topicId` is not all-zero bytes, exactly as version 1's constraint 5.

## Meaning

Each entry `(topicId, partition, position)` expresses causes on one channel — in version 2,
each pair carries its group's topic ID. The channel is the topic-partition identified by
topic ID, so a topic deleted and recreated under the same name is a different channel. The
entry stands for every cause of this message on that channel whose position is at or below
`position`.

A message's metadata expresses every cause of the message whose position is at or above its
channel's earliest retained position at send time. This includes causes known to the sender
only from the metadata of messages it had received and not yet delivered.

Entries never name the sending process, and never name a position that had not been assigned
when the message was sent. A message sent to a channel its sender also receives from never
carries an entry for its own channel at or above its own position.

A receiver may deliver a message only when, for every entry naming a channel in the receiver's
received-channel set, every position on that channel up to and including the entry's position
has either been delivered at that receiver or will never yield a message it receives. Entries
naming channels outside that set impose no constraint there, which includes dead incarnations
of recreated topics. The receiver still re-expresses them on its own sends while they can
still matter.

## Stability

The header key, the reserved prefix and both grammars are frozen. Version 1 remains valid,
and is what every writer emits until the writer flip D98 schedules — a flip that begins only
once every reader in the causal closure, non-parsley readers included, accepts version 2.
Any further change requires a new version byte. Readers encountering an unknown version fail
closed.
