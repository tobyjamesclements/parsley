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
receiver fails closed, and must not treat the metadata as absent. A header present with a
null or empty value counts as present and fails the grammar.

## Grammar

All fixed-width integers are big-endian. Version 1 is the only version: entries grouped by
topic, structural fields as minimal varints, positions fixed-width.

```
value          := version topicCount group*
version        := uint8    -- 0x01
topicCount     := varint   -- number of groups, >= 0
group          := topicId partitionCount pair*
topicId        := 16 bytes -- Kafka topic ID: 8 most significant bytes, then 8 least
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

1. The version byte is `0x01`. Any other value is undecodable, and readers must not guess
   forward compatibility.
2. The value contains exactly `topicCount` groups with no trailing bytes, and each group
   exactly `partitionCount` pairs.
3. Topic IDs ascend strictly across groups in the unsigned lexicographic order of their 16
   bytes, so each topic appears at most once.
4. `partitionCount` is at least one, and partitions ascend strictly within their group.
   This lists channels in the unsigned lexicographic order of their 20-byte encoding — the
   16 topic-ID bytes followed by 4 big-endian partition bytes — and with minimal varints
   the encoding of a given cause set is unique byte for byte.
5. `topicId` is not all-zero bytes. The substrate reserves the zero topic ID and never
   assigns it to a channel, so no genuine cause can name it; a group carrying it is
   undecodable. (A reader-side tightening, not a grammar change: no conforming writer has
   ever produced such a group, because writers only express channels the substrate named —
   D83 records the reasoning.)
6. `position` is non-negative.
7. `position` is below 2⁶³ − 1. No log reaches that many records, so no genuine cause can
   name it, and an implementation may keep it as an in-band marker of its own — this one
   records a deleted channel with it. (A reader-side tightening in the manner of
   constraint 5: no conforming writer has ever produced such a pair; D105 records the
   reasoning.)
8. `position` is the offset of a committed record on that channel: a record the substrate
   has stored and serves to a `read_committed` reader — never a control record, a record
   of an aborted transaction, or an offset at or beyond the log's end. A Parsley process
   satisfies this by construction, since its frontier holds only positions it received
   records at and positions it learned from received metadata, and so does every
   `CausalPast` token. A writer naming any other position — the log-end offset is the
   natural naive stamp — is out of contract: no reader is obliged to settle it, and a
   receiver holds the message, visibly in its status, until a later record on that
   channel settles the position (D115). Unlike constraints 1–7 this one is not decidable
   from the bytes, so no reader refuses it; it is the contract a writer signs.

## Meaning

Each pair, with its group's topic ID, expresses causes on one channel as the entry
`(topicId, partition, position)`. The channel is the topic-partition identified by topic ID,
so a topic deleted and recreated under the same name is a different channel. The entry
stands for every cause of this message on that channel whose position is at or below
`position`.

A message's metadata expresses every cause of the message, other than causes on channels the
sender has learned no longer exist. This includes causes known to the sender only from the
metadata of messages it had received and not yet delivered, and causes whose records
retention has since discarded: a cause is dropped only with its channel, never for its age.

Entries never name the sending process, and never name a position that had not been assigned
when the message was sent. A message sent to a channel its sender also receives from never
carries an entry for its own channel at or above its own position.

A receiver may deliver a message only when, for every entry naming a channel in the receiver's
received-channel set, every position on that channel up to and including the entry's position
has either been delivered at that receiver or will never yield a message it receives. Receipt
of a record at an offset establishes that for every offset below it, since a channel is fed in
order; the channel's deletion establishes it for every offset; and the position the receiver
started reading the channel from establishes it for every offset below that. Nothing else
does, and an entry naming an offset no committed record occupies (constraint 8) holds the
message until a later record on the channel settles it. Entries naming channels outside the
received-channel set impose no constraint there, which includes dead incarnations of
recreated topics. The receiver still re-expresses them on its own sends while they can still
matter.

## Stability

The header key, the reserved prefix and the version-1 grammar are frozen. Any change to the
grammar requires a new version byte. Readers encountering an unknown version fail closed.
