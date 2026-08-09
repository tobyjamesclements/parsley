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

All integers are big-endian. Version 1 is the only version.

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

1. The version byte is `0x01`. Any other value is undecodable, and readers must not guess
   forward compatibility.
2. `entryCount` is non-negative, and the value contains exactly `entryCount` entries with no
   trailing bytes.
3. Entries ascend strictly in the unsigned lexicographic order of their 20-byte channel
   encoding, being the 16 topic-ID bytes followed by the 4 partition bytes, compared left to
   right as unsigned bytes. At most one entry per channel, so the encoding of a given cause
   set is unique byte for byte.
4. `partition` and `position` are non-negative.

## Meaning

Each entry `(topicId, partition, position)` expresses causes on one channel. The channel is
the topic-partition identified by topic ID, so a topic deleted and recreated under the same
name is a different channel. The entry stands for every cause of this message on that channel
whose position is at or below `position`.

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

The header key, the reserved prefix and the version-1 grammar are frozen. Any change to the
grammar requires a new version byte. Readers encountering an unknown version fail closed.
