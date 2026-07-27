# Wire format

Byte layouts for everything Parsley writes: the one record header, and the state-store
entries. All integers are big-endian. Layouts are versioned where they travel between
processes; pre-1.0, versions change without compatibility aliases.

## The clock

The serialized form of a `Clock`, used both in the record header and inside state values.
Entries are sorted by channel, so equal clocks are byte-identical.

```
byte     version        (currently 1)
int32    entryCount
entry × entryCount:
    int64    topicId, most significant bits
    int64    topicId, least significant bits
    int32    partition
    int64    offsetWatermark
```

A malformed clock — unknown version, negative fields, length mismatch — throws
`CorruptClockException`. A present but undecodable clock always fails the task; it never reads
as empty ([fail closed](../foundations/delivery-gate.md#the-predicate)).

## Record headers

| Header | Value |
|---|---|
| `parsley-clock` | The record's dependency clock, serialized as above. Absent means the producer claims nothing. |

There are no other protocol headers and no protocol records.

## State-store keys

Keys are UTF-8 strings; `<channel>` is `<topicId-uuid>:<partition>`. Values:

| Key | Value layout |
|---|---|
| `scope` | `int32 channelCount`, then per channel `int64 msb, int64 lsb, int32 partition`; `int32 sinkCount`, then per sink `int64 msb, int64 lsb` |
| `f/<channel>` | `int64` frontier offset |
| `cc/<channel>` | A serialized clock (the channel's advertised view) |
| `ca` | A serialized clock (carried ancestry) |
| `q/<channel>/m` | `int64 headIndex`, `int64 tailIndex` (one past the last held entry) |
| `q/<channel>/e/<index>` | A held record (below); `<index>` is 16 lower-case hex digits, zero-padded so lexicographic order is numeric order |

## Held record

A record held in a channel's queue persists verbatim, so a restart restores exactly what was
fetched:

```
int64    offset
int64    timestamp
int32    clockLength
bytes    clock                    (serialized clock; empty clock when the header was absent)
int32    keyLength                (-1 = null key)
bytes    key
int32    valueLength              (-1 = null value)
bytes    value
```

Non-Parsley headers are not yet part of the envelope; a held record is delivered with empty
headers. This is a known limit of the current cut.

## Not persisted

`ownOutputs` (re-seeded from sink end offsets at every init, which dominate any persisted
copy) and the position-known watermark (re-reported by the host's position advances). Their
absence from the store is deliberate; see [state and recovery](../design/state.md).
