# State

## Ordering state

Each process owns one reserved Kafka Streams `KeyValueStore<Bytes, byte[]>`, named with the
reserved prefix `__parsley.`.

| Key | Value |
|---|---|
| `v` | Store format version |
| `f` + channelId | `fedUpTo`, int64 |
| `c` + channelId | Frontier position, int64 |
| `p` + channelId | Delivered causal past, int64 |
| `n` + topic name | Topic identity recorded for that name |
| `h` + channelId + position | Held message: key, value, headers, decoded causes, timestamp |

`channelId` is 20 bytes, being the topic identity followed by the partition, big-endian. Each
key class carries a distinct leading tag, so one class can be scanned without reading the
rest. Held messages sort by position within a channel, so a prefix scan returns a channel's
hold-back buffer in order.

Both the key layout and the held-message encoding are versioned. A version this build does
not recognise stops the process.

## Persistence and recovery

The store is persistent and changelogged. Under exactly-once semantics its content commits
atomically with the read positions consumed and the messages sent. This is what carries held
messages across a restart, even though Streams commits read positions past records that were
buffered rather than delivered.

A held message is restored with its key, value, headers and timestamp exactly as received.
Null keys, null values and null header values are each distinguished from empty ones.

The delivered causal past is stored separately from the frontier. The frontier governs what a
process may express on its sends. The delivered past governs what a channel joining the
received set later may deliver, by clamping it above effects already delivered while that
channel was absent. Both are advanced on delivery.

## Isolation from application state

Application state cannot alter ordering state. Application store names may not use the
reserved prefix, and the handler seam exposes only the stores a process declared.
