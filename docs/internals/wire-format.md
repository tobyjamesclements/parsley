# Wire format

All binary encodings are big-endian. All lengths are in bytes.

## `parsley-causal-dependencies` header

The public `CausalDependencies` facade and the internal `ParsleyClock` (node frontier) share this
encoding.

```
[version    :1]   0x01
[count      :4]   number of entries
per entry:
  [topicId MSB:8]
  [topicId LSB:8]
  [partition  :4]
  [offset     :8]
```

Size: `5 + 28 × n` bytes. Wire version `0x01`; deserialization throws `IllegalStateException` on mismatch.

Topic IDs are Kafka `Uuid` values stored as two `long` fields (most-significant bits, then least-significant bits).

## Buffer record (`ParsleySerializer` v3)

Records held in the `{ns}-buffer` state store are serialised with `ParsleySerializer`. The source
coordinate and dependency clock are written as typed fields (not headers); only the user's headers
are carried. The key/value bytes are produced by the Serde resolved from the typed source topic.

```
[version       :1]   0x03
[timestamp     :8]
[topic-len     :2]
[topic         :topic-len]   UTF-8 source topic
[topicId       :16]          topicId.MSB then topicId.LSB
[partition     :4]
[offset        :8]
[deps-len      :4]   -1 if absent (the empty clock still encodes as 5 bytes)
[deps          :deps-len]    parsley-causal-dependencies encoding
[header-count  :4]
per user header:
  [key-len     :2]
  [key         :key-len]
  [value-len   :4]   -1 if value is null
  [value       :value-len]
[key-len       :4]   -1 if key is null
[key           :key-len]
[value-len     :4]   -1 if value is null
[value         :value-len]
```

## Position-index key

The `{ns}-position-index` store maps coordinate+offset+recordId to an empty presence marker. The 36-byte key uses big-endian encoding throughout so that RocksDB lexicographic ordering produces a range scan for all records waiting on a given coordinate up to a given offset.

```
[topicId MSB    :8]
[topicId LSB    :8]
[partition      :4]
[requiredOffset :8]
[recordId       :8]   buffer insertion sequence
```

## Internal record headers

These headers carry the source coordinate through the consumer's outbox topic. They are written by
`ParsleyMessage.toForwardHeaders()` (only on the `ParsleyConsumer` outbox path, where the Kafka
record's own topic/partition/offset are the outbox's, not the source's) and stripped by
`ParsleyConsumer.poll()` before the record is returned to the application. The processor path never
emits them.

| Header | Encoding |
|---|---|
| `_parsley_src_topic` | UTF-8 string |
| `_parsley_src_topic_id` | 16 bytes: `topicId.MSB` then `topicId.LSB` |
| `_parsley_src_partition` | 4-byte big-endian int |
| `_parsley_src_offset` | 8-byte big-endian long |

## State store names and serdes

The default namespace is `parsley`. It is configurable via `CausalProcessors.builder(...).storeName(ns)`.

| Store | Key serde | Value serde | Purpose |
|---|---|---|---|
| `{ns}-frontier` | `String` | `byte[]` | Single entry at key `"f"`: serialised `ParsleyClock` |
| `{ns}-buffer` | `Long` | `byte[]` | Insertion sequence -> serialised `ParsleyMessage` |
| `{ns}-position-index` | `byte[]` | `byte[]` (empty) | 36-byte composite key -> presence marker |

All three stores are persistent and changelog-backed. Changelog topic names follow the Kafka Streams pattern: `{applicationId}-{storeName}-changelog`.

## Topic UUIDs

Topic UUIDs are not derived or guessed — each one must be registered explicitly via
`CausalTopic(topic, uuid)`, passed to `CausalProcessors.builder(...)` /
`CausalConsumers.builder(...)` / `CausalProducers.builder(...)` via `.addCausalTopic(s)(...)`. If a
topic's UUID is not registered, resolution fails fast with `IllegalStateException` rather than
falling back to a guess.

In a live topology, the UUID is typically resolved from the broker via `AdminClient` at startup
(e.g. `CreateTopicsResult.topicId(topic)` or `DescribeTopicsResult`) — the real UUID Kafka assigned
to the topic. A topic deleted and recreated with the same name gets a new UUID, so records stamped
against the old incarnation correctly fail to satisfy dependencies on the new one.

Tests without a live broker (`TopologyTestDriver`, unit tests) may use any stable `Uuid`, e.g.
`Uuid.randomUuid()`, as long as the same value is used consistently wherever that topic's identity
is referenced.
