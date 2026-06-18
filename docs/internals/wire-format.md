# Wire format

All binary encodings are big-endian. All lengths are in bytes.

## `parsley-causal-dependencies` header

Same encoding used for both `CausalDependencies` and `CausalFrontier`.

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

## Buffer record (`ParsleySerializer` v2)

Records held in the `{ns}-buffer` state store are serialised with `ParsleySerializer`. The key/value bytes are produced by the Serde resolved from the record's `_parsley_src_topic` header.

```
[version       :1]   0x02
[timestamp     :8]
[header-count  :4]
per header:
  [key-len     :2]
  [key         :key-len]
  [value-len   :4]   -1 if value is null
  [value       :value-len]
[key-len       :4]   -1 if key is null
[key           :key-len]
[value-len     :4]   -1 if value is null
[value         :value-len]
```

All headers on the record (user headers plus `_parsley_*` internal headers) are serialised in order.

## Wait-index key

The `{ns}-wait-index` store maps coordinate+offset+recordId to an empty presence marker. The 36-byte key uses big-endian encoding throughout so that RocksDB lexicographic ordering produces a range scan for all records waiting on a given coordinate up to a given offset.

```
[topicId MSB    :8]
[topicId LSB    :8]
[partition      :4]
[requiredOffset :8]
[recordId       :8]   buffer insertion sequence
```

## Internal record headers

These headers are added by `ParsleyProcessor` at ingest time and stripped before the record is returned to the application.

| Header | Encoding |
|---|---|
| `_parsley_src_topic` | UTF-8 string |
| `_parsley_src_topic_id` | 16 bytes: `topicId.MSB` then `topicId.LSB` |
| `_parsley_src_partition` | 4-byte big-endian int |
| `_parsley_src_offset` | 8-byte big-endian long |
| `_parsley_original_dependencies` | Same encoding as `parsley-causal-dependencies` |

`_parsley_original_dependencies` holds a copy of the producer's original dependencies saved before `ParsleyProcessorContext` overwrites `parsley-causal-dependencies` with the delivery-time frontier. `ParsleyConsumer.poll()` uses it to restore the producer's intent before returning records to the application.

## State store names and serdes

The default namespace is `parsley`. It is configurable via `CausalProcessors.builder(...).storeName(ns)`.

| Store | Key serde | Value serde | Purpose |
|---|---|---|---|
| `{ns}-frontier` | `String` | `byte[]` | Single entry at key `"f"`: serialised `CausalFrontier` |
| `{ns}-buffer` | `Long` | `byte[]` | Insertion sequence -> serialised `ParsleyRecord` |
| `{ns}-wait-index` | `byte[]` | `byte[]` (empty) | 36-byte composite key -> presence marker |

All three stores are persistent and changelog-backed. Changelog topic names follow the Kafka Streams pattern: `{applicationId}-{storeName}-changelog`.

## Topic UUIDs

In a live topology, topic UUIDs are fetched from the Kafka broker via `AdminClient` at startup and passed to the processor via `CausalProcessors.builder(...).topicUuids(map)`.

`CausalPosition.deriveUuid(topicName)` is a fallback used in tests and `TopologyTestDriver`. It computes a deterministic UUID using `UUID.nameUUIDFromBytes(topicName.getBytes(UTF_8))` (UUID version 3). This is not the UUID Kafka assigns to the topic. A topic deleted and recreated with the same name produces the same derived UUID, which would cause records stamped against the old topic to appear satisfied by the new one.
