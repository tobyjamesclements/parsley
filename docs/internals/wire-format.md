# Wire format

All binary encodings are big-endian. All lengths are in bytes.

## `parsley-causal-clock` header

The public `CausalClock` facade and the internal `ParsleyVectorClock` (node frontier) share this
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

Size: `5 + 28 × n` bytes. The wire version is `0x01`, and deserialization throws `IllegalStateException` on a mismatch.

Topic IDs are Kafka `Uuid` values stored as two `long` fields (most-significant bits, then least-significant bits).

On a forwarded business record this header carries the producing node's **outbound stamp**
(`ParsleyChannels.stamp()`: completeness ∪ ownOutputs ∪ highestDelivered), not just the record's
own dependencies. The encoding is identical; only the value's meaning differs by context.

## `_parsley_null_message` header (protocol null message)

A protocol null message is a record with a null value, keyed with the triggering record's key, carrying two headers: `_parsley_null_message` (an empty-byte marker) and `parsley-causal-clock` (the emitting node's completeness frontier, encoded exactly as above). A node emits one in place of a business record when a delivered input produced no downstream output, so completeness still propagates through non-emitting layers. The key is reused so the null message routes to the same partition the record's business output would; consumers identify it by the header, never by its key. Consumers identify a null message by the presence of the `_parsley_null_message` header (`CausalClock.isNullMessage`); a non-Parsley consumer sees a tombstone-shaped record and should skip it.

## Buffer record (`ParsleySerializer` v3)

The `{ns}-buffer` state store value has two layers: `StoreBackedBufferStore` prepends the buffer-admission
time to the `ParsleySerializer`-encoded record.

```
[bufferedAt    :8]   wall-clock time (epoch millis) the record was admitted to the buffer
[ParsleySerializer v3 payload, below]
```

The `ParsleySerializer` payload is what is passed to and decoded from `serialize` and `deserialize`.
The source coordinate and dependency clock are written as typed fields rather than headers, and only
the user's headers are carried. The key and value bytes are produced by the Serde resolved from the
typed source topic. The payload's own `[timestamp:8]` field is the record's original Kafka timestamp,
which is distinct from the outer `bufferedAt` and written independently of it.

```
[version       :1]   0x03
[timestamp     :8]   the record's original Kafka timestamp (not bufferedAt above)
[topic-len     :2]
[topic         :topic-len]   UTF-8 source topic
[topicId       :16]          topicId.MSB then topicId.LSB
[partition     :4]
[offset        :8]
[deps-len      :4]   -1 if absent (the empty clock still encodes as 5 bytes)
[deps          :deps-len]    parsley-causal-clock encoding
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

`ParsleyCausalBroadcast`'s startup index rebuild decodes only the outer `bufferedAt` and the `deps` field,
through `ParsleySerializer.deserializeIndexMetadata`, and never the key or value bytes. A record whose
user serde can no longer decode it therefore does not block restore or the drain scan — it only fails
once actually forwarded, via `ParsleySerializer.deserialize`.

## Candidate-index key

The `{ns}-candidate-index` store maps coordinate+offset+recordId to an empty presence marker. The 36-byte key uses big-endian encoding throughout so that RocksDB lexicographic ordering produces a range scan for all records waiting on a given coordinate up to a given offset.

```
[topicId MSB    :8]
[topicId LSB    :8]
[partition      :4]
[requiredOffset :8]
[recordId       :8]   buffer insertion sequence
```

## State store names and serdes

The namespace is the stage name `CausalTopology#assemble` derives (or an explicit name from
`CausalStream#process(name, supplier)`), used internally as `ParsleyProcessorSupplier.builder(...).addBufferStore(name)`.

| Store | Key serde | Value serde | Purpose |
|---|---|---|---|
| `{ns}-frontier` | `String` | `byte[]` | Single entry at key `"f"`: the combined `ParsleyChannels` blob — the node's contiguous delivered frontier clock, the per-input-channel clocks, the highest-received offsets, the carried-ancestry clock, the declared input set, the own-outputs clock, and the declared sink set (see below) |
| `{ns}-buffer` | `Long` | `byte[]` | Insertion sequence -> `bufferedAt` + serialised `ParsleyMessage` |
| `{ns}-candidate-index` | `byte[]` | `byte[]` (empty) | 36-byte composite key -> presence marker |
| `{ns}-forwarded-index` | `byte[]` | `byte[]` (empty) | 28-byte `(topicId, partition, offset)` key -> presence marker: offsets forwarded ahead of the contiguous frontier |

All four stores are persistent and changelog-backed. Changelog topic names follow the Kafka Streams pattern: `{applicationId}-{storeName}-changelog`.

### The `{ns}-frontier` `"f"` value

`ParsleyChannels` folds its persisted structures into the single `"f"` value (loaded once at init,
rewritten on change; the changelog dedups repeated puts by key per commit):

```
[frontier-clock-len:4][frontier ParsleyVectorClock bytes]
[channel-count:4]
  per channel:
  [topicId MSB:8][topicId LSB:8][partition:4][channel-clock-len:4][channel ParsleyVectorClock bytes]
[highest-received-count:4]
  per channel:
  [topicId MSB:8][topicId LSB:8][partition:4][offset:8]
[carried-ancestry-len:4][carried-ancestry ParsleyVectorClock bytes]
[input-count:4]
  per declared input:
  [name UTF][topicId MSB:8][topicId LSB:8]
[own-outputs-len:4][own-outputs ParsleyVectorClock bytes]
[sink-count:4]
  per declared sink:
  [name UTF][topicId MSB:8][topicId LSB:8]
```

The sections, in order:

- **Frontier clock** — the node's contiguous delivered frontier.
- **Channel clocks** — per input channel `(topicId, partition)`, the dependencies advertised on it,
  max-merged over the records and null messages received. `completeness()` — the max-merge of the
  frontier clock, the carried ancestry, and every channel's advertised clock, so a single genuine
  witness to a coordinate is enough — is computed from this value in memory.
- **Highest-received offsets** — per input channel, the highest offset ever physically received,
  making the bridge's skip detection exact across a restart.
- **Carried-ancestry clock** — causal ancestry re-homed from coordinates that have left the node's
  consumption scope; stamp-feeding state, so dropping it on restart would under-claim every
  subsequent stamp.
- **Declared input set** — the input topics (name → UUID) this state was written under, which is
  what makes a scope change detectable at the next init.
- **Own-outputs clock** — the node's own acknowledged output positions. Best-effort by design:
  acknowledgements arriving after the transaction's last persist are missing from the committed
  blob (state store caches flush before the producer flush completes acks), so a restored clock
  can trail by one transaction; the init-time sink end-offset seed re-covers it.
- **Declared sink set** — the sink topics (name → UUID) the own-outputs clock was written under,
  which is what lets the next init heal the trailing acknowledgements of a topic that is no longer
  a sink.

Each section past the channel clocks is trailing and optional on read, so a blob written by an
older layout loads with that section empty. There is no cross-version compatibility beyond that:
pre-1.0 versions have no upgrade path.

The forwarded-offset index stays its own keyed store (`{ns}-forwarded-index`): it is growable and
order-sensitive, so folding it into `"f"` would increase Rocks I/O.

## Topic UUIDs

Topic UUIDs are not derived or guessed. They are resolved from the broker through `AdminClient`. The
processor resolves them at `init()` from the task's `appConfigs()` for every topic registered as a
`ParsleySource` on `ParsleyProcessorSupplier.builder(...)`. If a registered topic does not exist on the
broker, resolution fails fast with `IllegalStateException` rather than falling back to a guess.

The real UUID Kafka assigned to the topic is what's used. A topic deleted and recreated with the
same name gets a new UUID, so records stamped against the old incarnation correctly fail to satisfy
dependencies on the new one.

When building `CausalClock` explicitly at the edge, topic names are resolved to UUIDs
internally through `CausalClock.using(props)` / `.builder(props)`, which cache each lookup.
The UUID names a coordinate in the dependency clock.

Tests without a live broker (`TopologyTestDriver`, unit tests) may use any stable `Uuid`, e.g.
`Uuid.randomUuid()` via `CausalClock.using(Map.of(...))`, as long as the same value is used
consistently wherever that topic's identity is referenced.
