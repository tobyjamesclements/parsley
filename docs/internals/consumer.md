# Consumer

`ParsleyConsumer<K,V>` implements `CausalConsumer<K,V>`. It drives a Kafka Streams topology internally and exposes a `poll()`-based API.

## Topology

```
input topics (byte[], byte[])
  -> ParsleyProcessor
       causal engine (buffer / cascade / evict)
       outbox delegate: saves ORIGINAL_DEPENDENCIES, forwards to outbox
  -> outbox topic (byte[], byte[])
     subscribed by internal KafkaConsumer
       poll() reconstructs ConsumerRecord<K,V>
```

The Streams topology never deserialises record keys or values. All records flow as `byte[]` from source to outbox. Deserialisation happens once, in `poll()`, using the Serde resolved by source topic.

## Outbox topic

Name: `{applicationId}-{storeName}-outbox`. Created at startup by `ParsleyTopicAdmin` with a partition count equal to the maximum partition count across the input topics.

The internal `KafkaConsumer` that reads the outbox is configured with:
- `isolation.level=read_committed`
- `auto.offset.reset=earliest`

## Outbox processor

`ParsleyOutboxProcessor` is the only node in the consumer topology. Unlike the Streams decorator
(`ParsleyProcessor` + `ParsleyProcessorContext`), it has no user delegate and does no frontier
stamping — it wants the application to see the producer's *original* dependencies, not a delivery-time
frontier. For each record the engine admits, it forwards (via `ParsleyMessage.toForwardHeaders()`):

- the user headers and the producer's original `parsley-causal-dependencies`, untouched, and
- the `_parsley_src_*` source coordinate, so the original topic/partition/offset can be reconstructed
  on the far side (the outbox record's own coordinate is the outbox's, not the source's).

## `poll()` path

1. Call `outboxConsumer.poll(timeout)` to get raw `ConsumerRecords<byte[],byte[]>`.
2. For each raw record, `ParsleyMessage.fromOutboxRecord(record)`:
    - reads the `_parsley_src_*` headers into the typed source coordinate,
    - decodes `parsley-causal-dependencies` into the typed clock,
    - keeps every other (user) header, dropping the internal `_parsley_*` ones.
3. Resolve `Serde<K>`/`Serde<V>` by source topic and deserialise key/value.
4. Construct `ConsumerRecord<K,V>` at the source partition and offset, with the user headers plus the
   producer's `parsley-causal-dependencies` re-encoded from the typed clock.
5. Group records by source `TopicPartition` and return as `ConsumerRecords<K,V>`.

The returned record's `headers()` carry the original producer dependencies (no internal `_parsley_*`
headers). `CausalDependencies.fromRecord(record)` on a record returned by `poll()` therefore returns
the producer's causal intent.

An evicted record takes the same path as any other: it flows through the outbox topic and is
returned from `poll()` like every other record (delivered out of causal order). Eviction is not
signalled on the record; the engine logs it with the causal gap and counts it via the violation
metric.

## Multiple input topics

Each subscribed topic is registered as a `CausalBuffer` carrying its own serdes (`CausalConsumers.builder(...).addBuffer(CausalBuffer.of(topic, keySerde, valueSerde))`), so topics with different key or value types each resolve their own. All types co-exist in the single outbox topic as `byte[]`, so there is no schema registry conflict. Deserialisation is deferred to `poll()`, where each record is decoded with its source topic's registered serde.
