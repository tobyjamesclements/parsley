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

## Outbox delegate processor

A second processor node sits between `ParsleyProcessor` and the outbox sink. It runs inside the Streams task before `ParsleyProcessorContext` stamps the frontier:

1. Read `parsley-causal-dependencies` from the inbound record headers.
2. Copy the bytes to `_parsley_original_dependencies`.
3. Forward (at which point `ParsleyProcessorContext.forward()` overwrites `parsley-causal-dependencies` with the delivery-time frontier).

The result in the outbox: both the delivery-time frontier (as `parsley-causal-dependencies`) and the original producer dependencies (as `_parsley_original_dependencies`) are present on the record.

## `poll()` path

1. Call `outboxConsumer.poll(timeout)` to get raw `ConsumerRecords<byte[],byte[]>`.
2. For each raw record:
    - Read `_parsley_src_topic`, `_parsley_src_partition`, `_parsley_src_offset` headers.
    - Resolve `Serde<K>` and `Serde<V>` by source topic.
    - Deserialise key and value.
    - Call `restoreOriginalDependencies(headers)`:
        - Strip all headers with keys starting with `_parsley_`.
        - If `_parsley_original_dependencies` was present, add it back as `parsley-causal-dependencies`.
    - Construct `ConsumerRecord<K,V>` at the source partition and offset.
3. Group records by source `TopicPartition`.
4. Return as `ConsumerRecords<K,V>`.

The returned record's `headers()` contain the original producer dependencies (not the delivery-time frontier). `CausalDependencies.fromRecord(record)` on a record returned by `poll()` therefore returns the producer's causal intent.

An evicted record takes the same path as any other: it is stamped `EVICTED` under the
`parsley-causal-result` header by the causal engine, flows through the outbox delegate and outbox
topic like every other record, and is returned from `poll()` like every other record. Check
`CausalResult.fromRecord(record)` on records returned by `poll()` to distinguish `EVICTED` from
`SATISFIED`.

## Frontier merging

The Streams topology may assign multiple tasks to the same application instance, each with its own `ParsleyProcessor` and its own local frontier. Each processor fires the `CausalFrontierListener` callback whenever its frontier advances.

`ParsleyConsumer` holds an `AtomicReference<CausalFrontier>` initialised to `CausalFrontier.empty()`. The listener callback calls `ref.updateAndGet(existing -> existing.merge(incoming))`. `frontier()` returns the current value of this reference.

This is the only synchronisation point. Kafka Streams runs tasks on different threads; the atomic merge ensures `frontier()` is always a conservative upper bound across all active tasks.

## Multiple input topics

If the input topic set contains multiple topics with different key or value types, the Serde resolver must be configured via `CausalConsumers.builder(...).serdesByTopic(keyFn, valueFn)`. All types co-exist in the single outbox topic as `byte[]`, so there is no schema registry conflict. Deserialisation is deferred to `poll()` where the source topic is known.
