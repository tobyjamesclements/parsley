# Streams integration

`CausalProcessors` wraps a standard Kafka Streams `Processor` so that its state-store reads, its
writes, and its `forward` calls all execute behind the causal guarantee. The wrapping is transparent,
and nothing extra is required on egress, because Streams sinks carry the stamped header out to the
topic.

## Building a causal processor

Write an ordinary `Processor<K, V, KOut, VOut>` and wrap its supplier with `CausalProcessors.builder`.

```java
ProcessorSupplier<String, Order, String, Enriched> user = new ProcessorSupplier<>() {
    public Processor<String, Order, String, Enriched> get() { return new EnrichOrder(); }
    public Set<StoreBuilder<?>> stores() { return Set.of(pricesStateBuilder); }  // your own stores
};

CausalProcessorSupplier<String, Order, String, Enriched> causal =
        CausalProcessors.builder(user)
                .addBufferStore("parsley", CausalBufferLimit.ofDuration(limit))
                .addBuffers(List.of("prices", "orders"), Serdes.String(), orderSerde)
                .build();

builder.stream(List.of("prices", "orders"), Consumed.with(Serdes.String(), orderSerde))
       .process(causal)
       .to("output-topic");
```

`addBufferStore(name, limit)` declares the buffer state store. The `name` is the state-store
namespace described below, and `limit` is the eviction trigger. This mirrors how Kafka Streams names
and sizes a store in one place.

Each input topic is registered as a `CausalBuffer` carrying the serdes that the buffer uses to round-
trip held records. The topic's stable UUID is resolved from the broker automatically at startup. When
a topic needs its own serdes, for example a topic carrying a different Avro type, register it with
`.addBuffer(CausalBuffer.of(topic, keySerde, valueSerde))` once per topic instead of using the shared
`addBuffers(topics, key, value)` convenience.

Parsley's own configuration is supplied on the builder the same way Kafka Streams configuration is,
through `.withConfig(key, value)`, `.withConfigs(Map)`, or `.withConfig(Properties)`. It is overlaid
on top of any `parsley.properties` classpath resource. For example:

```java
        .withConfig("parsley.buffer.deserialization.failure.policy", "continue")
```

Parsley registers its own internal state stores, the causal buffer and the frontier store, alongside
any stores declared by your `ProcessorSupplier.stores()`. You never interact with these stores
directly.

## Preconditions

Two preconditions apply before the guarantee holds.

**Closed effects.** Your `Processor.process()` must produce all side effects through
`ProcessorContext.forward()`. Any effect that escapes the processor, such as a direct database write
or an HTTP call that is not gated on the frontier, can act on a causal premise that the consumer has
not confirmed.

**Co-partitioning.** All causally related topics must be co-partitioned so that a single task
instance owns the complete partition set for a related group. See
[Co-partitioning](concepts.md#co-partitioning) in Concepts.

The guarantee holds for every record delivered in causal order. When a held record's dependencies are
still not satisfied and the configured `CausalBufferLimit` fires, the outcome depends on
`parsley.buffer.eviction.failure.policy`. Under the default `fail` policy, Parsley fails the task and
leaves the record buffered for retry. Under `continue`, the record is forwarded out of causal order.
In both cases the firing is logged with the causal gap and counted by a metric rather than signalled
on the record. See [Configuration](configuration.md) for the policy, and register a `CausalAudit`
(see [Audit logging](audit-logging.md)) to receive this event, and every other causal-buffering
event, as a per-record callback instead of a log line.

## Restart and recovery

The causal buffer and the frontier are kept in durable state stores. On a restart or a rebalance the
following happens.

- The frontier is restored to the position it held before shutdown, which is the frontier at which
  the last forwarded record was confirmed.
- Held records are re-evaluated against the restored frontier. No re-fetch from the broker is
  required.

## Operating notes

- A causal processor only helps when records arrive from multiple partitions concurrently, whether
  from multiple topics or from multiple partitions on one instance. With a single partition from a
  single topic, Kafka already provides total order and Parsley only adds overhead.
- Sustained buffer growth, meaning records being held without release, suggests a lagging partition
  or a co-partitioning problem. Each limit firing is logged with the causal gap, which identifies the
  specific coordinate that was missing at the time.
