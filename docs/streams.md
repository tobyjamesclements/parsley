# Streams integration

`CausalProcessors` wraps a standard Kafka Streams `Processor` so that its state-store reads,
writes, and `forward` calls all execute behind the causal guarantee — transparently, with no
`CausalProducer` on egress (Streams sinks carry the stamped header out to the topic).

## Building a causal processor

Write an ordinary `Processor<K, V, KOut, VOut>` and wrap its supplier with `CausalProcessors.builder`:

```java
ProcessorSupplier<String, Order, String, Enriched> user = new ProcessorSupplier<>() {
    public Processor<String, Order, String, Enriched> get() { return new EnrichOrder(); }
    public Set<StoreBuilder<?>> stores() { return Set.of(pricesStateBuilder); }  // your own stores
};

CausalProcessorSupplier<String, Order, String, Enriched> causal =
        CausalProcessors.builder(user, CausalBufferLimit.ofDuration(limit))
                .serdes(Serdes.String(), orderSerde)
                .build();

builder.stream(List.of("prices", "orders"), Consumed.with(Serdes.String(), orderSerde))
       .process(causal)
       .to("output-topic");
```

Parsley registers its own internal state stores (causal buffer, frontier store) alongside any
stores declared by your `ProcessorSupplier.stores()`. You never interact with these stores
directly.

## Preconditions

Two preconditions apply before the guarantee holds:

**1. Closed effects.** Your `Processor.process()` must produce all side effects through
`ProcessorContext.forward()`. Any effect that escapes the processor (a direct database write, an
HTTP call that is not gated on the frontier) can act on a causal premise the consumer has not
confirmed.

**2. Co-partitioning.** All causally related topics must be co-partitioned so that a single task
instance owns the complete partition set for a related group. See
[Co-partitioning](concepts.md#co-partitioning) in Concepts.

The guarantee holds for every record stamped `SATISFIED`. A record stamped `EVICTED` — the
configured `CausalBufferLimit` fired before its dependencies were satisfied — is forwarded anyway;
check `CausalResult.fromRecord(record)` in your own `process()` if your application needs to react
to that case.

## Restart and recovery

The causal buffer and frontier are stored in durable state stores. On restart or rebalance:

- The frontier is restored to the position it held before shutdown — exactly the frontier at
  which the last forwarded record was confirmed.
- Held records are re-evaluated against the restored frontier; no re-fetch from the broker is
  required.

## Operating notes

- A causal processor only helps when records arrive from multiple partitions concurrently (multiple
  topics, or multiple partitions on one instance). With a single partition from a single topic,
  Kafka already gives total order and Parsley only adds overhead.
- Sustained buffer growth (records being held without release) suggests a lagging partition or a
  co-partitioning issue. Each eviction is logged with the causal gap, which identifies the specific
  coordinate that was missing at eviction time.
