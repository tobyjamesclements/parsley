# Streams integration

`CausalProcessors` wraps a standard Kafka Streams `Processor` so that its state-store reads, its
writes, and its `forward` calls all execute behind the causal guarantee. The wrapping is transparent,
and nothing extra is required on egress, because Streams sinks carry the stamped header out to the
topic.

`CausalStreams` builds on top of `CausalProcessors` and owns the surrounding topology — see
[The high-level API: CausalStreams](#the-high-level-api-causalstreams) below for when to reach for it
instead.

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

The guarantee holds subject to the conditions below. They apply across the whole processor, not per
record, and Parsley cannot verify most of them, so treat them as a contract on how the topology is
built. Parsley can check one part at startup; see [Startup validation](#startup-validation).

**Your key is your shard.** Partition every causally related topic by the record key, with the same
partition count on each, so that a single task instance owns the complete partition set for a related
group. The key is the unit of causal locality: causally related records must share a key so they land
on the same partition on every topic. Parsley evaluates dependencies only against the partitions a
task owns, so a topology that is not partitioned this way evaluates the completeness frontier against
an incomplete partition set. An advanced user may partition by a coarser function of the key with a
custom `StreamPartitioner`, for example by hashing a `tenant` prefix out of a `tenant:order` key, as
long as that partitioner reads the key rather than the value. A partitioner that reads the value
cannot route a protocol watermark, which carries no value; put the correlation in the key instead.

**Do not change the key across a causal processor.** The key selects the partition, so changing it
moves a record to a different shard and breaks co-partitioning for everything downstream. Key-changing
operations such as a `groupBy` or a join on a derived key belong outside the causally related segment.

**Closed effects.** Your `Processor.process()` must produce all side effects through
`ProcessorContext.forward()`. Any effect that escapes the processor, such as a direct database write
or an HTTP call that is not gated on the frontier, can act on a causal premise that the consumer has
not confirmed.

**Forward uniformly to all children.** A causal processor advertises its progress downstream by
stamping its business output, or by emitting a protocol watermark when the delegate forwards nothing
for a delivered input. That watermark reaches every downstream child. If the delegate routes business
records selectively to some named children and not others, the children that received nothing are not
separately watermarked, so keep a causal processor's forwarding uniform across its children.

**Watermark-bearing topics must not be compacted.** A protocol watermark has a null value, so log
compaction treats it as a tombstone and may delete it before a slow consumer reads it, dropping the
completeness signal. Set `cleanup.policy=delete` on any topic that carries watermarks, which is any
output topic of a causal processor. Parsley does not manage this topic configuration at this level.

**Choose your delivery guarantee.** Parsley stamps each record with its own dependencies, so a
redelivered record re-evaluates identically against the frontier. At-least-once processing is
therefore safe, and exactly-once (`processing.guarantee=exactly_once_v2`) is a choice you make for
delivery deduplication, not a requirement of the causal guarantee.

A single causal stage that reads its sources and forwards to its outputs is sound on its own. A
multi-stage pipeline, where one causal processor's output topic is consumed by another causal
processor, relies on the key being preserved through each stage so that a stage's watermark routes to
the same partition its business records do. This is why the key must not change across a node.

## Startup validation

`parsley.topology.validation` controls how a causal processor reacts at startup to a detectable
topology misconfiguration. The default `warn` logs a mismatch and continues, `strict` fails the task
fast, and `off` disables the checks. A bare `CausalProcessors` decorator only ever sees its own
registered input topics, so it can check one precondition: that they share a partition count. A
`CausalStreams` stage owns its sinks too, so it widens the same check to also cover sink partition
counts and each sink's `cleanup.policy` — see
[The high-level API: CausalStreams](#the-high-level-api-causalstreams) below. See
[Configuration](configuration.md) for the full key reference.

The guarantee holds for every record delivered in causal order. When a held record's dependencies are
still not satisfied and the configured `CausalBufferLimit` fires, the outcome depends on
`parsley.buffer.eviction.failure.policy`. Under the default `fail` policy, Parsley fails the task and
leaves the record buffered for retry. Under `continue`, the record is forwarded out of causal order.
In both cases the firing is logged with the causal gap and counted by a metric rather than signalled
on the record. See [Configuration](configuration.md) for the policy, and register a `CausalAudit`
(see [Audit logging](audit-logging.md)) to receive this event, and every other causal-buffering
event, as a per-record callback instead of a log line.

## The high-level API: CausalStreams

`CausalStreams` is the topology-owning entry point: it builds the `Topology` itself — sources,
processor, and sinks — around the same `CausalProcessors` decorator, rather than handing you a
`CausalProcessorSupplier` to drop into a `StreamsBuilder` you wire yourself. It composes the
decorator; it does not reimplement the causal engine.

```java
CausalQuiesce quiesce = CausalQuiesce.create();

Topology topology = CausalStreams.builder(userSupplier)
        .addBufferStore("parsley", CausalBufferLimit.ofDuration(limit))
        .addSource(CausalBuffer.of("prices", Serdes.String(), priceSerde))
        .addSource(CausalBuffer.of("orders", Serdes.String(), orderSerde))
        .addSink("enriched-sink", "enriched-output", Serdes.String(), enrichedSerde)
        .withQuiesce(quiesce)
        .build();

KafkaStreams streams = new KafkaStreams(topology, props);
streams.start();
```

Reach for `CausalStreams` instead of the low-level decorator whenever a stage needs a guarantee that
requires owning the sinks, which the decorator structurally cannot do on its own:

- **A uniform sink partitioner.** `Builder#withPartitioner` applies one `StreamPartitioner` to every
  sink the stage declares (default: Kafka's own key-hash partitioner), so causal sinks in the same
  stage can never drift onto different partitioners — a real risk once a topology has more than one
  sink and only some calls remember to set a custom one. The partitioner must read only the key,
  never the value, for the same reason as the [shard precondition](#preconditions) above: a protocol
  watermark carries no value to read.
- **Co-partitioning validation across sinks.** `parsley.topology.validation` (see
  [Startup validation](#startup-validation)) folds sink partition counts into the same parity check
  it runs on inputs, and checks each sink's `cleanup.policy` for `compact` — a protocol watermark is
  a null-value record wire-indistinguishable from a compaction tombstone, so a compacted
  watermark-bearing topic can silently drop the completeness signal before a slow consumer reads it.
  Each sink is resolved independently, so one sink that does not exist yet never masks a genuine
  misconfiguration on a different sink in the same stage, even under `strict`.
- **Path integrity by construction.** A stage `CausalStreams` builds is exactly sources → one
  causal-decorated processor → sinks. There is no method on the builder that inserts a plain,
  non-Parsley node in between, so a hop that would silently drop the causal-dependencies header, or
  swallow a non-emitting invocation without a watermark, cannot be constructed through this API.
- **Coordinated graceful shutdown.** Register a `CausalQuiesce` with `Builder#withQuiesce`, call
  `requestQuiesce()` from your own shutdown path, then poll `isSafeToClose()` before calling
  `KafkaStreams#close`. A registered task keeps processing exactly as it does without quiesce — it
  only reports itself drained once its buffer empties through the ordinary delivery path, never by
  fabricating completeness. This is a stall-avoidance optimization, not a correctness requirement:
  every held record is already changelog-backed and survives an ungraceful stop regardless of
  whether quiesce was ever requested.
- **A double-wrap guard.** Passing an already-decorated `CausalProcessorSupplier` back into
  `CausalProcessors.builder(...)` (which `CausalStreams` calls internally) throws immediately,
  rather than silently building a nested double-decoration that would buffer and stamp every record
  twice.

Everything under [Preconditions](#preconditions) still applies unchanged — `CausalStreams` manages
more of the topology for you, but the causal guarantee itself has the same requirements either way.

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
