# Streams integration

`CausalStreamsBuilder`, `CausalTopology`, and `CausalStreams` are three roles mirroring Kafka Streams'
own `StreamsBuilder`, `Topology`, and `KafkaStreams`. Declare one or more causal stages on the builder,
build the topology, then hand it to a `CausalStreams` runtime.

```java
CausalStreamsBuilder builder = new CausalStreamsBuilder();

builder.stream(List.of("prices", "orders"), Serdes.String(), orderSerde)
       .process(new EnrichOrderSupplier())
       .to("enriched-output", Serdes.String(), enrichedSerde);

CausalTopology topology = builder.build();

CausalStreams causalStreams = new CausalStreams(topology, props);
causalStreams.start();
Runtime.getRuntime().addShutdownHook(new Thread(causalStreams::close));
```

## Building a causal topology

`stream(...)` registers one or more source topics as a stage's inputs. Multiple input topics are the
norm — a single `stream(topics, keySerde, valueSerde)` call fans several co-partitioned topics sharing
one serde pair into a single stage. To combine topics that were declared with different serdes, call
`stream(...)` once per serde group and combine the results with `CausalStream#merge`. Key/value serdes
can be omitted (`stream(topic)`, `to(topic)`), deferring to the runtime's `default.key.serde`/
`default.value.serde` — the same convention `KStream` uses.

`process(supplier)` binds a stage's sources to your processor supplier — an ordinary
`ProcessorSupplier<KIn, VIn, KOut, VOut>`. Its declared state stores are unioned with Parsley's own
internal stores (the causal buffer, frontier, candidate index, and forwarded index); you never
interact with Parsley's stores directly. The stage's name, which becomes the causal buffer's state-store
namespace and hence its changelog topic names, is auto-derived from `application.id` and declaration
order unless you name it explicitly with `process(name, supplier)` — give it a stable name if the
topology might be reordered later.

`to(topic)` declares a sink. A stage may declare more than one; `withPartitioner` applies one
`StreamPartitioner` uniformly across every sink the stage declares (default: Kafka's own key-hash
partitioner) so causal sinks in the same stage never drift onto different partitioners. The partitioner
must read only the key, never the value — a protocol watermark carries a null value and reuses its
triggering record's key, so a value-based partitioner cannot route it.

Unlike the Kafka Streams DSL, sources and sinks here take plain key/value `Serde`s rather than
`Consumed`/`Produced`: neither exposes its serdes for reading back, and Parsley's causal buffer needs
the real `Serde` to round-trip a held record across a restart.

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
long as that partitioner reads the key rather than the value.

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
completeness signal. Set `cleanup.policy=delete` on any sink topic of a causal stage. `CausalTopology`
checks this for you at startup; see [Startup validation](#startup-validation).

**Every branch into a node must see every coordinate that node's records depend on.** A node is
delivered a record only once its own contiguous frontier — the positions it has itself delivered —
dominates that record's dependencies; another channel's advertised claim never substitutes for local
delivery. There is no "this dependency is out of scope, treat it as satisfied" fallback: a dependency
naming a coordinate this node has no input channel for fails the task. In particular, a topology-epoch-coordinated deployment must ensure
every member's declared inputs and sinks jointly cover the full coordinated domain; see
[Evolving a running topology](#evolving-a-running-topology) for how `parsley.coordination.domain-topics`
and auto-wired passthrough sources satisfy that without a redundant business subscription.

**`processing.guarantee=exactly_once_v2` is required.** Parsley's crash-safety reasoning narrows an
at-least-once torn-write window across the buffer, frontier, and forwarded-index changelogs to a benign
tear direction, but only exactly-once-v2's transactional commit closes it completely. `CausalTopology`
fails startup with `IllegalStateException` if this is not set — it is not a delivery-guarantee choice
you make independently of the causal guarantee, it is required by it.

## Startup validation

`parsley.topology.validation` controls how a causal processor reacts at startup to a detectable
topology misconfiguration: the causal topics not sharing a partition count, and, for a stage's sink
topics, a `cleanup.policy` that includes `compact`. The default `warn` logs a mismatch and continues,
`strict` fails the task fast, and `off` disables the checks. Each sink is resolved independently, so
one sink that does not exist yet never masks a genuine misconfiguration on a different sink in the same
stage, even under `strict`. See [Configuration](configuration.md) for the full key reference.

## Restart and recovery

The causal buffer and the frontier are kept in durable, changelog-backed state stores. On a restart or
a rebalance the following happens.

- The frontier is restored to the position it held before shutdown, which is the frontier at which
  the last forwarded record was confirmed.
- Held records are re-evaluated against the restored frontier. No re-fetch from the broker is
  required.

## Evolving a running topology

A causal topology sometimes has to change while it runs: add a stage, replace a stage, or recompile
one. A new stage subscribes to its inputs from the earliest offset and replays them from the start, and
the causal frontier is a merge across every node, so a node replaying from offset 0 would otherwise drag
pre-epoch history into the shared frontier. Topology-epoch coordination lets a topology cross a
well-defined **epoch boundary** so a new node adopts the current floor and replays with pre-epoch
history stripped, instead.

Turn it on by setting `parsley.coordination.epoch-events-topic` in the `props` passed to
`CausalStreams`; `application.id` supplies the epoch member identity. `CausalStreams` owns the
coordination runtime for you — there is no separate handle to construct or register.

```java
Properties props = new Properties();
props.put(StreamsConfig.APPLICATION_ID_CONFIG, "enrichment-service");
props.put("parsley.coordination.epoch-events-topic", "parsley-epoch-events");

CausalStreams causalStreams = new CausalStreams(topology, props);
causalStreams.start();
// ... to evolve the running topology through a boundary, from any instance:
causalStreams.requestEpochTransition();
// ... in shutdown:
causalStreams.close(); // drains, then leaves the coordination domain, then stops KafkaStreams
```

The topology's **external source topics** — the entry points produced by systems outside the topology,
on which no epoch marker ever arrives — are derived from the shared log, not configured. Every stage
declares its input channels and sink topics when it joins, and a topic some member consumes but no
member produces is an external source.

Coordination also enforces a stricter contract: every running member's own subscriptions must jointly
cover the whole coordinated domain (every member's inputs and sinks), or the epoch round cannot commit.
A genuine multi-stage pipeline — app A produces a topic only app B consumes — requires each member to
also cover the topics only a sibling touches. Set `parsley.coordination.domain-topics` (comma-separated,
the full coordinated domain) so `CausalTopology` auto-wires, for each stage, a passthrough source for
any domain topic that stage does not otherwise consume or produce: it flows through the ordinary
delivery gate, contributing to the frontier, but is never handed to your processor. This is what
makes a genuinely cyclic topology (A produces to B, B produces back to A) coordinate correctly, without
a redundant business subscription on either side.

The coordination is **leaderless**: there is no coordinator process to deploy. Every application
instance folds the shared epoch-events log identically and agrees on each epoch's floor, which then
propagates **in-band** through the topology so every stage adopts it. A few operational consequences
follow.

- **Entirely optional.** Without `parsley.coordination.epoch-events-topic` a topology runs in epoch 0,
  exactly as a topology with no epoch machinery.
- **A new node blocks until it is admitted.** A stage deployed into an already-running topology waits at
  startup — for an unbounded time, with no timeout — until an epoch computed without it commits, then
  adopts that floor and replays its inputs with pre-epoch history stripped. It never proceeds on an
  unknown floor: if the domain cannot yet commit (an existing member is absent) the join simply waits.
- **A transition blocks until every member has published (no eviction).** A member that is absent —
  crashed, or briefly gone during a restart — is *waited for*, for an unbounded time, rather than
  evicted. Evicting it and committing a floor without it could strand records it still holds below that
  floor and release them before their causes, so the transition holds until the member returns and
  publishes. This trades reconfiguration liveness for causal safety: a crashed member blocks the next
  *transition* (and any new join) until it returns, though ongoing current-epoch processing is
  unaffected. There is one hardcoded membership behavior — block until drained, never evict — it is not
  a pluggable policy. A clean decommission happens automatically inside `CausalStreams#close()`, which
  drains the node's buffer before leaving the domain; a plain restart rejoins as a fresh member and
  waits to be re-admitted.

`requestEpochTransition()` opens a boundary across the currently-running nodes from any instance. The
full protocol — the floored clock, the leaderless log fold, the in-band markers, and the full-mesh
validation — is described in [Topology epochs](internals/topology-epochs.md).

## Operating notes

- A causal processor only helps when records arrive from multiple partitions concurrently, whether
  from multiple topics or from multiple partitions on one instance. With a single partition from a
  single topic, Kafka already provides total order and Parsley only adds overhead.
- Sustained buffer growth, meaning records being held without release, suggests a lagging partition or
  a co-partitioning problem. The `buffer-depth` and `buffer-oldest-buffered-at-ms` gauges (see
  [Configuration](configuration.md)) track it, and each held record's debug-level `Holding` log line
  identifies the specific coordinate still missing at the time.
- The causal buffer is unbounded: a record with an unsatisfied dependency waits, spilling to disk,
  rather than being forced out of causal order. There is no configuration that trades causal order for
  liveness. Monitor buffer depth in production — an unbounded buffer means a genuinely stuck dependency
  (for example, a producing topic that was deleted) grows without limit rather than being dropped.
