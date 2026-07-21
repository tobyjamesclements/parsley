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
must read only the key, never the value — a protocol null message carries a null value and reuses its
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

**Every processor between causally related topics must stamp.** Causal order is guaranteed only
along paths where every intermediate processor participates — a Parsley stage, or a client using
the `CausalClock` edge operations. A service that consumes stamped topics and re-produces
unstamped output severs the causal chain: its outputs are causally minimal by definition, and the
severance is undetectable at runtime, because an unstamped record is indistinguishable from a
genuine external event. Producers that only *originate* events need no participation — a record
that stamps nothing claims nothing and is delivered immediately. This is environmental assumption
E3 of the [causal consistency model](internals/causal-consistency.md#environmental-assumptions).

**Forward uniformly to all children.** A causal processor advertises its progress downstream by
stamping its business output, or by emitting a protocol null message when the delegate forwards nothing
for a delivered input. That null message reaches every downstream child. If the delegate routes business
records selectively to some named children and not others, the children that received nothing do not
separately receive a null message, so keep a causal processor's forwarding uniform across its children.

**Null-message-bearing topics must not be compacted.** A protocol null message has a null value, so log
compaction treats it as a tombstone and may delete it before a slow consumer reads it, dropping the
completeness signal. Set `cleanup.policy=delete` on any sink topic of a causal stage. `CausalTopology`
checks this for you at startup; see [Startup validation](#startup-validation).

**Consumed dependencies gate; the rest are ignored.** A node is
delivered a record only once its own contiguous frontier — the positions it has itself delivered —
dominates the record's dependencies on coordinates it consumes; another channel's advertised claim
never substitutes for local delivery. A dependency on a coordinate this node does not consume is
ignored, unconditionally — sound because stamps are transitively complete and merged
unconditionally, so any consumed causal ancestor is claimed directly in the same clock. Each ignore
increments the `parsley.deps.out-of-scope-ignored` metric.

**`processing.guarantee=exactly_once_v2` is required.** Parsley's crash-safety reasoning narrows an
at-least-once torn-write window across the buffer, frontier, and forwarded-index changelogs to a benign
tear direction, but only exactly-once-v2's transactional commit closes it completely. `CausalTopology`
fails startup with `IllegalStateException` if this is not set — it is not a delivery-guarantee choice
you make independently of the causal guarantee, it is required by it.

## Startup validation

`parsley.topology.validation` controls how a causal processor reacts at startup to a detectable
topology misconfiguration: the causal topics not sharing a partition count, and, for a stage's sink
topics, a `cleanup.policy` that includes `compact`. The default `strict` fails the task fast,
`warn` logs the mismatch and continues (the explicit opt-down), and `off` disables the checks. Each sink is checked independently, so
a transient describe failure on one sink never masks a genuine misconfiguration on a different sink
in the same stage, even under `strict`. Sink existence itself is not governed by this key: every
topic a stage declares — inputs and sinks alike — must exist before the application starts, and a
declared sink that cannot be resolved fails startup unconditionally, because own-output stamping
depends on its resolved identity. See [Configuration](configuration.md) for the full key reference.

## Restart and recovery

The causal buffer and the frontier are kept in durable, changelog-backed state stores. On a restart or
a rebalance the following happens.

- The frontier is restored to the position it held before shutdown, which is the frontier at which
  the last forwarded record was confirmed.
- Held records are re-evaluated against the restored frontier. No re-fetch from the broker is
  required.

## Evolving a running topology

A causal topology sometimes has to change while it runs: add a stage, replace a stage, or recompile
one. Joining needs **zero coordination**: a new stage simply starts consuming from wherever the log
starts, its hold-back queue converts arbitrary cross-partition replay arrival into causal delivery
order, and its truthful stamps make its outputs correctly gated everywhere from its first emission.
There is no join barrier, no admission wait, and nothing to configure.

Earlier versions coordinated joins through a topology-epoch subsystem (`requestEpochTransition()`,
the `parsley.coordination.*` keys). It contributed nothing to causal safety and has been removed
entirely: `requestEpochTransition()` no longer exists, and startup fails loudly if a
`parsley.coordination.*` key is present. Delete the keys and any `requestEpochTransition()` calls
when upgrading.

## Operating notes

- **Create every causal topic — sinks included — before the application starts.** Inputs must exist
  for the sources to resolve, and a declared sink must exist too: its UUID and end offsets feed
  own-output stamping, so an unresolvable sink fails startup loudly rather than running with stamps
  that under-claim the node's own outputs. Relying on broker auto-creation at first produce is not
  a supported path for causal sinks.
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
- **Size retention so it never destroys causally-live history.** A record some running or future
  consumer still needs must not be expunged before that consumer has delivered it — no protocol can
  deliver a destroyed record. Retention on causal topics must comfortably exceed the longest
  consumer outage, lag, or replay you intend to survive. Parsley fails closed on the detectable
  half: causal sources use `AutoOffsetReset.none()`, so a consumer whose position falls out of
  range crashes rather than silently jumping past destroyed causes, and log-start offsets are
  seeded only on a genuine first start (offset expiry is not a first start). Mid-replay expiry is
  therefore a loud crash-loop until an operator resets — a liveness stall by design, never a
  reorder. This is environmental assumption E2 of the
  [causal consistency model](internals/causal-consistency.md#environmental-assumptions); see
  [Troubleshooting](troubleshooting.md#retention-outran-a-causal-consumer) for recovery.
