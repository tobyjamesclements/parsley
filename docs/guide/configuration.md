# Configuration

## Causal delivery has exactly two dispositions

Parsley's own configuration surface is empty; this page documents the behaviour to observe rather
than knobs to set. A record is either **forwarded** once its dependencies are satisfied, or stays
**buffered** — unbounded, changelog-backed — while they are not. There is no configuration that
trades causal safety for liveness, no eviction, and no third disposition: a record that cannot be
evaluated at all (an undecodable payload or an undecodable causal-clock header) unconditionally fails
the task, and a dependency on a coordinate this node does not consume is ignored, soundly, with a
metric. This is the [delivery gate](../foundations/delivery-gate.md) restated operationally. See
[Troubleshooting](troubleshooting.md) for what it looks like in practice and how to recover from it.

---

## Header size

The serialised `parsley-causal-clock` header is `5 + 28 × entries` bytes. It counts against
Kafka's record-size limit (`message.max.bytes` and `max.request.size`, around 1 MB by default), and
there is no separate header budget.

- **Automatic Streams stamping** stamps the node's completeness frontier, which is bounded by the
  number of source topics in the subtopology, at one partition per topic per task. This stays small
  under normal topologies.
- **`CausalClock.fromRecord(trigger)`** carries only the partitions the upstream producer
  depended on. This is the recommended way to propagate causal context from a plain Kafka client.
- **A manually built `CausalClock`** is as wide as the coordinates you `require(...)`. Watch
  this on records that legitimately depend on many topic-partitions.

Parsley never truncates the dependencies header, because truncation would silently break the
guarantee. Keep the relevant-partition count within your record-size budget.

---

## Parsley has no configuration keys

Parsley's configuration surface is empty: there is no `parsley.*` key, and the `Properties` passed
to `CausalStreams` carry only standard Kafka Streams configuration. Every behaviour that once had a
key is unconditional, because each one guards causal safety and no viable deployment opts out of
it. Startup fails with `IllegalStateException`, naming every offending key, if any `parsley.*` key
is present in the `Properties` or in a `parsley.properties` classpath resource — a key that wires
nothing must not parse quietly.

The always-on startup checks, run once per task at init:

- The causal input topics must share a partition count. Unequal counts make co-partitioning
  impossible, so a task would evaluate the completeness frontier against an incomplete partition
  set.
- A `CausalTopology`-assembled stage's sink topics must each have at least as many partitions as
  the widest source. Protocol markers route to the forwarding task's own partition, so a narrower
  sink would fail the marker produce at runtime; a wider sink (a funnel fanning narrow sources into
  a re-keyed sink) passes.
- No causal source or sink topic may have a `cleanup.policy` including `compact`. A compacted
  source punches consumer-visible holes the skip-bridge would misread as transaction markers; a
  compacted sink can silently lose protocol null messages, which are wire-indistinguishable from
  compaction tombstones.

Each sink-side check is best-effort per topic: a transient describe failure on one sink skips that
check for that sink only, and never masks a genuine misconfiguration on a different sink in the
same stage. Sink existence itself is strict — a declared sink that cannot be resolved fails
startup, because own-output stamping depends on its resolved identity. See
[Streams integration](streams.md#startup-validation) and its
[preconditions](streams.md#preconditions) for the full contract.

## Metrics

Parsley wires a handful of Kafka Streams `Sensor`s per task, under the `stream-parsley-metrics`
group. Every metric is tagged `parsley-id` (the task ID, for example `0_1`) and `thread-id` (the
stream thread that registered it). They are visible over JMX like any Kafka Streams metric, and
in-process through `CausalStreams.metrics()`.

| Sensor | Kind | Meaning |
|---|---|---|
| `records-buffered` | rate/total | Records admitted into the causal buffer (held, not yet delivered). |
| `records-released` | rate/total | Records released from the buffer once their dependencies were satisfied. |
| `deserialization-errors` | rate/total | Records that failed to deserialise on the forward path. |
| `clock-resolution-errors` | rate/total | Records whose `parsley-causal-clock` header could not be decoded. |
| `deps-out-of-scope-ignored` | rate/total | Dependency coordinates on channels this node does not consume, ignored by the gate (one count per coordinate). Routine in topologies whose consumers have narrower scopes than their ancestors' stamps; a sustained unexpected rate can indicate a cross-wired deployment or a co-partitioning mistake. |
| `replays-skipped` | rate/total | Received records whose offset was already delivered here, skipped instead of being forwarded to the delegate again. Routine while an added input's re-fetched prefix replays past the carried-ancestry seed after a scope change; sustained counts outside that warrant investigation. |
| `reflected-claims-above-own-outputs` | rate/total | Inbound clocks claiming one of this node's own sink coordinates above its own-outputs clock. Diagnostic only, never a failure: it means the own-output view is stale beyond the init-time heal, or a peer's stamp is not truthful. |
| `records-held-above-highest-received` | gauge | Held records waiting past the stall threshold (the effective producer `delivery.timeout.ms`, 120 s unless overridden) on a dependency above its channel's highest received offset — nothing received so far can satisfy the claim, so the delay is unbounded until that channel produces again. Fail-safe, never unsafe; also logged at `WARN` when the count changes. |
| `buffer-depth` | gauge | Current number of records held in the causal buffer. |
| `buffer-oldest-buffered-at-ms` | gauge | Timestamp the oldest currently-held record was buffered at. |

Any non-zero rate on the two error sensors, or sustained growth in `buffer-depth`, is worth alerting
on — see [Troubleshooting](troubleshooting.md).

## Performance and tuning

Parsley has no performance-tuning keys of its own; its overhead is characterised in the
[protocols cost model](../protocols/index.md#cost-model), which names the layer each cost comes from
and how it scales. To size a deployment, measure your own topology end to end and watch the metrics
above rather than relying on isolated micro-benchmarks.

The one standard Kafka setting that materially affects a causal stage is `producer.linger.ms`. A
delegate that forwards several records per input pays a
[crossing-wait serialization](../protocols/causal-broadcast.md#cost) proportional to the linger on
each forward, so for multi-forward delegates lower `producer.linger.ms` (to a few milliseconds, or
0) so each batch ships immediately. A single-forward delegate is usually unaffected.
