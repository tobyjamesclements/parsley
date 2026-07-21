# Configuration

## Causal delivery has exactly two dispositions

A record is either **forwarded** once its dependencies are satisfied, or stays **buffered** —
unbounded, changelog-backed — while they are not. There is no configuration that trades causal safety
for liveness, no eviction, and no third disposition: a record that cannot be evaluated at all (an
undecodable payload or an undecodable causal-clock header) unconditionally fails the task, and a
dependency on a coordinate this node does not consume is ignored, soundly, with a metric. See
[Troubleshooting](troubleshooting.md) for what that looks like operationally and how to recover
from it.

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

## `parsley.properties`

Parsley reads its own behaviour from a `parsley.properties` resource on the classpath. A `parsley.*`
key may also be set in the `Properties` passed to `CausalStreams`, which overrides the classpath
resource key for key. The namespace is kept separate from Kafka Streams configuration because these
behaviours have no Streams equivalent. An absent file, or an absent key, falls back to the defaults
below; a key set to an unrecognised value fails startup with `IllegalStateException`.

```properties
# How a causal processor reacts at startup to a detectable topology misconfiguration: the causal
# input topics not sharing a partition count (co-partitioning impossible), and, for a
# CausalStreamsBuilder stage, its sink topics' partition counts and cleanup.policy too.
#   strict   (default) fails the task fast at startup
#   warn               logs the mismatch and continues (the explicit opt-down)
#   off                disables the checks
parsley.topology.validation = strict
```

| Key | Default | Values |
|---|---|---|
| `parsley.topology.validation` | `strict` | `off`, `warn`, `strict` |

No key under `parsley.coordination.*` is part of the configuration surface: joins need zero
coordination, so there is no coordination subsystem to configure. Startup fails with
`IllegalStateException`, naming the offending key, if one is present.

`parsley.topology.validation = strict` (the default) fails the task fast at startup when the causal
input topics do not share a partition count, which makes co-partitioning impossible. Set it to
`warn` to log a prominent warning and start anyway — the explicit opt-down for a deployment that
knowingly runs with the mismatch — or `off` to skip the checks. `CausalTopology`-assembled stages also fold
their sink topics' partition counts into the same parity check and check each sink's `cleanup.policy`
for `compact` (a protocol null message is a null-value record wire-indistinguishable from a compaction
tombstone). Each sink is checked independently, so a transient describe failure on one sink never
masks a genuine misconfiguration on a different sink in the same stage, even under `strict`; both
sink-side checks are skipped entirely (no admin round-trip) when validation is `off`. Sink existence
itself is not governed by this key: a declared sink that cannot be resolved fails startup
unconditionally, even under `off`, because own-output stamping depends on its resolved identity. Note that a sink with
fewer partitions than a source fails protocol-marker produces at runtime (markers route to the
task's own partition), so under `warn` a mismatched deployment crash-loops at the produce instead of
failing at startup — `strict` surfaces it once, clearly, at init. See
[Streams integration](streams.md#startup-validation) and its
[preconditions](streams.md#preconditions) for the full contract.

## Metrics

Parsley wires a handful of Kafka Streams `Sensor`s per task, under the `stream-parsley-metrics`
group. They are visible over JMX like any Kafka Streams metric, and in-process through
`CausalStreams.metrics()`.

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
