# Configuration

## Causal delivery has exactly two dispositions

A record is either **forwarded** once its dependencies are satisfied, or stays **buffered** —
unbounded, changelog-backed — while they are not. There is no configuration that trades causal safety
for liveness, no eviction, and no third disposition: a record whose dependencies are proven impossible
(an undecodable payload or dependencies header, or a dependency naming a coordinate this node has no
input channel for) unconditionally fails the task. See [Troubleshooting](troubleshooting.md) for what
that looks like operationally and how to recover from it.

---

## Header size

The serialised `parsley-causal-dependencies` header is `5 + 28 × entries` bytes. It counts against
Kafka's record-size limit (`message.max.bytes` and `max.request.size`, around 1 MB by default), and
there is no separate header budget.

- **Automatic Streams stamping** stamps the node's completeness frontier, which is bounded by the
  number of source topics in the subtopology, at one partition per topic per task. This stays small
  under normal topologies.
- **`CausalDependencies.fromRecord(trigger)`** carries only the partitions the upstream producer
  depended on. This is the recommended way to propagate causal context from a plain Kafka client.
- **A manually built `CausalDependencies`** is as wide as the coordinates you `require(...)`. Watch
  this on records that legitimately depend on many topic-partitions.

Parsley never truncates the dependencies header, because truncation would silently break the
guarantee. Keep the relevant-partition count within your record-size budget.

---

## `parsley.properties`

Parsley reads its own behaviour from a `parsley.properties` resource on the classpath. This is kept
separate from Kafka Streams configuration because these behaviours have no Streams equivalent. An
absent file, or an absent key, falls back to the defaults below.

```properties
# How a causal processor reacts at startup to a detectable topology misconfiguration: the causal
# input topics not sharing a partition count (co-partitioning impossible), and, for a
# CausalStreamsBuilder stage, its sink topics' partition counts and cleanup.policy too.
#   warn     (default) logs the mismatch and continues
#   strict             fails the task fast at startup
#   off                disables the checks
parsley.topology.validation = warn

# The shared, single-partition epoch-events log topic name. Set this to turn on topology-epoch
# coordination for a CausalStreams runtime. Absent (the default), a topology runs in epoch 0 exactly
# as without coordination: no epoch-events log, no coordination thread.
#
# The topic must exist with EXACTLY ONE partition and must retain its full history: every instance
# replays the log from the beginning on startup, so use cleanup.policy=delete with retention.ms=-1 —
# finite retention or compaction silently erases membership and epoch history. Both are validated at
# startup (the partition count, the cleanup policy, and the retention), failing fast on a topic that
# could ever lose events the fold needs.
# parsley.coordination.epoch-events-topic = parsley-epoch-events

# The comma-separated set of every topic in the coordinated domain (every member's inputs and sinks,
# external sources included). Only meaningful alongside parsley.coordination.epoch-events-topic; set
# it so CausalTopology auto-wires a passthrough source for any domain topic a stage does not otherwise
# consume or produce, letting that stage's declared subscriptions cover the full domain without a
# redundant business subscription. Required for a topology with a genuine cycle. See
# Evolving a running topology in streams.md.
# parsley.coordination.domain-topics = prices,orders,enriched-output
```

| Key | Default | Values |
|---|---|---|
| `parsley.topology.validation` | `warn` | `off`, `warn`, `strict` |
| `parsley.coordination.epoch-events-topic` | (unset) | a topic name |
| `parsley.coordination.domain-topics` | (unset) | comma-separated topic names |

`parsley.topology.validation = warn` logs a prominent warning at startup when the causal input topics
do not share a partition count, which makes co-partitioning impossible, and lets the task start. This
is visible without breaking a deployment that already ran with the mismatch. Set it to `strict` to
fail the task fast instead, or `off` to skip the checks. `CausalTopology`-assembled stages also fold
their sink topics' partition counts into the same parity check and check each sink's `cleanup.policy`
for `compact` (a protocol watermark is a null-value record wire-indistinguishable from a compaction
tombstone). Each sink is resolved independently, so one sink that does not exist yet never masks a
genuine misconfiguration on a different sink in the same stage, even under `strict`; both sink-side
checks are skipped entirely (no admin round-trip) when validation is `off`. Under topology-epoch
coordination, a sink partition-count mismatch always escalates to a hard failure regardless of this
setting — a crash loop is the actual failure mode a warning would otherwise hide. See
[Streams integration](streams.md#startup-validation) and its
[preconditions](streams.md#preconditions) for the full contract.

`parsley.coordination.epoch-events-topic` and `parsley.coordination.domain-topics` are read directly
from the `Properties` passed to `new CausalStreams(topology, props)` — see
[Evolving a running topology](streams.md#evolving-a-running-topology).

## Metrics

Parsley wires a handful of Kafka Streams `Sensor`s per task, under the `stream-parsley-metrics` group:

| Sensor | Kind | Meaning |
|---|---|---|
| `records-buffered` | rate/total | Records admitted into the causal buffer (held, not yet delivered). |
| `records-released` | rate/total | Records released from the buffer once their dependencies were satisfied. |
| `deserialization-errors` | rate/total | Records that failed to deserialise on the forward path. |
| `clock-resolution-errors` | rate/total | Records whose `parsley-causal-dependencies` header could not be decoded. |
| `deps-out-of-scope-ignored` | rate/total | Dependency coordinates on channels this node does not consume, ignored by the gate (one count per coordinate). Routine in topologies whose consumers have narrower scopes than their ancestors' stamps; a sustained unexpected rate can indicate a cross-wired deployment or a co-partitioning mistake. |
| `buffer-depth` | gauge | Current number of records held in the causal buffer. |
| `buffer-oldest-buffered-at-ms` | gauge | Timestamp the oldest currently-held record was buffered at. |

Any non-zero rate on the two error sensors, or sustained growth in `buffer-depth`, is worth alerting
on — see [Troubleshooting](troubleshooting.md).
