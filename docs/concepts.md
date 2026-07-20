# Concepts

## Causal dependencies

`CausalDependencies` is a snapshot of what a producer had observed when it sent a record. It is a map
from a `(topicId, partition)` coordinate to the highest offset the producer had seen on that
coordinate. Parsley serialises this map as a compact binary header named
`parsley-causal-dependencies` and attaches it to every outgoing record.

On the consumer side the header is decoded back into a `CausalDependencies` value. It describes the
minimum frontier a downstream consumer must reach before the record may be delivered. A record is
causally ready once the consumer's frontier has observed at least the offset the dependencies require
on every coordinate.

The keys are Kafka topic UUIDs rather than topic names. Because a topic that is deleted and recreated
receives a new UUID, a new incarnation of `prices` is treated as a distinct dependency. Records
stamped against the old `prices` are never satisfied by the new one.

Names are resolved to UUIDs once, at task initialisation. Deleting and recreating a causal topic
while an application is running is therefore not a supported operation: a `CausalStreams` instance
polls the broker's current topic IDs in the background and fails the application fast when a causal
topic's UUID changes mid-run, rather than letting records of the new incarnation be processed under
the old identity. Records fetched between the recreation and the next poll are the residual exposure,
so treat live recreation as an operational error; restarting after a recreation is safe (identity is
re-resolved, and the old incarnation's history reads as lost, never reordered).

## The frontier

The causal frontier is the node's internal clock. For each `(topicId, partition)` coordinate it
records the highest offset the node has delivered contiguously. It does not record an offset the node
merely happens to have seen something later than, because Parsley does not head-of-line block. A
later-offset record on a partition may be forwarded before an earlier record that is still held on
the same partition. Forwarding that later record advances the frontier only as far as the contiguous
run actually reaches, so the held record's gap is never skipped over. Once the held record is itself
delivered, whether it is released or evicted, the frontier catches up in a single step through
everything already forwarded above it.

The node's internal frontier is an implementation detail, and there is no public type for it: it is
contiguous (the highest offset delivered without a gap), which is specific to how the engine releases
held records. A node consuming with a plain Kafka client maintains its own frontier instead, as an
accumulating `CausalDependencies` value. Bind a resolver once with `CausalDependencies.using(props)`,
fold in each record you consume with `observe(record)`, and stamp the result onto each record you
produce. A one-to-one relay is `using(props).observe(record)`; a fan-in chains an `observe` per
input; a stateful node keeps one instance and observes into it across records. To read back the
dependencies a record already carries, without folding in a new position, use
`CausalDependencies.fromRecord(record)`.

When you consume a topic a Parsley topology produces, some records are *protocol watermarks*: null
key and value, carrying a completeness frontier so causal progress flows through processors that
produce no output for a given input. Still `observe(record)` them, so your frontier advances across a
service that emitted only watermarks on this path, but skip them as business records — test with
`CausalDependencies.isWatermark(record)` and `continue` past those it flags.

The frontier is persisted before each record is forwarded, so it survives restarts and rebalances.

## The causal buffer

A record whose dependencies are not yet satisfied is held in the causal buffer. Held records survive
a restart, and no re-delivery from the broker is required.

When a coordinate in the frontier advances because a causally ready record was forwarded, Parsley
checks the buffer for records that were waiting on that coordinate. Any record that is now satisfied
is released, and its own source coordinate advances the frontier in turn. This cascades until no
further releases occur.

## Delivery

A record is delivered once this node's **own contiguous frontier** — the positions it has itself
delivered, gap-free — dominates the record's dependencies. A dependency is satisfied only by this
node having delivered the cause locally; a claim carried on another channel's clock (a peer's
watermark advertising a coordinate the peer delivered) never substitutes for local delivery, or the
processor could see an effect before the cause it directly subscribes to. This covers a record
delivered immediately and a record delivered after a wait; a record that claims no dependencies (an
empty dependency set) is trivially satisfied.

The **completeness** clock — the frontier max-merged with every input channel's advertised clock —
is the *outbound stamp*, not the delivery gate: it carries transitive ancestry (coordinates an
upstream channel has advertised) downstream, where each receiver's own gate verifies them against
its own delivery history.

A dependency naming a coordinate this node does not consume at all — an undeclared topic, or a
partition a different task instance owns — is **ignored**, unconditionally. That is sound, not
vacuous satisfaction: stamps are transitively complete and merged unconditionally, so any consumed
causal ancestor of a record is claimed *directly* in that record's own clock — an unconsumed entry
only ever proxies ancestry the same clock already states, and ignoring it loses no ordering
observable at this node. Each ignore is counted by the `parsley.deps.out-of-scope-ignored` metric,
never a failure. There is no restriction on a node consuming both a topic
and a topic derived from it.
See the [causal consistency model](internals/causal-consistency.md) for the full contract and why a
single witness suffices.

## Causal buffer is unbounded and fail-closed

The causal buffer never evicts and never delivers a record out of causal order. There is no
configuration that trades causal safety for liveness. A record whose dependencies are proven
impossible — an undecodable payload or dependencies header, or a dependency naming a coordinate this
node has no channel for — unconditionally fails the task; it is never dropped or forwarded on an
unproven premise. The failure is logged with the record's coordinate and, for a decode failure, its
metadata (never the payload), and counted by a metric. See [Troubleshooting](troubleshooting.md) for
the operational playbook and [Configuration](configuration.md#metrics) for the metrics.

## Co-partitioning

Parsley evaluates dependencies against the partitions assigned to a single consumer instance. For the
guarantee to hold, an instance must be assigned every partition in a causally related set. The
standard way to arrange this is to partition related topics by the record key with a matching
partition count, so that causally related messages, which share a key, land on the same partition
number across topics and each instance owns partition `N` everywhere. The key is the shard: the unit
that keeps causally related events together on one partition. An advanced user can partition by a
coarser function of the key with a custom `StreamPartitioner`, provided the partitioner reads the key
rather than the value, since a protocol watermark carries no value to read.

Parsley does not enforce co-partitioning end to end, and most of it cannot be checked, so a
misconfigured topology evaluates against an incomplete partition set. At startup, `parsley.topology.validation`
(`warn` by default, `strict` to fail fast, `off` to disable) checks that a stage's causal input topics
share a partition count, and, since a stage built through `CausalStreamsBuilder` owns its sinks too,
folds sink partition counts into the same check and applies one partitioner uniformly across every sink
a stage declares so a shard cannot drift onto different partitions across topics by accident. See the
[Streams integration](streams.md#preconditions) preconditions for the full contract.

## Joining a running topology

A causal topology sometimes has to change while it runs — a new stage, a replaced stage, a recompile.
Joining needs **zero coordination**: a fresh application starts consuming from wherever the log
starts, its hold-back queue converts arbitrary cross-partition arrival into causal delivery order
(replay is just arbitrarily delayed delivery, which causal broadcast absorbs by construction), and
its truthful stamps make its outputs correctly gated everywhere from its first emission. A fresh
record with old dependencies simply sits low in the causal partial order — correct, not a hazard.
There is no join barrier, no admission, no membership roster, and no epoch. (Earlier versions
coordinated joins through a topology-epoch subsystem; it contributed nothing to causal safety and
has been removed. Its `parsley.coordination.*` keys fail startup loudly if present — delete them.)
