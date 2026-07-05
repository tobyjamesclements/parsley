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

## Buffer limits

A `CausalBufferLimit` bounds how long or how large the buffer may grow before eviction fires.

| Limit | Factory | Description |
|---|---|---|
| Size | `CausalBufferLimit.ofSize(n)` | Fires when the buffer holds at least `n` records. |
| Duration | `CausalBufferLimit.ofDuration(d)` | Fires after the buffer has held a record for `d`. This is time-based and requires the processor to call eviction on schedule. |
| First-of | `CausalBufferLimit.first(a, b, ...)` | Fires when the first of several limits fires. |

## Delivery and eviction

A record is delivered in causal order once its dependencies are confirmed by every input channel of
the processor. This covers a record delivered immediately, a record delivered after a wait, and a
record that claims no dependencies or carries an undecodable header (an empty dependency set, trivially
satisfied).

The gate is strict: a dependency on coordinate K is satisfied only when *every* input channel the
processor consumes has advertised K to at least the required offset. A producer stamps its full causal
frontier, which can span topics a given processor does not read directly — and the processor waits for
all of its input channels to confirm those coordinates rather than ignoring them. This is what lets the
processor enforce an ordering that runs through topics it does not itself consume, but it means the
**topology must route every depended-upon coordinate through every input branch** of a node (consuming
and watermarking it, even with no business logic). A coordinate that some input branch never observes
holds the record indefinitely. Two corollaries: a join of fully independent sources will hold a record
that depends on a coordinate an unrelated input never sees, and a node must not consume both a topic and
a topic derived from it (the ancestor channel can never confirm the descendant). See the
[causal consistency model](internals/causal-consistency.md) for the full contract.

When a held record's dependencies are still not satisfied and the configured `CausalBufferLimit`
fires, what happens next is governed by `parsley.buffer.eviction.failure.policy`. The default is
`fail`. Under `fail`, Parsley fails the task rather than deliver the record out of causal order. The
record stays in the buffer and is retried after a restart or once the backlog eases. A record whose
dependency never arrives is therefore never force-delivered under the default policy.

Setting the policy to `continue` makes Parsley forward records rather than fail. Under
`continue`, the limit evicts the oldest qualifying record and delivers it out of causal order.
Eviction still feeds the frontier exactly like a normal delivery, so once the evicted record's
coordinate closes its gap, every record already forwarded above it, and any record buffered
downstream that was waiting on it, catches up in the same step. An eviction under `continue` never
permanently stalls the records behind it. The [Configuration](configuration.md) page describes the
policy in full.

## Causal violations

Every eviction under the `continue` policy, and every fail-fast firing under the default policy, is
logged with the current frontier, the required dependencies, and the causal gap. The causal gap is a
per-coordinate shortfall that shows exactly how far the frontier was behind at the time the limit
fired. Each firing is also counted by a metric. Register a `CausalAudit` to receive these as
per-record callbacks (`recordViolation` for evictions under `continue`, `recordEvictionLimitExceeded`
for fail-fast firings under `fail`) in addition to, or instead of, relying on logs and metrics. See
[Audit logging](audit-logging.md).

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
misconfigured topology evaluates against an incomplete partition set. A bare `CausalProcessors`
decorator can check one necessary part at startup, that its causal input topics share a partition
count, controlled by `parsley.topology.validation` (`warn` by default, `strict` to fail fast, `off`
to disable). `CausalStreams` (see [Streams integration](streams.md#the-high-level-api-causalstreams))
owns the sinks too, so it widens the same check to the sink topics, and applies one partitioner
uniformly across every sink it declares so a shard cannot drift onto different partitions across
topics by accident. See the [Streams integration](streams.md#startup-validation) preconditions for
the full contract.

## Topology epochs

A causal topology sometimes has to change while it runs — a new stage, a replaced stage, a recompile.
A new stage replays its inputs from the earliest offset, and the completeness frontier is a minimum
across every node, so a node replaying from offset 0 would pull the shared frontier back to the start
and un-strip history the other nodes had already delivered. An **epoch** prevents this. It defines a
floor per coordinate; history below the floor is pre-epoch, so it feeds the delegate's state but does
not participate in causal time. A node deployed into a running topology adopts the current floor and
replays with everything below it stripped, so it never drags the frontier down.

`CausalCoordination` turns this on. It is optional and leaderless: participating applications share one
single-partition epoch-events log and each folds it identically to agree on every epoch's floor, which
then propagates through the topology in-band. Which topics are external entry points is derived from
what each node declares it consumes and produces, not configured by hand. Without a `CausalCoordination`
a topology runs in epoch 0 and behaves exactly as one with no epoch machinery. See
[Evolving a running topology](streams.md#evolving-a-running-topology) for the API and
[Topology epochs](internals/topology-epochs.md) for the protocol.
