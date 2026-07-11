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

There is no vacuous satisfaction, though: a dependency naming a coordinate this node has **no input
channel for at all** — an undeclared topic, or a partition a different task instance owns — is not
treated as satisfied just because it is out of scope. This node can prove it has no way to confirm such
a coordinate, never that the coordinate is genuinely irrelevant, so it fails the task fast instead of
buffering forever or guessing. The corollary is the **topology contract**: every coordinate a node's
records could ever depend on must be reachable to that node through at least one input channel,
directly or transitively. A join of fully independent sources can still receive a record depending on a
coordinate no input channel observes — route that coordinate through some input branch instead (see
[`parsley.coordination.domain-topics`](configuration.md) for doing this without a redundant business
subscription). Unlike the topology contract, there is no restriction on a node consuming both a topic
and a topic derived from it — single-witness merge has no unanimity requirement for that to violate.
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

## Topology epochs

A causal topology sometimes has to change while it runs — a new stage, a replaced stage, a recompile.
A new stage replays its inputs from the earliest offset, and the completeness frontier is a minimum
across every node, so a node replaying from offset 0 would pull the shared frontier back to the start
and un-strip history the other nodes had already delivered. An **epoch** prevents this. It defines a
floor per coordinate; history below the floor is pre-epoch, so it feeds the delegate's state but does
not participate in causal time. A node deployed into a running topology adopts the current floor and
replays with everything below it stripped, so it never drags the frontier down.

Setting `parsley.coordination.epoch-events-topic` turns this on. It is optional and leaderless:
participating applications share one single-partition epoch-events log and each folds it identically
to agree on every epoch's floor, which then propagates through the topology in-band. Which topics are
external entry points is derived from what each node declares it consumes and produces, not configured
by hand. Absent that key, a topology runs in epoch 0 and behaves exactly as one with no epoch
machinery.

Coordination also requires every running member's declared inputs and sinks to jointly cover the full
coordinated domain, or an epoch round cannot commit — a genuine multi-stage pipeline (app A produces a
topic only app B consumes) needs every member to also cover the topics only a sibling touches. Set
`parsley.coordination.domain-topics` so `CausalTopology` auto-wires a passthrough source for any domain
topic a stage does not otherwise consume or produce, covering it without a redundant business
subscription — this is what makes a genuinely cyclic topology (A produces to B, B produces back to A)
coordinate correctly. See [Evolving a running topology](streams.md#evolving-a-running-topology) for the
API and [Topology epochs](internals/topology-epochs.md) for the protocol.
