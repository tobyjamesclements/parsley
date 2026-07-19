# Causal consistency model

Parsley's concrete guarantee is causal delivery order for a Kafka Streams processor: records are
delivered to `process()` only after their declared dependencies have been satisfied, subject to the
conditions in [Streams integration](../streams.md). That guarantee is narrower than the
system-level causal consistency property described below. This page covers the theoretical model
the guarantee is built on, explains why traditional causal consistency algorithms do not apply
directly to Kafka stream processing, and describes Parsley's algorithm.

## Lamport's happened-before relation

Leslie Lamport's 1978 paper "Time, Clocks, and the Ordering of Events in a Distributed System"
defines a partial order on events called the happened-before relation, written A → B. It holds
when:

- A and B occur in the same process, and A occurs before B, or
- A is the sending of a message and B is its receipt, or
- there exists a C such that A → C and C → B (transitivity).

Causal consistency, applied to a storage or streaming system, says: if a write W1 happened before
a write W2, then any process that observes W2 must have previously observed W1. The transitive
case is the important one in practice — a consumer need not have subscribed to every intermediate
step to be bound by the ordering that runs through them.

Vector clocks implement the happened-before relation for distributed systems. Each process
maintains a clock: a map from process identifiers to the highest event number it has delivered
from that process. When a process delivers an event, it advances its own entry. When it sends a
message, it attaches a snapshot of its clock. A receiving process holds the message until its
local clock dominates the attached snapshot — until it has delivered everything the sender had
observed.

## How traditional algorithms work

Systems like COPS (Consistency, Availability, and Partition-tolerance of Scalable reads) and
causal broadcast protocols build on vector clocks under one shared assumption: every node that
must enforce causal ordering receives every write.

In COPS, writes propagate to all datacenters. When a datacenter receives a write carrying
dependencies, it can check each one because it already holds, or will receive, the writes those
dependencies name. The delivery predicate is always checkable: the coordinates in the dependency
clock are coordinates this datacenter will eventually observe.

In causal broadcast protocols the same assumption holds in a different form. Every process in the
group receives every message. The delivery predicate (local clock dominates the message's attached
clock) always converges because every coordinate in the attached clock is a message that will
eventually arrive.

This is the total visibility assumption: a node enforcing causal order has, or will have,
visibility into every event the dependency clock references.

## Why Kafka stream processing is different

A Kafka stream processor subscribes to a fixed set of input topics and receives messages on those
topics only. A producer stamps the causal dependencies header with its own frontier — the highest
offset it had observed on each topic-partition it consumed — and that frontier routinely spans
topics a downstream consumer never reads. A processor subscribing to T1 and T3 will receive records
whose dependency clocks include a coordinate for T2, an intermediate topic it has no connection to.

The traditional algorithms above assume total visibility: a node enforcing causal order receives, or
will receive, every event its dependency clocks reference. Kafka's subscription model does not
provide this for free — the processor will never receive T2's messages, so it cannot satisfy a
dependency on T2 through its own delivery.

Parsley's response is to **require every coordinate a message could ever depend on to be reachable
to this node** — every dependency a record can carry must name a topic-partition this node itself
consumes — rather than weaken the guarantee. Parsley does not scope a coordinate out when this node
happens not to consume it — that would be unsound, since a coordinate no channel here can ever
observe is never satisfiable. And it does not accept a *claim* in place of a delivery: a peer's
advertised clock saying "K reached k over there" is never a substitute for this node having delivered
K up to k itself — see [why local delivery is required](#why-local-delivery-is-required).

## Parsley's algorithm

> A record is delivered once this node's own contiguous delivered frontier dominates every
> coordinate the record depends on. A dependency is satisfied only by this node having itself
> delivered the cause; a claim advertised on another channel never substitutes for local delivery.

### The delivery gate

A record is delivered when this node's own contiguous frontier dominates its dependency clock, with
only the record's own self-cycle removed (a record never waits on its own offset). That is the
entire gate, `ParsleyEngine.isDeliverable()`:

```java
frontier().dominates(deps.withoutSelfReference())
```

There is no vacuously-satisfied dependency. A dependency on coordinate K is satisfied once this
node's own K channel has contiguously delivered up to the required offset; until then the record is
buffered.

### The completeness clock (the outbound stamp)

The **completeness clock** is this node's own delivered frontier, max-merged with every input
channel's advertised dependencies (`ParsleyVectorClock.merge`), computed in `ParsleyChannels.completeness()`.
Each channel contributes the dependencies its records and watermarks have advertised (the pairwise-max
over what it has seen on that channel). Forwarded records and protocol watermarks are stamped with
this clock (`ParsleyProcessor` reads `engine.completeness()`): it carries *transitive ancestry* — a
coordinate an upstream node delivered that this node's stamp must keep advertising — downstream,
where each receiving node's own gate verifies every coordinate it names against its own delivery
history. The stamp is how facts travel; the gate is where they are proven, locally, before anything
is released on them.

A dependency naming a coordinate this node has **no channel for at all** — not merely one that hasn't
advertised yet, but one structurally outside this node's own registered inputs — is a different case
from an ordinary unsatisfied dependency, and is not treated as either: it fails the task fast rather
than being buffered forever or silently admitted. This node can prove it has no way to confirm such a
coordinate, never that the coordinate is genuinely irrelevant — so guessing either way (waiting forever
with no way to ever succeed, or delivering on an unproven premise) is unsound. See
[Independent inputs](#the-topology-contract) for why this arises and how to fix the topology instead
of relying on this fail-closed behavior as a substitute.

### Why local delivery is required

This is the Birman-Schiper-Stephenson CBCAST delivery condition, instantiated on Kafka's own
`(topicId, partition)` coordinates: in BSS, a process delivers a message only once *its own*
delivered vector covers the message's timestamp — the condition is over the receiver's own delivery
history, never over what a peer reports having delivered. The distinction matters because the
guarantee Parsley makes is about the *order this node's processor observes events in*, not merely
about whether an event exists somewhere. "K reached offset k" being true at some peer does not mean
this node's delegate has processed K@k yet: if a claim carried on a sibling channel could satisfy the
gate, a record caused by K@k could reach the delegate before K@k itself does on this node's own K
subscription — an effect delivered before its cause, to a processor subscribed to both. So the gate
checks the local frontier exclusively, and an advertised claim is only ever *carried* (in the
outbound stamp) for downstream nodes to verify against their own frontiers in turn.

What the model still requires is that every coordinate a node's messages could ever depend on be
consumed by that node — see the [topology contract](#the-topology-contract). That is both a liveness
requirement (this node must eventually deliver the fact locally) and the reason the fail-closed
unreachable-dependency check exists (a coordinate this node never consumes could never be proven
here at all).

### The topology contract

Every coordinate any of a node's dependency clocks could ever name must be reachable to that node
through **at least one** input channel — directly, or transitively through a channel that has itself
genuinely observed it. This is the metadata overhead of causal consistency without built-in total
visibility, placed on topology construction rather than hidden in the engine.

- **Independent inputs.** A node joining unrelated sources can receive a record depending on a
  coordinate no input channel of this node ever observes — nothing here can ever confirm it, so the
  completeness clock never includes it. Rather than buffer such a record forever (an undiagnosable,
  permanent stall) or admit it on the unproven assumption that the coordinate is irrelevant, this fails
  the task fast immediately. To make such a record genuinely deliverable, route the coordinate through
  some input branch instead — have it consume and pass the coordinate through, emitting watermarks even
  when it runs no business logic.
- **Consuming both an ancestor and its own descendant is fine.** A node may consume both a topic `T`
  and a topic derived from `T`. A descendant record's dependency on `T` is satisfied by the node's
  own `T` channel delivering up to it — the ordinary gate, no special case; the descendant's stamp
  carrying `T`'s progress transitively matters only for the node's own outbound stamp, never for its
  gate. (An earlier version of this contract forbade this pattern; that restriction was a consequence
  of the retired intersection-based gate, not a fundamental limit — see the changelog.)

### Protocol watermarks (heartbeats)

The completeness stamp advances only as this node's own frontier or its channels advertise progress,
so a node must keep advertising even when it produces no business output. A consumed message that
yields no business record — a filter that drops it, a record held in the buffer, a not-yet-ready
aggregate — emits a *protocol watermark*: a record with a null value, marked with the
`_parsley_watermark` header, carrying the node's current completeness frontier (emitted when a held
record advances completeness,
or when a delivered record produces no forward). The watermark is keyed with the triggering record's
key so it routes to the same partition that record's business output would, which keeps completeness
propagation correct across a sink boundary; it is identified by the header, never by its key. A
downstream node delivers the watermark's *own offset* on its source channel — advancing that
channel's contiguous frontier exactly like a business record's own coordinate, which is what can
release held records — and folds the carried frontier into that channel's advertised clock, feeding
its own outbound stamp (never its gate). It then re-emits its own watermark only when the carried
clock taught it something new, so progress propagates contiguously through layers that produce no
business records without ping-ponging around a cycle. Parsley's own processors
consume watermarks internally (`ParsleyProcessor` / `ParsleyEngine.onWatermark`); a plain Kafka
client folds them into its running frontier with `CausalDependencies.observe` while skipping them as
business records, which it detects with `CausalDependencies.isWatermark`.

### Each channel's own coordinate is a contiguous boundary

The frontier's value for each coordinate is a *contiguous* delivered boundary, not a running
maximum: the highest offset delivered on that channel without a gap. A later record on a partition
can be forwarded before an earlier record still held on the same partition; the boundary only
advances past the held record's position once that record is itself delivered. This matters because
the frontier is exactly what the `dominates` gate checks: if a coordinate jumped over a gap, a
record waiting on an offset inside the gap would be released on bookkeeping alone, without the
record at that offset having been delivered. The contiguous boundary ensures the gate reflects
actual delivery history, not observed-but-undelivered offsets. (It is distinct from the *protocol
watermark* records above, which carry a completeness frontier between nodes.)

## Violations

There is no path that breaks Lamport's happened-before guarantee. Delivery is unconditionally
fail-closed: there is no eviction and no configuration that trades causal order for availability. A
record whose dependencies are proven impossible to satisfy — an undecodable payload or dependencies
header, or a dependency naming a coordinate this node has no channel for (see
[the topology contract](#the-topology-contract)) — fails the task fast rather than being delivered out
of order or dropped. The engine throws, the task restarts, and the record stays in the buffer changelog
for recovery. No violation ever reaches downstream.

This takes the consistency side of the trade-off described in the CAP theorem and in the causal
consistency literature unconditionally: Parsley has no policy knob that spends causal order for
liveness. A genuinely stuck dependency (a lagging partition, a co-partitioning gap, a producing topic
that was deleted) shows up as unbounded buffer growth or a fail-closed task restart, never as a silent
reordering — see [Troubleshooting](../troubleshooting.md).
