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
to this node** — directly, or via a genuine relay — rather than weaken the guarantee. But it does not
require every one of a node's own input channels to independently corroborate the same coordinate: a
single channel that has genuinely, contiguously delivered up to a coordinate is enough. Requiring
unanimous confirmation across a node's own channels adds nothing to correctness — see
[why a single witness suffices](#why-a-single-witness-suffices) — and would forbid an ordinary
consumption pattern (a node consuming both an ancestor topic and one of its own descendants) for no
safety reason. Parsley does not scope a coordinate out when this node happens not to consume it —
that would be unsound, since a coordinate no channel here can ever observe is never satisfiable — but
it also does not require every channel to agree.

## Parsley's algorithm

> A record is delivered once this node's own completeness clock dominates every coordinate the
> record depends on. A single genuine witness — this node's own delivery, or a channel's honestly
> advertised dependency — is enough for any one coordinate; no cross-channel corroboration is
> required.

### The completeness clock

The **completeness clock** is this node's own delivered frontier, max-merged with every input
channel's advertised dependencies (`ParsleyClock.merge`), computed in `ParsleyFrontier.completeness()`.
Each channel contributes the dependencies its records and watermarks have advertised (the pairwise-max
over what it has seen on that channel). This node's own coordinates — the `ParsleyFrontier` offset for
each channel it directly, contiguously delivers — always win any merge against a channel's separately
advertised view of the same coordinate, since this node's own gate already required that value to be
proven before it could advance.

A coordinate that no input channel has ever observed is absent from the completeness clock: it is not
yet confirmed by anything reachable to this node, so a dependency on it is not yet satisfiable. Once
*any* channel has genuinely advertised it, it counts — there is no requirement that every channel
independently repeat the same confirmation.

### The delivery gate

A record is delivered when the completeness clock dominates its dependency clock, with only the
record's own self-cycle removed (a record never waits on its own offset). That is the entire gate,
`ParsleyEngine.isDeliverable()`:

```java
completeness().dominates(deps.withoutSelfReference())
```

There is no vacuously-satisfied dependency. A dependency on coordinate K is satisfied once *any* input
channel has advertised K to at least the required offset; until then the record is buffered. Forwarded
records and protocol watermarks are stamped with this same completeness clock (`ParsleyProcessor` reads
`engine.completeness()`).

A dependency naming a coordinate this node has **no channel for at all** — not merely one that hasn't
advertised yet, but one structurally outside this node's own registered inputs — is a different case
from an ordinary unsatisfied dependency, and is not treated as either: it fails the task fast rather
than being buffered forever or silently admitted. This node can prove it has no way to confirm such a
coordinate, never that the coordinate is genuinely irrelevant — so guessing either way (waiting forever
with no way to ever succeed, or delivering on an unproven premise) is unsound. See
[Independent inputs](#the-topology-contract) for why this arises and how to fix the topology instead
of relying on this fail-closed behavior as a substitute.

### Why a single witness suffices

This is structurally a per-process vector clock — the Birman-Schiper-Stephenson CBCAST delivery
condition, instantiated directly on Kafka's own `(topicId, partition)` coordinates. A dependency
names a fact about some coordinate's stream ("K has reached offset k"), and once *one* channel has
genuinely, contiguously delivered up to k on K — its own gate already required that before it could
advance — the fact is true, full stop. Waiting for a second, unrelated channel to separately confirm
the same already-true fact adds nothing: the second channel's own lag is a fact about *its* coordinate,
never about whether K really reached k. A gate clock only ever advances at the moment of a genuinely
gated delivery — never from a received-but-undelivered message or from ungated gossip — which is what
makes an honestly advertised value trustworthy no matter which channel relays it.

What the model still requires is that every coordinate a node's messages could ever depend on be
*reachable* to it, directly or through a channel that genuinely, transitively carries it — see the
[topology contract](#the-topology-contract). That is a liveness requirement (can this node ever learn
the fact at all), not a safety one (single-witness merge never lets a node deliver something before it
is genuinely true).

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
  and a topic derived from `T` — single-witness merge has no unanimity requirement to violate. The
  descendant's completeness stamp already carries `T`'s progress transitively; the `T` channel's own
  contribution simply merges in alongside it, redundantly but harmlessly. (An earlier version of this
  contract forbade this pattern; that restriction was a consequence of the retired intersection-based
  gate, not a fundamental limit — see the changelog.)

### Protocol watermarks (heartbeats)

The completeness clock advances only as this node's own frontier or its channels advertise progress,
so a node must keep advertising even when it produces no business output. A consumed message that
yields no business record — a filter that drops it, a record held in the buffer, a not-yet-ready
aggregate — emits a *protocol watermark*: a record with a null value, marked with the
`_parsley_watermark` header, carrying the node's current completeness frontier (emitted when a held
record advances completeness,
or when a delivered record produces no forward). The watermark is keyed with the triggering record's
key so it routes to the same partition that record's business output would, which keeps completeness
propagation correct across a sink boundary; it is identified by the header, never by its key. A
downstream node folds the carried frontier into
that source channel's clock and re-runs its drain, then re-emits its own watermark, so progress
propagates contiguously through layers that produce no business records. Parsley's own processors
consume watermarks internally (`ParsleyProcessor` / `ParsleyEngine.onWatermark`); a plain Kafka
client folds them into its running frontier with `CausalDependencies.observe` while skipping them as
business records, which it detects with `CausalDependencies.isWatermark`.

### Each channel's own coordinate is a contiguous boundary

A channel's contribution for its own coordinate is a *contiguous* delivered boundary, not a running
maximum: the highest offset delivered on that channel without a gap. A later record on a partition
can be forwarded before an earlier record still held on the same partition; the boundary only
advances past the held record's position once that record is itself delivered (by release or
eviction). This matters because completeness feeds the `dominates` check: if a channel's own
coordinate jumped over a gap, a record waiting on an offset inside the gap would be released on
bookkeeping alone, without the record at that offset having been delivered. The contiguous boundary
ensures completeness reflects actual delivery history, not observed-but-undelivered offsets. (It is
distinct from the *protocol watermark* records above, which carry a completeness frontier between
nodes.)

## Violations

Eviction is the only path that breaks Lamport's happened-before guarantee. Under the default
policy (`parsley.buffer.eviction.failure.policy = fail`), the engine throws rather than deliver
a record whose dependencies are not met, and the task restarts. No violation reaches downstream.

Under the `continue` policy, the record is delivered out of causal order. Its outgoing stamp
carries the node's completeness frontier at eviction time, which does not dominate the evicted
record's declared dependencies. Downstream nodes receiving that stamp may release records they would
otherwise have held. The violation is transitive. The `continue` policy is an explicit trade of
Lamport's guarantee for availability under a full buffer; it is logged, metered, and documented
as a deliberate choice.

The distinction between `fail` and `continue` maps directly onto the consistency-availability
trade-off described in the CAP theorem and in the causal consistency literature. The default
takes the consistency side.
