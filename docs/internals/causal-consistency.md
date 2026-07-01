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

Parsley's response is to **require total visibility of the topology** rather than weaken the
guarantee. The delivery rule is strict: a record is delivered only once *every* input channel has
confirmed *every* coordinate the record depends on. For that to be answerable, the topology must
route every depended-upon coordinate through every input branch of the node (the
[topology contract](#the-topology-contract)). The alternative — scoping a coordinate out when the
node does not consume it, treating it as vacuously satisfied — is unsound: it lets a node deliver a
record before a sibling branch confirms a shared ancestor, and lets a lagging or recovering branch
later introduce an earlier-ordered record after the fact. Parsley does not scope; it waits for every
channel.

## Parsley's algorithm

> A record is delivered only once every input channel has confirmed every coordinate the record
> depends on.

### The completeness frontier

For each coordinate, the **completeness frontier** is the greatest offset that every input channel
has confirmed. It is the per-coordinate minimum across all input channels
(`ParsleyClock.intersectMin`), computed in `ParsleyEngine.completeness()`. Each channel contributes:

- the dependencies its records and watermarks have advertised (the pairwise-max over what it has
  seen on that channel), and
- its own contiguous delivered position — the `ParsleyFrontier` offset for the channel's own
  coordinate.

A coordinate that any input channel has **not** observed is absent from the completeness frontier
entirely: it is not confirmed by all branches, so a dependency on it is not yet satisfiable. Channels
are seeded at registration, so a silent channel holds the minimum down rather than being absent from
the fold.

### The delivery gate

A record is delivered when the completeness frontier dominates its dependency clock, with only the
record's own self-cycle removed (a record never waits on its own offset). That is the entire gate,
`ParsleyEngine.isDeliverable()`:

```java
completeness().dominates(deps.withoutSelfReference())
```

There is no in-scope filtering and no vacuously-satisfied dependency. A dependency on coordinate K is
satisfied only when every input channel has advertised K to at least the required offset; until then
the record is buffered. Forwarded records and protocol watermarks are stamped with this same
completeness frontier (`ParsleyProcessor` reads `engine.completeness()`).

### Why every channel must confirm

The protocol cannot prove that a causally-earlier message will not later arrive on some other input
channel until that channel has advertised past the dependency. A channel that is behind — or crashed
and recovering — is exactly the case where, on catching up, it emits records that belong earlier in
the order. Holding a record until every channel confirms its dependencies is therefore the guarantee
working, not a stall. This is strictly stronger than the happened-before minimum, and it is what lets
Parsley enforce a transitive ordering (`T1 → ... → T3`) at a node that does not consume the
intermediate topics — provided the contract below is met.

### The topology contract

Because every input channel must confirm a declared coordinate, the topology must be built so that
**every input branch of a node observes (consumes and watermarks) every coordinate any branch's
records depend on.** This is the metadata overhead of causal consistency without built-in total
visibility, placed on topology construction rather than hidden in the engine. Two consequences:

- **Independent inputs.** A node joining unrelated sources will hold a record depending on a
  coordinate that a sibling input never observes (its channel never confirms that coordinate, so the
  completeness minimum never includes it). To make such a record deliverable, route the coordinate
  through every input branch — have each branch consume and pass it through, emitting watermarks even
  when it runs no business logic.
- **No ancestor with its own descendant.** A node must not consume both a topic `T` and a topic
  derived from `T`. The `T` channel can never confirm the derived topic — `T` records are produced
  before the derived records exist — so a dependency on the derived topic is never satisfied. Consume
  only the derived topic; the ancestor's progress arrives transitively through the derived topic's
  completeness stamp.

### Protocol watermarks (heartbeats)

The completeness minimum advances only as channels advertise progress, so a node must keep
advertising even when it produces no business output. A consumed message that yields no business
record — a filter that drops it, a record held in the buffer, a not-yet-ready aggregate — emits a
*protocol watermark*: a record with a null value, marked with the `_parsley_watermark` header,
carrying the node's current completeness frontier (emitted when a held record advances completeness,
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
