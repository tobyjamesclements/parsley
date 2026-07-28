# The causal model

## Happened-before

The happened-before relation is a partial order on events, written A → B. It holds when A and
B occur in the same process and A occurs first, when A is the sending of a message and B its
receipt, or transitively through any chain of the first two. Causal delivery, applied to
stream processing, says: if record A happened-before record B, then any processor that
consumes both delivers A before B. The transitive case is the one that matters in practice — a
consumer need not subscribe to every intermediate step to be bound by the ordering that runs
through them.

In the consistency hierarchy this sits above eventual consistency, which constrains no order,
and below linearisability, which forces a single global timeline. Causal consistency is the
strongest model maintainable without coordination, which is what lets Parsley deliver it with
no membership, no admission barrier, and no configuration handshake.

## Channels, coordinates, clocks

Kafka's unit of total order is the partition, not the process, so Parsley's clocks are indexed
by channel:

- A **channel** is `(topicId, partition)`, where `topicId` is the topic's stable Kafka UUID. A
  topic deleted and recreated under the same name is a different channel, so a coordinate can
  never silently rebind to a different record history.
- A **coordinate** is a channel plus a broker offset — the identity of one appended record.
- A **clock** maps channels to offset watermarks: an entry `(c, n)` claims every offset at or
  below `n` on `c`. Absent channels claim nothing.

A clock plays two roles, distinguished in the literature as VT(m) and VT(p): attached to a
record it states the record's dependencies; held by a node it accumulates what the node has
delivered and learned. The [gate](delivery-gate.md) compares the first against the node's
frontier; the [stamp](../design/architecture.md#the-stamp) publishes the second.

## What breaks the textbook algorithms here

Classical causal broadcast rests on two assumptions, and Kafka stream processing violates
both:

**Total visibility.** COPS-style systems and group-broadcast protocols assume every node that
enforces causal order receives every write, so the delivery predicate always converges. A
stream processor subscribes to a few topics out of many; most coordinates in a dependency
clock name channels it will never see. Parsley's answer is not to restore visibility but to
make the predicate sound without it — the gate's ignore branch, developed in
[the delivery gate](delivery-gate.md).

**Reliable dense FIFO channels.** A Kafka partition observed through a `read_committed`
consumer is not dense: transaction commit markers and aborted records occupy real offsets the
consumer never returns, and retention means consumption need not start at zero. The density
adaptation (seeding, bridging, position advance) repairs this at receive time — see
[state and recovery](../design/state.md#density-adaptation) and
[liveness](liveness.md).

One further deviation from the textbook is Kafka-specific: the sender's clock increment is
performed by the broker (offset assignment), learned asynchronously from producer
acknowledgements. A record's own coordinate therefore cannot appear in its own stamp, and a
node claims its in-flight sends in its own send-sequence space, resolved by receivers from
the sender tag every stamped record carries — developed in
[architecture](../design/architecture.md#the-stamp).

## The delivery invariant

Stated the way the verification oracle checks it, with no reference to the protocol's own
clocks: when record `m` is delivered to the user processor at node `n`, every ancestor `a` of
`m` (under real happened-before) with `a`'s channel consumed by `n` and `a`'s offset at or
above `n`'s baseline on that channel has already been delivered at `n`. The baseline is the
seed point — history below a node's first sighting of a channel is outside its scope by
definition, a finite-retention fact rather than an ordering exception.

## Preconditions

The guarantee holds under these environmental conditions:

- **Stable channel identity**: topic UUIDs do not change mid-run. A recreated topic is a new
  channel; its old incarnation's history is loss, never reordering.
- **Exactly-once processing**: `exactly_once_v2`, with `read_committed` consumption. All of
  Parsley's state commits in the task's transaction, which is what makes crash recovery a
  pure restore.
- **Truthful stamps between causally related topics**: every processor on a causal path
  stamps its outputs (Parsley stages do this automatically; plain producers use a
  [`CausalClock`](../guide/clients.md)). An absent stamp claims nothing — safe for the record
  itself, invisible to downstream ordering.
- **Closed handler effects.** A handler's logic is arbitrary, but the guarantee covers only
  effects that leave through its return value: returned emissions are stamped, claimed, and
  ordered downstream, and returned fold state commits with the delivery. An effect that
  bypasses the boundary — a write to an external system, a send through a private producer,
  a mutation of shared state — is invisible to the causal model; no stamp claims it and no
  gate can order against it. Keep logic pure and route causally significant effects through
  emissions.
- **Co-partitioning by key across a stage's input topics.** A task consumes partition *p* of
  every source topic, and the guarantee is per task: every cause *this task* consumes is
  delivered first. Causally related records keyed to different partition slices land in
  different tasks, each of which correctly ignores the other slice's dependency — so a
  processor reading its inputs as whole topics observes causal order only when related keys
  route to the same slice. This is the same precondition Kafka Streams processors carry for
  joins and aggregations, and like Streams, Parsley does not enforce it.
- **Retention covers consumer lag** on causal topics: a consumer must be able to fetch, or
  observe the skip of, every offset a claim names. An undersized retention fails loudly at
  the consumer's position reset, never silently.
