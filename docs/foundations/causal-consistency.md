# Causal consistency

Parsley's guarantee is causal delivery order for a Kafka Streams processor: a record reaches
`process()` only after every dependency the processor consumes has been delivered locally. This
page develops the model that guarantee is built on. It covers Lamport's happened-before relation
and its realisation with vector clocks, the two assumptions traditional causal-consistency
algorithms make, and the reason Kafka stream processing satisfies neither. The
[delivery gate](delivery-gate.md) then states the predicate Parsley delivers under, and
[environmental assumptions](assumptions.md) states what the guarantee rests on in the environment.

## Lamport's happened-before relation

The happened-before relation is a partial order on events, written A → B.[^lamport] It holds when:

- A and B occur in the same process and A occurs before B, or
- A is the sending of a message and B is its receipt, or
- there is a C such that A → C and C → B (transitivity).

Causal consistency, applied to a storage or streaming system, says that if a write W1 happened
before a write W2, then any process that observes W2 has previously observed W1. The transitive
case is the one that matters in practice: a consumer need not have subscribed to every intermediate
step to be bound by the ordering that runs through them.

In the consistency hierarchy this sits above eventual consistency, which constrains no order, and
below linearisability, which orders every event through a single global timeline. Causal
consistency is the strongest model that can be maintained without coordination, which is what lets
Parsley deliver it with no membership, no admission barrier, and no configuration.

## Vector clocks

Vector clocks realise the happened-before relation.[^vectorclocks] Each process maintains a clock,
a map from process identifiers to the highest event number it has delivered from that process. When
a process delivers an event it advances its own entry. When it sends a message it attaches a
snapshot of its clock. A receiving process holds the message until its local clock dominates the
attached snapshot, that is, until it has delivered everything the sender had observed.

Parsley's clocks are the indexed-by-channel variant. The keys are `(topicId, partition)`
coordinates and the values are broker offsets, because in Kafka a partition, not a process, is the
unit that carries a total order. A clock plays two roles, distinguished in the literature as VT(m)
and VT(p): the dependencies attached to a record, and the knowledge a node accumulates as it
delivers. `CausalClock` is the public facade over both.

## How traditional algorithms work

Systems such as COPS[^cops] and classical causal-broadcast protocols build on vector clocks under
one shared assumption: every node that must enforce causal ordering receives every write. In COPS,
writes propagate to all datacenters, so the delivery predicate is always checkable, because the
coordinates in a dependency clock are coordinates this datacenter will eventually observe. In
causal-broadcast protocols the assumption takes a different form: every process in the group
receives every message, so the delivery predicate, that the local clock dominates the attached
clock, always converges.

This is the **total visibility** assumption: a node enforcing causal order has, or will have,
visibility into every event a dependency clock references.

A second assumption concerns the transport: messages travel on **reliable FIFO channels** with
stable identity and dense sequencing. Both assumptions fail for Kafka stream processing, in
different ways, and each of Parsley's two lower layers exists to deal with exactly one of them.

## The three protocol layers

Parsley restores both assumptions and delivers under them, in a stack of three protocols, each
presented in the module style of Cachin, Guerraoui, and Rodrigues.[^cgr] Each layer consumes the
guarantees of the one below it and offers a clean assumption set to the one above.

```
┌──────────────────────────────────────────────────────────────────┐
│ gossip (ParsleyGossip)             liveness / clock dissemination │
│   causal progress observable on every channel, converging on      │
│   any topology shape including cycles                             │
├──────────────────────────────────────────────────────────────────┤
│ causal broadcast (ParsleyCausalBroadcast)   receive / deliver     │
│   Birman–Schiper–Stephenson causal delivery to the user's         │
│   delegate, over gap-free, stable-identity channels               │
├──────────────────────────────────────────────────────────────────┤
│ channels (ParsleyChannels)         Kafka → reliable-FIFO-channel  │
│   adaptation: dense, stable-identity event coordinates; the       │
│   node's own output positions; normalised dependency clocks       │
└──────────────────────────────────────────────────────────────────┘
```

- The **[channels module](../protocols/channels.md)** repairs the transport assumption. Kafka
  partitions are not classical channels, and each way they fall short has one mechanism that
  answers it, so that the layer above sees dense, stable channels:
  - Topic recreation rebinds a name to a fresh partition history. **UUID-keyed coordinates**
    identify a channel by the topic's stable Kafka UUID, so a coordinate never silently rebinds.
  - EOS commit markers and aborted records occupy offsets a consumer never returns. **Bridging**
    folds a permanently skipped run of offsets into the contiguous walk, so a marker cannot wedge
    the frontier forever.
  - Retention truncates history below the first offset a node ever observes. **Seeding** folds
    everything below that first sighting into the frontier as a density baseline.
  - The broker assigns the sender's own sequence numbers asynchronously. **Own-output tracking**
    reconstructs the node's own clock entry from produce acknowledgements.
  - Parsley itself delivers within a partition out of order. **The contiguous frontier** advertises
    only the gap-free prefix of what a node has delivered.
- The **[causal-broadcast module](../protocols/causal-broadcast.md)** repairs the visibility
  assumption, not by restoring total visibility but by making the delivery predicate sound without
  it, with the two-branch [delivery gate](delivery-gate.md).
- The **[gossip module](../protocols/gossip.md)** is a liveness layer: it keeps clock progress
  observable through processors that produce no business output, so downstream progress never
  stalls on a quiet path. It can never release a record the gate would hold.

The [protocols overview](../protocols/index.md) gives the module boxes and the class map;
[named invariants](invariants.md) catalogues the properties I1–I9 the layers preserve.

[^lamport]: Leslie Lamport, "Time, Clocks, and the Ordering of Events in a Distributed System",
    *Communications of the ACM*, 1978. See the [bibliography](../reference/bibliography.md).
[^vectorclocks]: Colin Fidge, "Timestamps in Message-Passing Systems That Preserve the Partial
    Ordering", 1988; Friedemann Mattern, "Virtual Time and Global States of Distributed Systems",
    1988. See the [bibliography](../reference/bibliography.md).
[^cops]: Wyatt Lloyd, Michael J. Freedman, Michael Kaminsky, and David G. Andersen, "Don't Settle
    for Eventual: Scalable Causal Consistency for Wide-Area Storage with COPS", 2011.
[^cgr]: Christian Cachin, Rachid Guerraoui, and Luís Rodrigues, *Introduction to Reliable and
    Secure Distributed Programming*, 2nd edition, 2011.
