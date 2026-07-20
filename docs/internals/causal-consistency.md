# Causal consistency model

Parsley's concrete guarantee is causal delivery order for a Kafka Streams processor: records are
delivered to `process()` only after every dependency the processor consumes has been satisfied,
subject to the conditions in [Streams integration](../streams.md). This page covers the
theoretical model the guarantee is built on, why Kafka stream processing does not satisfy the
assumptions traditional causal-consistency algorithms make, how Parsley's three protocol layers
restore them, and the environmental assumptions the guarantee itself rests on.

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

Vector clocks (Fidge 1988, Mattern 1988) implement the happened-before relation. Each process
maintains a clock: a map from process identifiers to the highest event number it has delivered
from that process. When a process delivers an event, it advances its own entry. When it sends a
message, it attaches a snapshot of its clock. A receiving process holds the message until its
local clock dominates the attached snapshot — until it has delivered everything the sender had
observed. Parsley's clocks are the indexed-by-channel variant: keys are `(topicId, partition)`
coordinates and values are broker offsets, because a Kafka partition — not a process — is the
unit that carries a total order.

## How traditional algorithms work

Systems like COPS and causal broadcast protocols build on vector clocks under one shared
assumption: every node that must enforce causal ordering receives every write. In COPS, writes
propagate to all datacenters, so the delivery predicate is always checkable — the coordinates in
a dependency clock are coordinates this datacenter will eventually observe. In causal broadcast
protocols the same assumption holds in a different form: every process in the group receives
every message, so the delivery predicate (local clock dominates the attached clock) always
converges.

This is the total visibility assumption: a node enforcing causal order has, or will have,
visibility into every event the dependency clock references.

The second assumption is about the transport: messages travel on reliable FIFO channels with
stable identity and dense sequencing. Both assumptions fail for Kafka stream processing, in
different ways, and Parsley's two lower layers exist to deal with exactly one each.

## The three protocol layers

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

- The **[channels module](channels.md)** repairs the transport assumption. Kafka partitions are
  not classical channels: topic recreation rebinds names, EOS commit markers and aborted records
  occupy offsets a consumer never sees, retention truncates history, the broker assigns the
  sender's own sequence numbers asynchronously, and Parsley itself delivers within a partition
  out of order. UUID-keyed coordinates, seeding, bridging, the contiguous frontier, and
  own-output tracking each repair one of these, so the layer above sees dense, stable channels.
- The **[causal-broadcast module](causal-broadcast.md)** repairs the visibility assumption — not
  by restoring total visibility, but by making the delivery predicate sound without it, with the
  two-branch gate described next.
- The **[gossip module](gossip.md)** is a liveness layer: it keeps clock progress observable
  through processors that produce no business output, so downstream completeness never stalls on
  a quiet path. It can never release a record the gate would hold.

## The delivery gate

> A dependency on a coordinate this node consumes is satisfied only by this node's own contiguous
> delivered frontier reaching it. A dependency on any other coordinate is ignored,
> unconditionally.

Evaluated over the normalised dependency clock (the record's exact self-cycle stripped), the gate
is two branches, total:

```
for each coordinate c in deps:
    if consumed(c):  require frontier(c) >= deps[c]   # local delivery, never hearsay
    else:            ignore                            # counted by a metric, never a failure
```

`consumed(c)` means an input channel of this task, on a partition this task owns. A record
carrying no clock at all (a plain Kafka producer on a consumed topic) is causally minimal by
definition — a producer that stamps nothing claims nothing — and is deliverable immediately.

### Why local delivery is required

This is the Birman–Schiper–Stephenson CBCAST delivery condition, instantiated on Kafka's own
`(topicId, partition)` coordinates: in BSS, a process delivers a message only once *its own*
delivered vector covers the message's timestamp — the condition is over the receiver's own
delivery history, never over what a peer reports having delivered. The distinction matters
because the guarantee Parsley makes is about the *order this node's processor observes events
in*, not merely about whether an event exists somewhere. "K reached offset k" being true at some
peer does not mean this node's delegate has processed K@k yet: if a claim carried on a sibling
channel could satisfy the gate, a record caused by K@k could reach the delegate before K@k itself
does on this node's own K subscription — an effect delivered before its cause, to a processor
subscribed to both. So the gate checks the local frontier exclusively, and an advertised claim is
only ever *carried* (in the outbound stamp) for downstream nodes to verify against their own
frontiers in turn.

### Why ignoring unconsumed coordinates is sound

A producer stamps a clock spanning everything it consumed, so a record routinely names
coordinates a narrower downstream consumer has no subscription to. Ignoring them is sound, not
vacuous satisfaction, and the argument is a transitivity theorem over two invariants of the
stamp:

- **Transitive completeness.** Every node's outbound stamp dominates the dependency clocks *and
  the coordinates* of every event it has delivered.
- **Unconditional merge.** Every clock fold — the channel folds, the advertised state, every
  outbound stamp — merges the entire inbound clock, including coordinates on channels the node
  does not consume. The gate may ignore; the merge may not.

Together these mean that for any causal chain m₁ → m₂ → m₃ that passes through an unconsumed
middle channel, every intermediate node delivered its predecessor and merged unconditionally — so
m₃'s clock claims m₁ *directly*. An unconsumed entry in a clock only ever proxies ancestry the
same clock already states explicitly, and ignoring it loses no ordering observable at this node.
Each ignore is counted by the `deps-out-of-scope-ignored` metric.

Earlier versions instead failed the task fast on a dependency naming a coordinate the node had no
channel for. That fail-fast added no safety — no execution exists, under the fault model stated
below, in which it prevents a causal-order violation — and it actively manufactured a
coordination problem: incumbents crashed whenever a new application's stamps first circulated, so
joins needed admission, barriers, and membership. All of that machinery has been removed. Joining
a running topology needs zero coordination (see
[Concepts](../concepts.md#joining-a-running-topology)).

## The outbound stamp

The stamp attached to every outbound record — business forwards and protocol null messages alike,
through one stamping site — is the max-merge of three clocks:

- **completeness**: the node's contiguous frontier, its carried ancestry from any retired
  channels, and every input channel's advertised clock — transitive ancestry, carried downstream
  for each receiver's own gate to verify locally;
- **ownOutputs**: the node's own acknowledged output positions. The broker performs the sender's
  clock increment (offset assignment), learned via producer acknowledgements, so a node's second
  output provably claims its first — including across sink topics and partitions, which a
  pre-stamp crossing wait covers;
- **highestDelivered**: the max projection of the delivered vector, so an output emitted from a
  record delivered above a contiguous-frontier gap still claims that record.

Some stamp entries deliberately over-claim — the init-time end-offset seed, an aborted
transaction's acknowledgements, the gap offsets below an above-gap claim. Every such entry names
a position at or below a real appended offset, so an over-claim can only *delay* downstream
delivery (until the position is delivered or bridged), never reorder it. The gate is monotone in
the dependency clock and frontiers advance only on physically received offsets, so raising a
stamp entry can only move outcomes toward holding — fail-safe, never unsafe. The delay is
unbounded if the claimed channel goes permanently silent; a record held on a dependency above its
channel's highest received offset beyond a threshold is surfaced by the
`records-held-above-highest-received` gauge, since that is the exact signature of a claim nothing
received so far can satisfy.

## Environmental assumptions

The guarantee rests on three stated assumptions about the environment. None is enforced by
coordination; the first two are enforced in code where that is possible, and all three are
operational constraints to design for.

### E1 — stable channel identity

A coordinate must never rebind to a different record. Kafka violates this on topic delete and
recreate (offsets restart under a new incarnation), so identity is keyed by topic UUID and bound
once per process lifetime, and a background topic-identity poll fails the application fast when a
causal topic's UUID changes mid-run. Detection is bounded by the poll interval, not
instantaneous, so live recreation of a causal topic remains an operational error — loud and
bounded rather than silent. Across a restart, recreation degrades to history loss (E2's kind),
never reordering: a clock naming the old UUID can never be satisfied by the new topic's offsets.
See [Troubleshooting](../troubleshooting.md#a-causal-topic-was-deleted-or-recreated-while-the-application-ran).

### E2 — retention must not destroy causally-live history

A record whose effects some running or future consumer still needs must not be expunged before
every such consumer has delivered it. No protocol can deliver a destroyed record. Parsley
enforces the detectable half in code: every causal source is configured with
`AutoOffsetReset.none()`, so a consumer whose position falls out of range fails fast rather than
silently jumping past destroyed causes, and the offset seeder seeds log-start positions only on a
genuine first start, refusing when surviving state shows the group is not new (offset expiry is
not a first start). Mid-replay retention expiry is therefore a loud crash-loop until an operator
resets — a liveness stall by design, never a reorder. The preventative half is a retention-sizing
constraint on the operator: retention on causal topics must comfortably exceed the longest
consumer outage or replay you intend to survive. See the
[operations note in Streams integration](../streams.md#operating-notes) and
[Troubleshooting](../troubleshooting.md#retention-outran-a-causal-consumer).

### E3 — compliant participants; participation is per-path

Every producer that stamps clocks is assumed to stamp truthfully and transitively completely, and
to preserve received causal history. Producers that stamp nothing are causally minimal *sources*
— safe by construction, no declaration needed. The subtle consequence: **causal order is
guaranteed only along paths where every intermediate processor stamps.** A service that consumes
stamped topics and re-produces unstamped output silently severs the custody chain — its outputs
are causally minimal by definition, and the severance is undetectable at runtime, because an
unstamped record is indistinguishable from a genuine external event. This is an architectural
constraint of the same kind as E2's retention constraint: place every processor that sits between
causally related topics inside the stamping boundary (a Parsley stage, or a client using the
`CausalClock` edge operations). Byzantine stamping — forged clocks, invented offsets — defeats
any causal-broadcast algorithm and is out of scope.

## Violations

There is no path that breaks Lamport's happened-before guarantee. Delivery is unconditionally
fail-closed: there is no eviction and no configuration that trades causal order for availability.
A record that can be proven impossible to evaluate — an undecodable payload or dependency header —
fails the task fast rather than being delivered out of order or dropped; the task restarts, and
the record stays in the buffer changelog for recovery. A dependency on a coordinate this node
does not consume is not a violation and not a failure: it falls to the gate's ignore branch,
which the transitivity theorem above makes sound.

This takes the consistency side of the trade-off described in the CAP theorem and in the causal
consistency literature unconditionally: Parsley has no policy knob that spends causal order for
liveness. A genuinely stuck dependency (a lagging partition, a co-partitioning gap, a producing
topic that was deleted) shows up as unbounded buffer growth or a fail-closed task restart, never
as a silent reordering — see [Troubleshooting](../troubleshooting.md).
