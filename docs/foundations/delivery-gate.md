# The delivery gate

The delivery gate is the predicate Parsley delivers under. It is the point where the
[causal-broadcast module](../protocols/causal-broadcast.md) decides whether a received record may
reach the user's processor now or must wait. The whole guarantee rests on it being sound without
the total-visibility assumption that [classical algorithms](causal-consistency.md) require.

> A dependency on a coordinate this node consumes is satisfied only by this node's own contiguous
> delivered frontier reaching it. A dependency on any other coordinate is ignored, unconditionally.

Evaluated over the normalised dependency clock (the record's exact self-cycle stripped), the gate
is two branches, and it is total:

```
for each coordinate c in deps:
    if consumed(c):  require frontier(c) >= deps[c]   # local delivery, never hearsay
    else:            ignore                            # counted by a metric, never a failure
```

`consumed(c)` means an input channel of this task, on a partition this task owns. A record carrying
no clock at all (a plain Kafka producer on a consumed topic) is causally minimal by definition, a
producer that stamps nothing claims nothing, and is deliverable immediately.

## Why local delivery is required

This is the Birman–Schiper–Stephenson CBCAST delivery condition,[^bss] instantiated on Kafka's own
`(topicId, partition)` coordinates. In BSS a process delivers a message only once *its own*
delivered vector covers the message's timestamp. The condition is over the receiver's own delivery
history, never over what a peer reports having delivered.

The distinction matters because the guarantee Parsley makes is about the *order this node's
processor observes events in*, not merely about whether an event exists somewhere. "K reached
offset k" being true at some peer does not mean this node's delegate has processed K@k yet. If a
claim carried on a sibling channel could satisfy the gate, a record caused by K@k could reach the
delegate before K@k itself does on this node's own K subscription, an effect delivered before its
cause to a processor subscribed to both. So the gate checks the local frontier exclusively, and an
advertised claim is only ever *carried* in the outbound stamp, for downstream nodes to verify
against their own frontiers in turn.

## Why ignoring unconsumed coordinates is sound

A producer stamps a clock spanning everything it consumed, so a record routinely names coordinates
a narrower downstream consumer has no subscription to. Ignoring them is sound, not vacuous
satisfaction, and the argument is a transitivity theorem over two invariants of the stamp:

- **Transitive completeness.** Every node's outbound stamp dominates the dependency clocks *and the
  coordinates* of every event it has delivered.
- **Unconditional merge.** Every clock fold, the channel folds, the advertised state, every
  outbound stamp, merges the entire inbound clock, including coordinates on channels the node does
  not consume. The gate may ignore; the merge may not.

Together these mean that for any causal chain m₁ → m₂ → m₃ that passes through an unconsumed middle
channel, every intermediate node delivered its predecessor and merged unconditionally, so m₃'s
clock claims m₁ *directly*. An unconsumed entry in a clock only ever proxies ancestry the same
clock already states explicitly, and ignoring it loses no ordering observable at this node. Each
ignore is counted by the `deps-out-of-scope-ignored` metric. These two properties are the invariants
[I2 and I9](invariants.md).

The ignore branch is also what makes joins coordination-free. A new application's stamps routinely
circulate coordinates that incumbent nodes have no channel for, and the gate ignores those soundly
instead of treating them as an error, so joining a running topology needs no admission, no barrier,
and no membership (see [Streams integration](../guide/streams.md#evolving-a-running-topology)).

## The outbound stamp

The gate consults the local frontier; the stamp is what carries a node's knowledge onward. The
stamp attached to every outbound record, business forwards and protocol null messages alike, through
one stamping site, is the node's vector time, the max-merge of:

- **the contiguous frontier, the carried ancestry** from any retired channels, **and every input
  channel's advertised clock**. This is transitive ancestry, carried downstream for
  each receiver's own gate to verify locally.
- **ownOutputs**: the node's own acknowledged output positions. The broker performs the sender's
  clock increment (offset assignment), learned via producer acknowledgements, so a node's second
  output provably claims its first, including across sink topics and partitions, which a pre-stamp
  crossing wait covers.
- **highestDelivered**: the max projection of the delivered set, so an output emitted from a
  record delivered above a contiguous-frontier gap still claims that record.

Some stamp entries deliberately over-claim: the init-time end-offset seed, an aborted transaction's
acknowledgements, the gap offsets below an above-gap claim. Every such entry names a position at or
below a real appended offset, so an over-claim can only *delay* downstream delivery, until the
position is delivered or bridged, never reorder it. The gate is monotone in the dependency clock and
frontiers advance only on physically received offsets, so raising a stamp entry can only move
outcomes toward holding, which is fail-safe, never unsafe (invariant [I8](invariants.md)). The delay
is unbounded if the claimed channel goes permanently silent; a record held on a dependency above its
channel's highest received offset beyond a threshold is surfaced by the
`records-held-above-highest-received` gauge, since that is the exact signature of a claim nothing
received so far can satisfy.

## Violations

There is no path that breaks Lamport's happened-before guarantee. Delivery is unconditionally
fail-closed: there is no eviction and no configuration that trades causal order for availability. A
record that can be proven impossible to evaluate, an undecodable payload or dependency header, fails
the task fast rather than being delivered out of order or dropped; the task restarts, and the record
stays in the buffer changelog for recovery. A dependency on a coordinate this node does not consume
is not a violation and not a failure: it falls to the gate's ignore branch, which the transitivity
theorem above makes sound.

This takes the consistency side of the trade-off described in the CAP theorem and in the
causal-consistency literature unconditionally: Parsley has no policy knob that spends causal order
for liveness. A genuinely stuck dependency (a lagging partition, a co-partitioning gap, a producing
topic that was deleted) shows up as unbounded buffer growth or a fail-closed task restart, never as
a silent reordering. See [Troubleshooting](../guide/troubleshooting.md).

[^bss]: Kenneth Birman, André Schiper, and Pat Stephenson, "Lightweight Causal and Atomic Group
    Multicast", *ACM Transactions on Computer Systems*, 1991. The ISIS system's CBCAST protocol. See
    the [bibliography](../reference/bibliography.md).
