# The delivery gate

The gate is the predicate that decides whether a received record may reach the user processor.
Everything else in the protocol exists to make this predicate sound and its inputs cheap.

## Head-of-line blocking

Each consumed channel has a FIFO hold queue. Records enter in receive order — which is offset
order, because Kafka delivers a partition in order — and only the **head** of each queue is
ever evaluated. A blocked head blocks its channel; it never blocks other channels.

This is the design's central structural choice. Delivering out of order within a partition
would buy lower convoying latency, at the price of a second projection of the delivered
vector, an index of ahead-of-frontier deliveries, and a gap-absorption walk on every advance.
Head-of-line blocking collapses all of that: one frontier per channel, one queue per channel,
and a gate that only ever looks at heads. Convoying within a channel is the accepted cost.

Two consequences fall out for free:

- **Per-channel FIFO delivery** at every consumer, so a record never needs to claim its own
  channel's earlier offsets — same-channel order is structural.
- **The frontier is contiguous by construction**: it advances only when the head delivers, or
  when a known-clean prefix folds in (seed, bridge, position advance).

## The predicate

A record `m` arrives on consumed channel `c` carrying dependency clock `D` (absent header
means the empty clock — a producer that stamps nothing claims nothing). When `m` reaches the
head of `c`'s queue:

1. **Normalise**: ignore `D`'s entry for `c` itself. At or above `m`'s own offset it is a
   self-cycle (an event cannot precede itself); below it, per-channel FIFO has already
   delivered everything the entry could name. Either way the entry carries no information the
   structure has not already enforced.
2. **Restrict**: keep only entries whose channel this node consumes. Unconsumed entries are
   **ignored** — the ignore branch, justified below.
3. **Gate**: `m` is deliverable iff the frontier dominates the restricted clock pointwise —
   for every entry `(ch, n)`, `frontier[ch] ≥ n`.

Delivering `m` advances `frontier[c]` to `m`'s offset and re-evaluates every other head; the
cascade runs to fixpoint. A delivery can release a chain of held heads across channels in one
step.

Fail closed: a present but undecodable clock header fails the task. Treating it as empty would
silently drop the sender's claims and let an effect precede its cause; failing leaves the
offset uncommitted for a retried, successful decode.

## Why the ignore branch is sound

The gate ignores dependency entries on channels the node does not consume. This is safe only
if every *consumed* ancestor hiding behind an unconsumed coordinate is claimed **directly** in
the same clock — otherwise ignoring would open a hole exactly one hop wide.

That directness is an induction along business paths. Consider any real ancestor `a` of `m`
with `a` consumed here. Causality flows only through business records, so there is a chain
`a → x₁ → … → m` of deliveries and emissions. The node that delivered `a` has `a` in its
frontier; its stamp folds the frontier, so `x₁`'s clock claims `a`. Every downstream node
folds the **full** clock of every record it receives into its per-channel advertised clocks,
and its own stamps fold those — so the claim on `a` survives every hop to `m`'s clock,
whether or not any intermediate node consumed `a`'s channel. The restricted comparison in step
3 therefore sees `a` directly, and the unconsumed entries it skipped carried nothing the gate
needed.

This is also why the per-channel advertised clocks fold at **receive** rather than delivery:
the stamp of any output emitted after delivering a record must dominate that record's entire
clock, and folding early only ever over-claims real appended offsets — a delay, never a
reorder (see [liveness](liveness.md#why-over-claims-cannot-deadlock)).

## Replays

A received offset at or below the channel's frontier is dropped without effect. Offsets only
reach that state through a committed delivery, a seed, or a scope-growth resume, so the drop
is a replay guard, not a filter — under exactly-once commits it fires only when a scope change
deliberately re-reads a prefix the node has already accounted for.
