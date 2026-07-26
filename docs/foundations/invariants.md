# Named invariants

The implementation preserves a fixed set of named invariants: the properties the three protocols
guarantee, in the module style's sense. This page is the definitive statement of each, and the
numbering is stable: numbers are never reused. The [delivery gate](delivery-gate.md) and the
[causal consistency](causal-consistency.md) model give the arguments these invariants encode; the
environmental assumptions E1–E3 they rest on are stated under
[environmental assumptions](assumptions.md).

## I1 — causal delivery

A record reaches the delegate only after every consumed-coordinate dependency has been locally
and contiguously delivered. There is no timeout, no eviction, and no configuration that trades
causal order for liveness.

## I2 — stamp transitive completeness

Every node's outbound stamp dominates the dependency clocks and the coordinates of every event
it has delivered, including records delivered out of order above a contiguous-frontier gap.
This is the precondition of the gate's ignore branch: a dependency on an unconsumed channel can
be ignored only because any consumed ancestor behind it is claimed directly in the same clock.
The above-gap case is carried by `highestDelivered` in the channels module: the maximum
projection of the delivered vector, observed on every delivery, folded into the stamp (never
into the gate), and reconstructed at restart from the forwarded index. The cross-node inductive
step of this invariant is I9.

## I3 — per-producer stamp monotonicity

A producer's successive stamps onto one partition are non-decreasing. This is what makes
non-head-of-line delivery within a partition preserve FIFO order per producer: if an earlier
record from a producer is held, every later one is held too. Any change to stamping must
preserve it. The `ownOutputs` fold does, because own outputs only grow.

## I4 — contiguous frontier

A coordinate's frontier value is the highest offset delivered without a gap. It never advances
past an undelivered offset.

## I5 — normalised clocks

After channel-layer normalisation, no clock inside the causal-broadcast layer carries a
self-reference. Normalisation happens once, at receive time, in `ParsleyChannels`.

## I6 — relay on consumed-channel advance

A null message is relayed if and only if its carried clock advanced this node's knowledge on a
channel the task consumes at its own partition. Carried custody folds unconditionally (I9) and
rides every later emission, but custody never obliges a relay. The trigger is restricted to
consumed-scope advances because a whole-clock trigger (relay whenever the carried clock is not
dominated) provably does not quiesce on topic cycles of three or more nodes; the analysis and the
rule are in [the gossip module](../protocols/gossip.md#the-relay-rule).

## I7 — not in use

Reserved. Invariant numbers are never reused.

## I8 — stamp over-claim soundness

A stamp entry naming a real log position this node has not verified — an end-offset seed, an
aborted-transaction acknowledgement, another producer's record on a shared sink, or the gap
offsets implicitly claimed below an above-gap `highestDelivered` entry — can only delay
downstream delivery, never reorder it. A committed record at the claimed position is
eventually delivered, and an aborted one is a consumer-skipped hole the bridge folds. The
argument is a monotonicity lemma: every I8 mechanism only adds or raises clock entries, the
gate is monotone in the dependency clock, and frontiers advance only on physically received
offsets, so an over-claim can only move an outcome toward holding. The delay is unbounded when
the claimed channel-partition goes permanently silent (an aborted tail whose producer died, or
keying that never revisits the partition); that is fail-safe, never unsafe, and observable
through the `records-held-above-highest-received` gauge and its warning log. Any future stamp
source must preserve the rule that a stamp entry always names a position at or below a real
appended offset.

## I9 — unconditional merge

The channel-clock fold, the advertised state, and every outbound stamp merge the entire
inbound clock, including coordinates on channels this node does not consume. The gate may
ignore; the merge may not. This custody chain is the inductive step that makes I2 hold across
nodes with different consumption sets: a chain m1 → m2 → m3 through an unconsumed middle
channel surfaces m1 directly in m3's clock only because every intermediate node merged
unconditionally. Because the frontier is both gate-consulted and stamp-re-emitted, scope
changes split the two views: on shrink, pruned frontier entries and retired channels' values
re-home into a carried-ancestry clock the stamp keeps merging; on growth, added channels seed
from that carried ancestry. Carried ancestry may be skipped, never dropped, and never
re-entered; only provably destroyed coordinates (recreated topic UUIDs) leave stamp-feeding
state.
