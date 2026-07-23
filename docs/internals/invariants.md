# Named invariants

The implementation preserves a fixed set of named invariants. Javadoc, tests, and commit
messages cite them by number (I1, I2, and so on), and this page is the definitive statement of
each. The numbering is stable: a retired invariant keeps its number so historical citations
stay readable. The environmental assumptions E1–E3 that these invariants' soundness arguments
rest on are stated in the
[causal consistency model](causal-consistency.md#environmental-assumptions).

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
rides every later emission, but custody never obliges a relay. The original whole-clock trigger
(relay whenever the carried clock was not dominated) provably never quiesced on topic cycles of
three or more nodes; the analysis and the current rule are in
[the gossip module](gossip.md#the-relay-rule).

## I7 — retired

I7 was "fail closed outside certification": a coordinate on a channel the node had no
certified knowledge of failed the task fast. The certification concept was removed together
with membership. Under the fault model, an unconsumed coordinate in a clock was stamped by a
compliant participant and its consumed ancestry rides in the same clock (I2 and I9), so it is
safely ignorable; the fail-fast added no safety and made joins require coordination. The
replacement observability is the out-of-scope-ignored metric and startup topology validation.
The number stays retired in place so later invariants keep theirs.

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
