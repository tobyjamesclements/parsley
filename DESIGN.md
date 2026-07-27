# Parsley fresh-start design

Ground-up rewrite of Parsley: causal delivery order for Kafka stream processing. Same intent as
the original — a record reaches the user's processor only after every cause the processor
consumes has been delivered locally — with a different architecture. This document is the spec;
the simulator's oracle (`src/test/java/.../sim`) enforces it mechanically.

## Departures from the original architecture

1. **Verification first.** A deterministic simulator with a ground-truth causal oracle is the
   primary correctness gate. The simulator models partitions, offsets, EOS transactions (commit
   markers occupying offsets, aborted records), asynchronous producer acknowledgements, node
   crashes with transactional state restore, and seeded random interleavings. The oracle computes
   real happened-before ancestry outside the protocol and checks every delivery against it.
2. **Head-of-line blocking per channel.** Delivery within a channel is strictly FIFO: the head of
   a channel's hold queue is the only record ever gated. This collapses the original's two
   projections of the delivered vector into one (`highestDelivered` is gone), and removes the
   forwarded index, the contiguous absorb walk, and the candidate index. The buffer is a
   per-channel FIFO queue. The cost is convoying within a channel, accepted as the default.
3. **Transport-agnostic core.** The protocol core (`core` package) has no Kafka dependency. It
   speaks to the world through small SPIs: `StateStore` (keyed bytes, transactional with
   delivery), `SendTracker` (async acknowledgements + end offsets), and a receive/deliver API
   driven by the host. Kafka Streams is one adapter (`kafka` package); the simulator is another.
4. **Keyed incremental persistence.** Causal state persists under per-channel keys, not one blob.
   Only what changed is written on an advance.
5. **Backpressure, not unbounded buffering.** The core signals `pauseWanted(channel)` when a
   channel's hold queue exceeds a bound; the host pauses fetching that channel. Safe for
   liveness: a held head's missing causes never arrive on the held channel itself.
6. **Truncation hook.** `truncate(stability)` drops clock entries at or below a supplied global
   stability bound from every stamp-feeding clock. Sound because every node's frontier already
   dominates the bound, so no gate anywhere still needs those entries. The stability input is an
   SPI; no coordination protocol is shipped, but the wire format and state layout support
   truncation from day one.

## Model

- **Channel**: `(topicId: UUID, partition: int)`. Identity is the topic's stable UUID (a
  recreated topic is a different channel).
- **Coordinate**: a channel plus a broker offset.
- **Clock**: map channel → offset watermark ("everything at or below is claimed").
- **Record identity for causality**: the coordinate it was appended at.

## Happened-before (ground truth, what the oracle checks)

`a → b` iff: same node delivered/emitted them in program order, or `b` was emitted by a node
after it delivered `a`, or transitively. **Delivery invariant**: when record `m` is delivered to
the user processor at node `n`, every ancestor `a` of `m` with `a.channel ∈ consumed(n)` and
`a.offset > baseline(n, a.channel)` has already been delivered at `n`. The baseline is the seed
point (below the node's first sighting is out of scope). **Liveness invariant**: when the
simulation drains (no in-flight records, all nodes stepped to fixpoint, no crashes pending),
every hold queue is empty. **Quiescence invariant**: the system emits no protocol records at
all, so an idle topology is byte-idle; a world that cannot drain inside its step budget fails.

## The gate

A record `m` arrives on consumed channel `c` with dependency clock `D` (from its header;
absent header = empty clock). Steps:

1. **Normalise**: drop `D`'s entry for `c` if it is `≥ m.offset` (the exact self-cycle; entries
   below `m.offset` on `c` are auto-satisfied by FIFO anyway).
2. **Restrict**: keep only entries whose channel is consumed by this node. Unconsumed entries
   are ignored — sound because a stamp claims consumed ancestry directly (transitive closure of
   claims), so any consumed cause behind an unconsumed coordinate has its own entry in `D`.
3. **Gate**: deliverable iff `frontier ≥ D|consumed` pointwise. Only the head of each channel's
   hold queue is ever evaluated.
4. On delivery: advance `frontier[c]` (contiguous by construction), then re-evaluate the heads
   of all channels whose head might have become deliverable (cascade).

Fail closed: an undecodable clock header fails the task; nothing is ever reordered or dropped.

## Density adaptation (channels layer)

Kafka partitions are not dense: EOS commit markers and aborted records occupy offsets a
`read_committed` consumer never returns, and retention means consumption starts above 0.

- **Seed**: the first offset ever received on a channel folds everything below it into the
  frontier (baseline).
- **Bridge**: Kafka delivers a partition in offset order, so a received offset above
  `highestReceived + 1` proves the gap was consumer-skipped (markers/aborts) — fold it.
  `highestReceived` persists per channel.
- **Trailing markers**: a claim may name a marker offset (end-offset seeding, below). The gap
  is bridged when any later record arrives, or by a position advance when none ever does.

## The stamp

Outbound records carry `stamp = frontier ∪ channelClocks ∪ carriedAncestry ∪ ownOutputs`
(pointwise max). Each term closes one escape route for a real cause:

- **frontier** — everything delivered here, contiguously.
- **channelClocks** — per consumed channel, the max clock carried by records received on it:
  ancestry that reached this node through channels it does not consume (keeps the ignore branch
  sound).
- **carriedAncestry** — delivered past on channels no longer consumed (scope shrink); merged
  forever, truncatable only via `truncate()`.
- **ownOutputs** — this node's own acknowledged sends, so its outputs order across partitions
  and sink topics.

Stamping happens at one site. Before stamping, the node folds all pending acknowledgements and
**waits for quiescence of unacknowledged own sends to other channels** (the crossing wait); an
ack failure or timeout fails the task — a stamp must never under-claim own outputs.

**Init end-offset seed**: persisted `ownOutputs` may trail the final pre-crash acks, so at init
each declared sink's end offset is folded in — an over-claim on real appended offsets, delay-only,
therefore sound.

## No gossip layer

The original architecture carries a third protocol layer of in-band null messages for
liveness. This design has none, because three properties make it unnecessary:

1. **Every claim names a really-appended offset** (frontiers, acknowledgement folds, and
   end-offset seeds are all append-time facts), so every dependency eventually has its record —
   or its skip — arrive at every consumer of its channel.
2. **Custody propagates along business paths alone.** Causality itself only flows through
   business records, and every stamp folds the full dependency clocks of everything received,
   so any consumed ancestor behind an unconsumed coordinate is claimed by induction along the
   same path the causality took. No side-channel dissemination is needed for the ignore
   branch's soundness.
3. **Position advances bridge trailing skips.** The one genuine liveness gap — a claim naming
   a trailing transaction marker's offset, with no later record to trigger a bridge — is closed
   by information Kafka already gives consumers for free: the consumer's position advances past
   markers and aborted batches even when no records return. The host reports that through
   {@code positionAdvance}, which is the entire liveness mechanism.

The wait graph is acyclic — every wait edge (a dependency wait or a head-of-line wait) points
from a later-appended record to an earlier-appended one, even under end-offset over-claims,
whose watermarks are append-time snapshots taken before the claiming record's own append — so
the system cannot deadlock, and V5's drain check enforces this empirically on every run.

Consequences: business topics carry no protocol records at all, plain consumers have nothing
to skip, retention never interacts with protocol traffic, and stream time is untouched.

## Own outputs and EOS

All causal state (frontier, channelClocks, carriedAncestry, ownOutputs, highestReceived, scope
record, hold queues) commits in the same transaction as the delivery that mutated it and the
sends it caused. On restart, uncommitted receives are refetched; uncommitted sends were aborted;
acknowledged-but-uncommitted own outputs are re-covered by the end-offset seed.

## Scope changes

The store records the declared input set. At init, a diff against the current declaration:
shrink → max-merge the departed channel's frontier and channel clock into carriedAncestry;
growth → seed the new channel at the node's carried knowledge of it (never log-start); a held
record whose channel left scope fails init loudly (fail closed). Destroyed-UUID entries are the
only outright removals.

## Package layout

- `io.github.tobyjamesclements.parsley.core` — protocol: `Channel`, `Clock`, `CausalNode`
  (gate + queues + stamp + position-advance bridging), SPIs (`StateStore`, `SendTracker`).
- `io.github.tobyjamesclements.parsley.kafka` — Kafka Streams processor adapter + edge ops.
- test `...parsley.sim` — the simulator: `SimCluster` (brokers/partitions/EOS), `SimNode`
  hosting a `CausalNode`, `Oracle` (ground-truth ancestry + invariants), `SimRunner` (seeded
  schedules, crash injection).

## Verification obligations (all enforced by tests)

| # | Obligation | Enforced by |
|---|---|---|
| V1 | Causal delivery invariant under random interleavings | `Oracle` on every delivery |
| V2 | No loss, no reorder within a channel (FIFO) | `Oracle` per-channel sequence check |
| V3 | Crash/restart preserves V1/V2 (EOS restore) | crash-injecting sim runs |
| V4 | EOS markers/aborts never wedge the frontier | sim runs with aborted transactions |
| V5 | Liveness: drained sim ⇒ empty hold queues | end-of-run assertion |
| V6 | Cycles drain; trailing markers never wedge | damped-feedback and filter scenarios |
| V7 | Truncation below a true stability bound preserves V1–V5 | truncating sim runs |
| V8 | The oracle itself catches a broken implementation | mutation tests of the sim (a
      deliberately FIFO-only node must fail V1) |
