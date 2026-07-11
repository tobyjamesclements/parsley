# Topology epochs

A causal topology sometimes has to change while it is running: add a stage, replace a stage, or
recompile one whose logic has moved. In Kafka Streams a new stage subscribes to its inputs from the
earliest offset and replays them from the start. Under the causal guarantee that replay is a problem.
The new node re-observes the entire history of its input topics and stamps records with offsets from
the beginning of time, and the shared completeness frontier is a minimum across every node, so a node
replaying from offset 0 drags that minimum down and un-strips history that every other node had long
since delivered.

Topology epochs solve this without a coordinator process and without stopping the topology. An
**epoch** defines a floor per `(topicId, partition)` coordinate. History below the floor is pre-epoch:
it feeds state but does not participate in causal time. A node deployed into a running topology adopts
the current floor, replays its inputs from the start with everything below the floor stripped, and so
never pins the shared frontier down. The whole mechanism is opt-in through setting
`parsley.coordination.epoch-events-topic`, described from the user's side in
[Evolving a running topology](../streams.md#evolving-a-running-topology); without it a topology runs in
**epoch 0**, whose floor is 0 everywhere, and behaves exactly as a topology with no epoch machinery at
all. Internally, `CausalStreams` owns a `ParsleyCoordination` handle over the configured topic — it is
not user-constructed.

## The floored clock

Each epoch `k` defines a floor `n_k` per coordinate; epoch 0 is floor 0 everywhere. There is one rule
applied every epoch: a dependency **below the floor is ignored** (it cannot have happened in this
epoch's domain), and a dependency **at or above the floor is gated normally**. The epoch is encoded by
offset position against the floor, so no per-record epoch header is needed.

Underneath that rule is a single invariant: **no causal clock carries an entry below the floor**. This
covers the decoded dependencies of an incoming record, the node's own frontier, each input channel's
clock, the `completeness()` output, and the outbound stamp. Delivering a below-floor record still feeds
the user's state — it is a real message the delegate should see — but it must not anchor the causal
frontier below the floor. This generalises the per-channel origin that `ParsleyFrontier` already seeds
at registration into a domain-wide, epoch-versioned origin.

Under epoch 0 the invariant is a no-op (nothing is below a floor of 0), which is why an uncoordinated
topology is bit-for-bit the same as before epochs existed.

## The epoch-events log

Coordination is leaderless and event-sourced. Every participating application instance shares one
single-partition **epoch-events log** topic for the domain and folds it through an identical
deterministic function, `ParsleyEpochLog`. Because the fold is a pure function of the totally-ordered
log, every node independently agrees on round ownership, membership, and each epoch's floor with no
leader and no lock. The log carries only this handshake; the floor itself travels in the data plane
(see [In-band markers](#in-band-markers) below).

The log has five event types, each carrying a member id (`application.id` plus the Streams task id, so
instances of the same application share it for failover while distinct applications and sub-topologies
stay distinct):

| Event | Meaning |
|---|---|
| `JoinRequested` | A node announces itself and declares its input and sink topics. It becomes a running member at the next commit. |
| `SnapshotRequested` | A node proposes a snapshot round. The first one after the last commit opens the round; the rest coalesce into it. |
| `FrontierPublished` | A running member publishes its completeness frontier for the open round. |
| `EpochCommitted` | The decided `(epochId, lowerBounds)` for the round. Idempotent, deduplicated by `epochId`. |
| `Leave` | A member is removed from the domain by its own graceful decommission. |

A round's lifecycle is defined by log position, not by an id in the events. The first
`SnapshotRequested` after the last `EpochCommitted` opens a round and elects its author as owner. Every
running member publishes its current completeness. Once all of them have published, the round is
complete and the new floor is the per-coordinate `mergeMin` of the published frontiers — the minimum,
over the members that observed each coordinate, of where they had reached. Applying the resulting
`EpochCommitted` advances the settled epoch, promotes pending joiners to running members, and clears
the round.

Because the commit is a deterministic function of the folded state and is deduplicated by `epochId`,
**any node with a local member commits a complete round**, not a single elected owner. Two nodes that
both commit produce the identical `EpochCommitted`, and the second fold is a no-op. There is no owner
whose failure stalls the round, so a departed owner cannot freeze the domain.

## The overlapping-epoch transition

A floor cannot switch instantaneously, because at the moment of transition the topology still holds
in-flight records written under the previous epoch. Parsley uses the classic two-clock solution: a
message is gated against the floor of the epoch it was **written** in, so the transition is an interval,
not an instant.

`ParsleyEpochState` sits above `ParsleyFrontier` and holds the settled floor plus, while a transition
is open, the pending next floor and the set of channels on which its opening marker has been seen. The
window opens on the first epoch marker and becomes engaged once that marker has arrived on every input
channel (a Chandy-Lamport cut across the node's inputs). It closes once the node's completeness
dominates the pending floor, at which point the pending floor is promoted to settled. The effective
floor stays at the previous epoch's value for the whole window, so a record written under the old epoch
is still gated correctly while the new floor is being adopted. This state lives in the frontier's
persisted blob, so a restart in the middle of a transition resumes it.

The transition is invisible in the data plane. A node's outgoing stamp is its delivered frontier, which
equals its completeness, carried unfloored; only the epoch-events log ever sees the received frontiers
and the decided floor.

## In-band markers

The floor reaches each coordinate in the data plane, not through the log. Two control records ride the
topics alongside business records, each identified by a header: an **epoch snapshot** marker (the
`SnapshotRequested` round's cut) and an **epoch boundary** marker (`_parsley_epoch_boundary`, carrying a
committed floor). They propagate the same way protocol watermarks do — a node that receives one relays
it to its children on the same key, so it stays on its partition lane end to end, and every partition is
covered because every upstream task relays. A marker is never delivered to the user delegate.

A node consuming a marker also advances: the relayed marker carries the node's own completeness, so one
record both drives the transition and advances the downstream channel clock. This reuses the machinery
that already propagates watermarks edge by edge through the topology — relayed further only when the
receiving channel's clock genuinely advanced, never unconditionally. That gate is what keeps a topology
with a genuine cycle (a stage that also consumes something derived from its own output) from
ping-ponging the same marker forever: once a node has already folded a marker's carried clock and it
teaches the node nothing new, relay stops there instead of looping.

## The source-topic registry

External producers — plain Kafka clients, or any system outside the topology — emit no epoch markers.
A stage consuming such a topic will therefore never receive a marker on that channel in-band, so it must
**self-initiate** the wave: publish its completeness when a round opens, inject the snapshot marker to
its children, and adopt its own source coordinate's floor from the committed `EpochCommitted` rather than
waiting for a marker that will never come.

Which topics are external is derived from the log, not configured. Every `JoinRequested` declares the
node's input channels and its sink topics, and the fold computes the domain's **external source topics**
as the inputs that no member produces:

```
external sources = (union of every member's inputs) − (union of every member's sinks)
```

A topic that some member consumes but no member produces is an entry point from outside the topology.
The derivation is over every declared member, running or still pending, so a node's source-layer
identity is correct from the very first round; a `Leave` drops that member's declaration, so a topic only
the departed member produced becomes external again. A node is source-layer when one of the topics it
consumes appears in this derived set, which it re-evaluates on each poll because the registry grows as
members join.

This replaces an earlier design in which each application was configured by hand with its own external
source topic names. A hand-written declaration could silently disagree with the real topology — a genuine
intermediate topic marked external, or an external topic left undeclared — and either mistake breaks the
wave for that coordinate. Deriving the registry from what every node actually consumes and produces
removes that class of misconfiguration. Sink topics are declared automatically from
`CausalStreamsBuilder`/`CausalProcessedStream#to(...)`; internally these become `sinkTopics` passed to
`ParsleyProcessors.Builder#sinkTopics`.

## Joining a running topology

A node deployed into an already-running topology must not begin consuming until an epoch computed
**without it** commits. Its sub-topology resets to earliest and would replay from offset 0, so if it
participated in the current epoch it would drag the floor's minimum over running members toward 0 and
un-strip history for the live nodes. The fold already encodes the escape: a joiner is a pending member
that publishes nothing, so it does not constrain the cut.

At `init()` a joining task joins, then blocks until it is a **running member** — which happens only when
a commit promotes it. That one rule covers every case. A fresh joiner is not a running member, so it
blocks until an epoch computed without it commits and admits it, then adopts that floor and settles its
epoch state directly at it (a joiner has no in-flight prior-epoch records, so there is no overlap window —
it strips everything below the floor from its first replayed record). A restart of an existing member is
still a running member on the log — nothing removes it while it is absent — so it proceeds at once and
resumes the floor from its persisted state, draining its restored buffer under that unchanged floor. A cold
start is epoch 0 and proceeds at once. The block is unbounded — there is no join timeout: a joiner never
proceeds on an unknown floor, so if the domain cannot yet commit (an existing member is absent) it simply
waits until it can.

## Membership: block until drained

A close is treated as a restart, not a departure: the member stays in the domain so it can return without
epoch churn. A member is removed from the domain only by a genuine decommission — `ParsleyCoordination.leave()`
— **never by a timeout**. `leave()` itself honours "only a drained node is excluded": it **quiesce-drains**
first (blocks until every local member's causal buffer has emptied through the ordinary delivery path, so
no held record is stranded), then appends the `Leave`, then requests a new epoch over the remaining members
in which it is no longer a member — returning without waiting for that epoch to commit, so a decommission
is never coupled to the other members' liveness. The caller must have stopped feeding the node new input
before decommissioning; `leave()` drains the in-flight buffer, not records that arrive after.

A member that is simply gone — crashed, or briefly absent during a restart — is **waited for**. A
transition's floor is the `mergeMin` of every running member's published frontier, and the round cannot
commit until every running member has published. There is no eviction timeout: a member may be excluded
only once it is *drained*, and a crashed member is never known to be drained from outside, so the
transition **blocks**, for an unbounded time, until the member returns and publishes. This is the
causal-safety choice. Committing a floor without an absent member would strand any records it still holds
below that floor, where their real dependencies are stripped and they would be released before their
causes — the violation the whole design exists to prevent. A member that participates in the round
protects its own un-drained work automatically, because its frontier pulls the `mergeMin` floor down below
its buffered dependencies.

The cost is that a crashed member blocks the next epoch *transition* — and therefore any new join — until
it returns. Ongoing processing in the current epoch is unaffected; only topology evolution waits. Block
until drained, never evict, is the only membership behavior — it is hardcoded, not a pluggable strategy.

Because a round needs every member's publication to commit, publication is driven off the folded log, not
off a one-shot in-band marker: any member that observes a round it has not yet published to publishes its
current completeness. A member that restarts mid-round re-derives from the log that it owes a publication
and re-publishes, so a lost publication cannot deadlock the round.

## Deployment

The coordination is entirely optional and requires no separate process. A domain needs one
single-partition epoch-events log topic, shared by every participating application, named via
`parsley.coordination.epoch-events-topic` in the `Properties` each application passes to its
`CausalStreams`. Without that key a topology runs in epoch 0. Because a Kafka Streams application must
run one topology on every instance, a zero-downtime rolling topology change is not a Streams capability;
a genuine new stage is a redeploy. The epochs machinery is what lets that redeploy re-enter causal time
cleanly rather than replaying obsolete history into the shared frontier.

Coordination also requires every running member's declared inputs and sinks to jointly cover the full
coordinated domain — `ParsleyEpochLog#isFullMeshSatisfied()` is a conjunct of `isRoundComplete()`, so a
round cannot commit while any running member has a gap. `parsley.coordination.domain-topics` plus
auto-wired passthrough sources (see `CausalTopology#assemble`) let a stage cover a domain topic it does
not otherwise consume or produce, without a redundant business subscription — this is what makes a
genuinely cyclic topology coordinate correctly. See
[Evolving a running topology](../streams.md#evolving-a-running-topology) for the user-facing API.
