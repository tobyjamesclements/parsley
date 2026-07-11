# Changelog

All notable changes to this project are documented in this file. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- **The completeness gate required every one of a node's input channels to independently corroborate a
  coordinate before it counted (`ParsleyClock#intersectMin`), which permanently excludes a coordinate
  genuinely present on only one channel** — the documented "no ancestor with its own descendant" fan-in
  restriction existed only to route around this, and a node's private, unshared coordinate could be
  starved forever by an unrelated sibling channel that had no reason to ever mention it.

  `ParsleyFrontier#completeness` now takes the max-merge of a node's own frontier and every channel's
  advertised clock instead of intersecting them — a single genuine witness suffices, matching the
  Birman-Schiper-Stephenson CBCAST delivery condition instantiated directly on Kafka's own
  `(topicId, partition)` coordinates. `ParsleyClock#intersectMin` is deleted (its only caller); no wire
  format change. The channel-clock update that feeds this merge now happens only at the moment of a
  record's own genuine, gated delivery — never pre-loaded from the record's own claimed dependencies —
  so every message, including a node's own broadcast, is checked against this node's actual, already-
  proven state, never against a stamp the record itself supplies. The "no ancestor with its own
  descendant" restriction in `docs/internals/causal-consistency.md` is retired: under max-merge, a node
  consuming both an ancestor and its own descendant is ordinary, safe vector-clock composition.
- **A dependency on a coordinate this node has no input channel for at all (an undeclared topic, or a
  partition a different task instance owns) was silently dropped from the gate check and treated as
  satisfied** — sound only if the coordinate is genuinely, permanently irrelevant, but this node cannot
  actually prove that; it can only prove it has no way to check. Guessing "satisfied" traded an
  unbounded stall for an unproven delivery, which the causal-safety contract does not permit.

  A new `ParsleyClock.CoordinatePredicate` scope on `ParsleyEngine` now fails such a dependency closed
  instead: dead-lettered (`DeadLetter.Reason#UNREACHABLE_DEPENDENCY`) when a dead-letter sink is
  configured, or a hard task failure (`ParsleyUnreachableDependencyException`) otherwise — checked
  independently of whether dead-lettering is enabled, since scope is a static, structural fact, not
  something a missing DLQ should suppress. New `ParsleyMetrics#recordUnreachableDependencyError` and
  `CausalAudit#recordUnreachableDependencyFailure` hooks mirror the existing poison/clock-resolution
  failure signals. This removes the "carry an unconsumed coordinate through the stamp ungated, for a
  downstream node to enforce ordering against later" relay pattern some topologies relied on — that
  record now fails at the first node that cannot verify the coordinate, rather than passing through;
  route the coordinate through a genuine input branch instead (see "Independent inputs" in
  `docs/internals/causal-consistency.md`).

- **Topology-epoch coordination never checked that a multi-app DAG's declared subscriptions actually
  formed a full mesh** — a member could join and run indefinitely with real gaps (a downstream app never
  consuming an upstream app's own input topic), silently relying on the very vacuous-satisfaction
  behavior the fail-closed change above removes. Once removed, such a gap surfaced only as a data-path
  crash loop, discovered per record instead of at startup, or a round that silently hung forever.

  `ParsleyEpochLog` gains `domainTopics()` (∪ every declared member's inputs and sinks),
  `missingSubscriptions(memberId)`, and `isFullMeshSatisfied()` (true iff every running member's own
  subscriptions cover the whole domain) — the last is now a conjunct of `isRoundComplete()`, so an epoch
  can never commit while any running member cannot actually see the full domain. `ParsleyEpochRuntime`
  mirrors `domainTopics()` for cross-thread readers and logs a `WARN`/`INFO` transition when the mesh
  becomes insufficient or recovers. `ParsleyProcessor.init()` adds a startup self-check
  (`validateFullMeshCoverage`), called immediately after `awaitJoinCommit`, that fails fast — mirroring
  the existing `validatePartitionParity` coordination precedent, escalating the default `warn` mode to
  strict — when this member's own declared topics do not cover the known domain. A genuine multi-stage
  pipeline (app A produces a topic app B alone consumes) requires each member to also cover the topics
  only a sibling touches; see the new `parsley.coordination.domain-topics` passthrough wiring below for
  covering such a topic without a direct, redundant business subscription.

- **A received watermark or epoch marker (snapshot/boundary) was always relayed downstream unconditionally,
  even when it taught this node's channel nothing it did not already know** — sound only on an acyclic
  topology, since a marker's own delivery is itself a "genuine advance" by the old logic, so a cycle (a
  full mesh, or any marker-only passthrough channel) would ping-pong the same marker forever, formally
  identified as the one real gap in the max-merge model's termination proof.

  `ParsleyEngine.onWatermark` now takes the marker's own offset and marks that position genuinely
  delivered unconditionally — `seedIfFirstSeen`, then `deliver`, then `propagate`, exactly like a business
  record's own coordinate — so a marker-only channel's frontier still advances even though no business
  record ever flows on it (previously such a channel could never contribute to this node's own
  completeness). It returns a new `WatermarkOutcome` reporting whether the channel's carried clock
  genuinely changed. `ParsleyProcessor`'s three marker handlers (`handleWatermark`, `handleEpochBoundary`,
  `handleEpochSnapshot`, via the shared `advanceChannelClockFromMarker`) now relay downstream only when
  that report is `true` — gated on record kind (a marker's own delivery is never itself a reason to relay
  further), not on whether any business data changed, so a node that has already converged with its peers
  has nothing new to say and simply stops, without needing per-edge "already relayed this" bookkeeping.
  Source-layer marker injection (`injectSnapshot`/`adoptAndInjectBoundary`, driven by the coordination log
  rather than a received marker) is unaffected — it was already correctly gated on genuine epoch/round
  advance by its own counters.

### Added
- **`parsley.coordination.domain-topics`** (comma-separated; only meaningful alongside
  `parsley.coordination.epoch-events-topic` — the reverse is not required, so an existing coordinated
  deployment needs no change) declares the full coordinated domain's topic set — every member's inputs
  and sinks, external sources included. `CausalTopology#assemble` uses it to auto-wire, for each stage, a
  domain topic that stage does not otherwise consume or produce as an extra, raw `byte[]`/`byte[]` source
  feeding that stage's own processor node, so `validateFullMeshCoverage` (see above) can pass without a
  hand-wired, redundant business subscription. `ParsleyProcessor` recognises a passthrough record by its
  own source topic (never a header): it flows through the ordinary completeness gate exactly like any
  other channel, contributing its causal progress to the frontier, but is never handed to the delegate —
  every *other* record a passthrough delivery happens to release from the shared buffer as a side effect
  still reaches the real delegate correctly, since there is only ever one delegate per processor node.
  Verified end to end against a real broker, including a genuine two-application cyclic topology
  (A→B→A, B's visibility into A's root topic supplied entirely by passthrough) delivering correctly —
  the headline capability this whole redesign exists to enable.

### Fixed
- **A node observing its own produced coordinate reflected back to it — directly, by also consuming its
  own sink, or indirectly, via a downstream peer's stamp in a topology cycle — could fail two different
  ways.** A direct self-consumer (the tightest possible cycle) never converged: every watermark it
  received on that channel carried its own ever-advancing self-position, which the ordinary out-of-scope
  logic cannot distinguish from genuine foreign progress, so `channelAdvanced` never settled `false` and
  the marker relayed forever — an infinite loop, found via a `TopologyTestDriver` test during this
  redesign's verification pass, not merely a slow test. Indirectly, a peer's stamp reflecting this node's
  own coordinate back to it (e.g. B, in a two-node A→B→A cycle, stamping a reply with A's own `from-a`
  position) was instead wrongly rejected as an unreachable dependency, even though the coordinate is this
  node's own and therefore never actually unverifiable.

  `ParsleyEngine` gains `ownSinkTopics`, a predicate for coordinates this node itself produces
  (`ParsleyProcessor` resolves sink-topic UUIDs unconditionally at `init()`, never gated by `parsley.
  topology.validation`, since this is a correctness mechanism, not a lint). Both `effectiveDependencies`
  (the business-record gate) and `onWatermark` now strip a node's own sink coordinates from any inbound
  dependency or marker clock before every check. This is sound and deliberately narrower than — not a
  relaxation of — the general out-of-scope fail-closed rule: a claim naming this node's own coordinate can
  only ever have arisen from something this node itself already produced, since nothing else can ever
  advance it, so the claim is either already, trivially known here or could not have legitimately arisen
  at all. `ParsleyClock#retaining`'s Javadoc is updated to document this one narrower exception to its
  "never on an inbound dependency clock" rule.
- **A mismatched sink partition count only warned by default, but under topology-epoch coordination it
  crash-loops the task instead.** `ParsleyMarkerPartitioner` routes an epoch marker to this task's own
  owned partition (`taskId().partition()`) unconditionally; with a sink that has fewer partitions than
  a source, the produce fails outright at runtime, and the task restarts into the same failure instead
  of surfacing a clear, one-time startup error.

  `ParsleyProcessor#validatePartitionParity` now escalates a partition-count mismatch to a hard failure
  whenever topology-epoch coordination is configured, regardless of the configured `parsley.topology.validation`
  mode — `strict` behaves as before, and an explicit `off` still disables the check entirely (a
  deliberate, complete opt-out), but the default `warn` is treated as `strict` under coordination since
  the failure mode a warning would otherwise hide is a crash loop, not a quiet correctness gap.
- **Epoch floors were not actually monotonic across epochs, contrary to `ParsleyEpochState.onBoundary`'s
  documented assumption.** `proposeCommit`'s raw `mergeMin` of published frontiers can drag a shared
  coordinate's floor backwards: a member admitted mid-round that consumes from `earliest` publishes
  completeness far behind the already-committed floor, so the next commit's `mergeMin` on that
  coordinate reflects the newcomer's lag, not genuine progress. Every consumer of the floor tolerates a
  regression except `ParsleyFrontier#pruneStaleOrphans` — an orphan entry pruned under a floor that
  later regresses below it holds that coordinate's dependents forever again.

  `ParsleyEpochLog` now tracks the last committed `lowerBounds` and `proposeCommit` clamps the proposed
  floor to it via `ParsleyClock#merge` (the per-coordinate maximum) before returning the commit — a
  floor can only ever hold or advance, never regress, regardless of what a newly-promoted member's
  frontier reports. Every node computes the identical clamp (a pure function of the same ordered log),
  so the leaderless collect-then-commit protocol's "every node agrees" property is unaffected.
- **`ParsleyEpochRuntime`'s committed epoch id and lower bounds were two independent volatile fields, so
  a caller reading both back-to-back could observe a torn pairing.** `pollEpochCoordination` reads the id
  and then the bounds as two separate volatile reads while `runOnce` writes id-then-bounds; a commit
  landing in between yields a boundary stamped with a fresher id than the bounds it carries. The relayed
  boundary is then never re-adopted (the per-epoch guard only advances), so every consumer downstream is
  merely conservative until the next commit — not unsafe, but avoidable entirely.

  The two fields are now one `CommittedEpoch(epochId, lowerBounds)` record behind a single volatile
  reference, read together via the new `committedEpoch()` accessor wherever both are needed as a pair
  (`ParsleyProcessor`'s epoch-state initialisation and `pollEpochCoordination`'s boundary relay). The
  existing `committedEpochId()`/`committedLowerBounds()` accessors remain for callers that only need one.
- **Two classes of stale index entry were never pruned: below-watermark forwarded-index entries, and
  candidate-index entries `orphan()` discovers pointing at an already-removed record.** Neither is a
  correctness bug — both are purely cosmetic, unbounded store growth — but both are permanent once hit,
  since neither entry's coordinate can ever revisit and clean itself up naturally (a below-watermark
  offset can never be re-absorbed; an orphaned coordinate never advances again).

  `ParsleyForwardedIndex` gained `pruneAtOrBelow`, called once per restored coordinate when a durable
  `ParsleyFrontier` loads, sweeping any entry left below that coordinate's watermark (e.g. by the
  benign tear direction `deliver`'s Javadoc describes — now closed off by the `exactly_once_v2`
  requirement, but still possible in a store carried over from before that requirement existed).
  `ParsleyEngine.orphan`'s `letter == null` branch (a stale candidate whose record another step already
  removed this pass) now calls `candidateIndex.prune` before continuing, instead of leaving the entry
  indexed forever — mirroring `propagate()`'s existing stale-entry pruning, which an orphaned coordinate
  can never trigger on its own since it never advances again.
- **`orphan()`'s scan gating was floor-blind: a coordinate discovered twice in one cascade, via two
  different parent branches, at two different floors, skipped the second scan entirely.** The set
  tracking "coordinates already scanned this pass" keyed on `(topicId, partition)` alone, so once a
  coordinate was scanned at some floor, a later worklist task for the same coordinate at a *lower*
  floor was dropped without ever scanning `[lowerFloor, alreadyScannedFloor)` — a genuine dependent
  requiring an offset in that range was never dead-lettered in this pass, staying buffered until (at
  best) the next full drain, an unbounded liveness delay on an idle topology.

  `ParsleyEngine.orphan` now tracks `Map<Coord, Long> lowestScannedFloor` instead of a set, and rescans
  whenever a newly-popped task's floor is strictly lower than what's already recorded for that
  coordinate, recording the new minimum. `findCandidatesRequiringAtLeast` at a lower floor is a strict
  superset of any narrower scan already done, so already-handled records are skipped via the existing
  `seenRecords` guard; nothing is scanned twice for no reason beyond the genuinely-new range.
- **`tryAdvanceEpoch` gated on the DAG-wide committed floor, so an epoch transition could never settle
  at a non-terminal node.** The committed floor `F_e` is the `mergeMin` of every member's published
  completeness, so it names coordinates for every topic in the DAG — including topics downstream of (or
  parallel to) a given node. A node's own `completeness()` can never contain a coordinate it has no
  input channel for, so comparing it against the unfiltered floor made the dominance check permanently
  false everywhere except the terminal stage: the epoch feature's per-coordinate floor advance,
  below-floor dependency stripping, and orphan pruning on promotion were silently inert at every other
  node, and a fresh joiner (which settles directly at the full committed floor) could reach a floor an
  established member never could.

  `ParsleyFrontier.tryAdvanceEpoch` now filters the pending floor to this node's own channel
  coordinates (`ParsleyClock.retaining`, the same scoping `pruneToScope` already applies) before the
  dominance check — a coordinate this node can never observe no longer blocks the transition. A
  coordinate that *is* in scope but not yet advertised is deliberately left in the filtered floor, so
  `dominates` still holds the window for it (conservative: an absent coordinate is never dominated) —
  the transition never closes early against a channel that genuinely hasn't caught up.
- **`ParsleyOrphanIndex.markOrphaned` kept the highest floor ever recorded for a coordinate, but the
  lowest dead-lettered offset is the true, permanent floor — a coordinate's contiguous frontier freezes
  at the earliest offset ever proven unreachable, regardless of what is proven unreachable afterward.**
  Keeping the maximum resolved cross-offset dead-letters in the wrong direction both ways: a later
  dead-letter at a higher offset silently weakened an already-established floor (overwriting it upward);
  a cascade discovering a lower, truer floor for an already-orphaned coordinate was silently dropped as a
  no-op. Either way, a dependent requiring an offset between the true floor and the wrongly-recorded one
  could pass `isProvenImpossible`'s check and be buffered instead of dead-lettered — and since the
  coordinate's frontier is frozen below the true floor forever, that dependent was never released, never
  proven impossible, and never cascaded: a permanent silent wedge, exactly the failure mode
  dead-lettering exists to prevent.

  `RocksOrphanIndex` and `MockOrphanIndex` now keep the minimum floor: `markOrphaned` is a no-op only
  when an existing floor is already at or below the new one, and always establishes on a coordinate's
  first call regardless of the new floor's value (the `-1` "not orphaned" sentinel is now an explicit
  absence check, not folded into the numeric comparison). `isProvenImpossible` and `pruneStaleOrphans`
  were already written correctly for minimum semantics and needed no change.
- **The handoff grace cache was in-memory only, so a crash inside the handoff window could lose a
  departing topic's grace cycle permanently.** `pollEpochCoordination` gave a topic that just stopped
  being an external source (some member declared it as their sink) exactly one more adoption cycle from
  its outgoing self-adopter, tracked via a per-task in-memory field (`lastAdoptedExternalSourceTopicIds`)
  that reset on restart. A crash between the topic leaving the live registry and this task's next
  adoption cycle lost that memory: the post-restart poll saw empty adoption targets and silently advanced
  past the handoff epoch with no relay ever sent for it — a permanent per-epoch floor-advance gap for
  that one coordinate.

  `ParsleyEpochLog` now retains a two-slot shift register of `externalSourceTopics()` snapshots, updated
  on every `EpochCommitted` — `externalSourceTopicsAsOfPreviousCommit()` is always "the registry as of
  one commit ago," derived purely from replaying the log, which every node (including one that just
  restarted, with no other memory) reconstructs identically. `pollEpochCoordination`'s adoption targets
  are now `live ∪ externalSourceTopicsAsOfPreviousCommit()` instead of `live ∪ lastAdopted`, so the grace
  window survives a crash — the per-task in-memory field is gone.
- **Epoch marker relay depended on a business key that might not exist yet, silently stalling every
  downstream lane for a genuinely idle source-layer task.** `forwardEpochSnapshot`/`forwardEpochBoundary`
  routed by reusing `lastSeenKey` — the most recent business record's key — through whatever partitioner
  the sink used, because that was the only way to steer a record onto a specific partition. A source-layer
  task that had not yet processed a business record (notably right after a restart, whose in-memory
  `lastSeenKey` is wiped) had nothing to route on, so `injectSnapshot`/`adoptAndInjectBoundary` silently
  skipped the relay and retried on a later poll — but if that task's restored completeness already
  dominated a floor committed while it was down, its very first poll could self-adopt and promote in the
  same tick, settling with zero downstream relays ever sent. Every downstream lane then stalled until the
  task's first post-restart business record finally triggered a retry — unbounded, since the input topic's
  idle time bounds it.

  `ParsleyMarkerPartitioner` (installed by `CausalTopology` on every sink a stage declares, wrapping
  whatever partitioner — custom or default — the stage already used) now routes a marker to the
  forwarding task's own owned partition directly, via a new `ParsleyMarkerPartition` thread-local
  `ParsleyProcessor#forwardToSinks` sets immediately before and clears immediately after every marker
  forward. A business forward is unaffected — the override is only ever set around a marker's own
  `context.forward` call. Since routing no longer depends on a key at all, the `lastSeenKey != null`
  relay-skip-and-retry gates in `injectSnapshot`/`adoptAndInjectBoundary`/`pollEpochCoordination` are gone;
  every relay now goes out unconditionally, on the very first poll.
- **Dead-letter paths removed a victim from the buffer before durably recording its orphan floor, the
  same torn-write shape the delivery paths were fixed for.** `ParsleyEngine#fetchForDeadLetter` (used by
  `drainSatisfied`'s proven-impossible branch and `orphan`'s own cascade loop) and the poison/
  proven-impossible branches of `propagate`/`drainSatisfied` all called `buffer.remove` before
  `deadLetterRoot` (and therefore before `ParsleyOrphanIndex#markOrphaned`) ever ran for that record's own
  coordinate. A crash between the two writes left the record gone from the buffer with no orphan floor
  recorded — a buffered dependent on that exact coordinate was then held forever, never proven impossible,
  the same permanent-wedge shape as the dead-letter-at-ingest bug above, and the record could be lost
  without ever reaching the dead-letter topic. Every dead-letter path now marks a victim's own coordinate
  orphaned *before* removing it from the buffer, mirroring the earlier frontier-persistence-ordering fix,
  so a crash always tears toward "orphan floor recorded, victim still buffered" — resolved as a harmless
  duplicate dead-letter by the next `drainSatisfied`/restore pass. `orphan`'s worklist loop now tracks
  which coordinates it has scanned for dependents in a set local to the call, independent of
  `markOrphaned`'s return value — needed because that return value can no longer double as "first time
  seeing this coordinate" once a victim's own coordinate may already be pre-marked by the time its
  worklist task is popped.
- **An `UNRESOLVABLE_CLOCK` record dead-lettered at ingest never orphaned its coordinate, deterministically
  stranding dependents.** `ParsleyProcessor#onUnresolvableClock` dead-lettered a record whose
  causal-dependencies header could not be decoded entirely inside the processor — `engine.onRecord` was
  never called for it — so unlike the `POISON`/`ORPHAN_CASCADE` paths (which go through `deadLetterRoot`
  → `orphan` → `markOrphaned`), nothing durably recorded that the coordinate could never advance past
  that offset: the offset was never delivered, the contiguous frontier froze below it forever, and any
  buffered record depending on that exact offset or later was held indefinitely, never proven impossible
  and never cascaded. No crash needed — an intra-topic dependency was enough (a producer stamps `U@k+1`
  with a dependency on `U@k`; `U@k`'s header is undecodable; `U@k+1` buffers forever). `ParsleyEngine`
  gains `deadLetterAtIngest(topicId, partition, offset)`, the ingest-time counterpart to `deadLetterRoot`
  for a record dead-lettered before it ever became engine state: it runs the same `orphan` cascade —
  marking the coordinate permanently unable to advance past that offset and dead-lettering any
  already-buffered dependent — without re-recording the root record itself (the processor already does).
- **A replayed already-delivered offset leaked a permanent, purely cosmetic entry in the forwarded
  index.** `ParsleyFrontier#deliver` unconditionally marked the delivered offset before walking the
  contiguous absorb run, but that walk only ever scans strictly above the current watermark — so an
  at-least-once replay of an already-delivered offset (`deliver(C, k)` with `frontier(C) >= k`, e.g. a
  duplicate redelivery) marked an entry below the watermark that could never be found and unmarked
  again, growing the changelog-backed forwarded-index store unbounded. `deliver` now returns immediately
  for an at-or-below-watermark offset, before ever marking it.
- **A new sink join could permanently strand a topic's epoch-boundary marker for one transition.**
  `externalSourceTopics()` is a live, memoryless view of the coordination log's current declarations, so
  the instant a new member declares an until-now-external topic as its sink, that topic drops out of the
  DAG-wide external-source registry immediately — one full round before the declaring member is even
  running, let alone able to relay anything in-band (it structurally cannot relay the very epoch whose
  round admits it). The outgoing self-adopter used to stop adopting for that topic in the same poll it
  left the live registry, so nobody ever injected that one epoch's boundary onto it — a conservative-safe
  but real, sometimes-permanent floor stall for anything downstream. `ParsleyProcessor` now injects a
  newly-committed epoch's boundary onto the union of the live registry and the registry as of its own
  last adoption, giving a departing topic one more adoption cycle from its outgoing self-adopter. Also
  fixed two related gaps in the same mechanism: a fresh joiner's `lastAdoptedEpoch` was pre-seeded to the
  epoch that admitted it, silently skipping that joiner's own chance to relay the admitting epoch
  downstream on its own external-source inputs; and the per-epoch/per-round adoption guards used to
  advance even when the relay itself was skipped for lack of a routing key, permanently forfeiting a
  task's one chance instead of retrying once it had actually forwarded something.

  Still open: marker relay routes on a business key (`record.key()`/the last-seen key), not an explicit
  per-partition broadcast, so a topic with a genuinely idle partition can still starve that partition's
  marker — a separate, deeper design question tracked in BACKLOG.md.
- **`onRecord` could skip the drain a proven-impossible record's own channel advance had just enabled.**
  A record's admission always updates its channel's clock first, before its own disposition (deliver,
  buffer, or dead-letter) is decided — this is what lets two sibling records depending on a shared
  ancestor unblock each other without deadlocking. But when the record turned out to be proven
  impossible (dead-lettered rather than forwarded), `onRecord` returned immediately after recording the
  dead letter, skipping the channel-advance drain and epoch-transition check at the end of the method —
  so another buffered record the channel advance had just made deliverable stayed held with no further
  trigger to re-check it, potentially forever (a fan-in node with no subsequent watermark on that
  channel). `onRecord` no longer returns early from the proven-impossible branch; every disposition now
  falls through to the same tail drain.
- **A torn changelog flush under at-least-once could permanently strand a coordinate's frontier.** The
  buffer store, frontier store, and forwarded-index store are three separate changelog topics with no
  cross-store atomicity, so a crash mid-release could tear two different writes apart, in two distinct
  places: (1) `drainSatisfied`/`propagate` removed a record from the buffer before persisting the
  frontier's delivery of it, so a crash in between left the record gone from the buffer (unrecoverable
  on restart) but the frontier still showing it undelivered, permanently freezing that coordinate; (2)
  `ParsleyFrontier.deliver`'s `mergeForward` pruned absorbed forwarded-index entries before the frontier
  advance that accounted for them was persisted, so a crash in that narrower window durably lost the
  forwarded-index entries backing an advance nothing else remembered — the same permanent-wedge shape,
  entirely internal to `deliver()`, affecting even records delivered immediately (never buffered). Both
  release paths now persist the frontier/forwarded-index advance *before* removing the buffer entry, and
  `deliver()` persists the new frontier value *before* pruning the entries it absorbed — so a crash
  anywhere in either window now always tears toward an already-accepted benign outcome (an
  at-least-once duplicate redelivery, or a harmless stale forwarded-index entry below the frontier)
  instead of an unrecoverable wedge.
- **`ParsleyEpochRuntime.unregisterMember` was never called, so a rebalanced-away member could hang or be
  wrongly evicted by a later `leave()`.** `ParsleyProcessor#close` — called by Kafka Streams whenever a
  task stops running on this instance, including a rebalance that migrates it elsewhere, not just a
  genuine shutdown — never told the shared runtime the member had gone. The departed member stayed in
  `localMembers` forever: its stale (possibly non-drained) `reportDrained` state never updated again, so a
  later graceful `leave()` on this instance could hang unboundedly in its drain phase, or — if driven past
  that — append a `Leave` in its remove phase for a member actually running elsewhere, precisely the
  "excluding an un-drained member" hazard `ParsleyMembershipStrategy`'s safety invariant exists to
  prevent. `close()` now calls `unregisterMember` unconditionally, dropping the member from local
  bookkeeping the moment its task leaves this instance; a re-join on this or another instance re-adds it
  with no log event required.
- **`awaitJoinCommit` could deadlock an instance when a joiner shared a `StreamThread` with a running
  member.** Kafka Streams runs every task on a `StreamThread` — including `init()` and punctuators — on
  that one thread, so a joiner blocked in `awaitJoinCommit` inside `init()` also blocked any running
  member sharing the thread from ever running `pollEpochCoordination()`, the only place its
  `publishFrontier` was invoked. The round the joiner opened could then never complete, wedging the
  instance forever. `ParsleyEpochRuntime` now lets its own background thread — distinct from every
  `StreamThread` — publish a stalled local member's completeness on its behalf, from a live snapshot
  (`ParsleyProcessor#stampFrontier`) registered once the member's engine exists
  (`registerLocalCompleteness`). Always-safe, no timeout: completeness is monotonic and the committed
  floor is already a conservative merge-min, so publishing a possibly-stale snapshot only ever makes the
  floor more conservative, never unsafe.
- **`ParsleyEngine.propagate()` could both forward and dead-letter the same record in one cascade
  pass.** It used to collect deliverable candidates into a batch and release them only after scanning
  every candidate at that level, so a poison candidate found later in the same scan could dead-letter
  (via its orphan cascade) an entry already collected as deliverable but not yet removed from the
  buffer — the release loop then forwarded it anyway, violating `Outcome`'s "never both" contract and
  permanently corrupting the frontier/orphan-index state for its dependents. Each candidate is now
  committed to its final disposition the moment it is decided, never staged for a later batch step.
  `propagate()` also now checks `isProvenImpossible` (as `onRecord`/`drainSatisfied` already did), so a
  candidate whose direct dependency was orphaned by an unrelated cascade is dead-lettered instead of
  forwarded just because the completeness frontier — driven by cross-channel header advertisement, not
  genuine local delivery — happens to dominate it.
- **`CausalStreams#close()` could hang forever if a task's buffer emptied before `requestQuiesce()` was
  ever called.** `ParsleyProcessor#updateQuiesceState` only re-evaluates `isQuiesceRequested() && empty`
  on a buffer-depth-changing event; a task already idle-and-drained at that point recorded
  `drained=false` (quiesce hadn't been requested yet) and never got another chance to report otherwise,
  since nothing further ever changed its buffer depth. Found via the dead-letter IT below, but not
  specific to dead-lettering — any topology whose last held record drains through the ordinary path
  before shutdown could hit it. The existing periodic metrics-refresh punctuator (every 5s) now also
  re-pushes the drained state, closing the gap within one tick.

### Changed
- **Breaking: `CausalTopology#assemble` (and therefore `CausalStreams`) now requires
  `processing.guarantee=exactly_once_v2`, unconditionally.** The write-ordering fixes throughout
  `ParsleyEngine`/`ParsleyFrontier` (frontier-before-buffer-removal, orphan-before-buffer-removal,
  persist-before-prune) narrow an at-least-once torn-write window to a benign tear direction, but two
  separate changelog topics have no cross-store atomicity under at-least-once — a crash during the
  commit-time flush can, rarely, ack one topic's batch and lose the other's, so their "always tears
  toward the benign side" claims overclaimed slightly. Exactly-once-v2 wraps every state-store changelog
  write, every produced record, and the consumer offset commit into one Kafka transaction, so a crash
  genuinely cannot tear one write from the other at all — the same way a transactional producer requires
  `enable.idempotence`/a transactional id rather than treating it as optional hardening. Assembling a
  topology without it now fails fast with `IllegalStateException`, never gated by
  `parsley.topology.validation` (a correctness requirement, not a topology-shape lint).

### Added
- **`mvn test` (and therefore CI) now fails on a unit-test coverage regression.** Jacoco's `check` goal
  gates the overall bundle at 80% instruction / 75% branch coverage — a few points below the current
  86.0%/80.9% baseline, so routine refactors have headroom but a real drop fails the build. Scoped to the
  bundle total rather than per-class: several classes are exercised only by the Testcontainers ITs this
  report excludes (matching the existing mutation-testing exclusion), so a per-class minimum would fail
  the build on files with no real gap.
- **Dead-letter sink: the only liveness escape from causal delivery, fired solely on proven
  impossibility.** `CausalTopology#assemble` gives every stage its own dead-letter sink node (never one
  sink shared across stages — that would union their Kafka Streams node groups), all writing to one
  topic name: `parsley.deadletter.topic` if set, else `{application.id}-deadletter`.
  `CausalStreams#start()` provisions that topic (partitions from the new `parsley.deadletter.partitions`,
  default 1) before starting the underlying `KafkaStreams`, tolerating a concurrent creation race.

  A record is dead-lettered only when its dependencies are *proven* unsatisfiable, never on pressure or
  time: a poison record (undecodable on the forward path), an unresolvable causal-dependencies header at
  ingest, or a dependent of either. A dead-lettered coordinate's frontier can never legitimately advance
  again, so it is recorded in a new durable, changelog-backed **orphan index** (`ParsleyOrphanIndex` /
  `RocksOrphanIndex`, mirroring `ParsleyForwardedIndex`); `ParsleyEngine` then worklist-scans this node's
  own buffer for anything depending on that coordinate at or beyond its floor and dead-letters those too,
  recursively — Lamport transitivity in reverse. This is local to one node's own buffer: a *different*
  node still buffering on the same doomed coordinate just sees a channel that stopped advancing,
  indistinguishable from ordinary lag, until a forced epoch-floor advance (a later change) resolves it
  DAG-wide. Without a dead-letter sink configured (the low-level `ParsleyProcessors` builder path, unless
  `.deadLetterSink(...)`/`.sinkNodeNames(...)` are called explicitly), a proven-impossible record still
  fails the task fast, exactly as before this change.

  The dead-letter record carries raw bytes (a poison record's value can never be reconstructed as `V`) and
  new headers: `parsley-deadletter-reason` (`POISON`/`UNRESOLVABLE_CLOCK`/`ORPHAN_CASCADE`),
  `parsley-deadletter-source-topic`/`-source-topic-id`/`-source-partition`/`-source-offset`, and, for an
  unresolvable clock, `parsley-deadletter-original-dependencies` (the undecodable bytes, verbatim, for
  operator forensics). `CausalAudit` gains `recordDeadLetter(topic, partition, offset, reason)`, fired for
  every dead-lettered record including a cascade victim that was never itself a deserialization/clock
  failure. `ParsleyMetrics` gains a `dead-lettered` rate-total sensor.

### Changed
- **Breaking: `CausalTopics` (since renamed `ParsleyTopics`) is no longer public; `CausalDependencies.using`/`builder` gain
  `Properties`/`Map<String, Uuid>` overloads directly.** `CausalTopics.of(Admin)` dated to an earlier
  design where Parsley avoided owning any Kafka client lifecycle at all — the caller constructed and
  closed its own `Admin` and handed it in. That no longer matches the rest of the public API (`CausalStreams`
  already owns its `KafkaStreams` instance, provisions topics, and owns quiesce/coordination internally),
  so the resolver type is now an internal implementation detail of `CausalDependencies` rather than a
  separate public type: `CausalDependencies.using(props)` / `.builder(props)` resolve topic UUIDs
  internally, and `.using(Map<String, Uuid>)` / `.builder(Map<String, Uuid>)` remain the broker-free path
  for tests. A `Properties`-backed resolver holds no live connection between calls — each distinct topic
  name is resolved (and cached) through a fresh, short-lived Kafka admin client opened and closed for that
  one lookup — so there is nothing for a caller to construct or close.
- **Breaking: `CausalAudit.recordDeserializationFailure`/`recordClockResolutionFailure` drop their
  trailing boolean.** `dropped`/`failed` were always hardcoded constants (`false`/`true` respectively)
  carrying no information; the new `recordDeadLetter` (above) is the actual disposition signal now that a
  proven-impossible record has a real second outcome besides failing the task.
- **A processor node's plain, unaddressed forward is no longer always a Kafka Streams broadcast.**
  Attaching a dead-letter sink — registered with `Serdes.ByteArray()` — as a second child of a stage's
  processor node means the zero-arg `context.forward(record)` Kafka Streams itself provides would also
  broadcast a business/control forward to it, throwing `ClassCastException` on the very next record.
  `ParsleyProcessorContext`'s one-arg `forward` and `ParsleyProcessor`'s watermark/epoch-marker forwards
  now address every declared business sink by name instead whenever a stage has one; with no dead-letter
  sink configured (every low-level `ParsleyProcessors` caller that hasn't opted in), the plain Kafka
  Streams broadcast is unchanged.
- **Breaking: concise, topology-level public API — `CausalStreamsBuilder` / `CausalTopology` /
  `CausalStreams`.** The public surface collapses to three roles mirroring Kafka Streams'
  `StreamsBuilder`/`Topology`/`KafkaStreams`. `CausalStreamsBuilder` declares one or more causal stages
  (`stream(topic[s][, keySerde, valueSerde])` — deferring to the runtime's default serdes when omitted —
  `.process(supplier)`, `.to(topic[, keySerde, valueSerde])`, `.withPartitioner`/`.withAudit`); combine
  streams declared with different serdes with `CausalStream#merge`. `.build()` produces a `CausalTopology`
  — a specification, not yet a real Kafka Streams `Topology`. The `CausalStreams` name is repurposed from
  today's topology-owning builder (removed) to the **runtime**: `new CausalStreams(topology, props)` /
  `.start()` / `.close()`, mirroring `new KafkaStreams(topology, props)`. Unlike the Kafka Streams DSL,
  sources/sinks take plain `Serde`s rather than `Consumed`/`Produced` — neither exposes its serdes for
  reading back, and Parsley's causal buffer needs the real `Serde` to round-trip a held record.

  `ParsleyQuiesce` and `ParsleyCoordination` are no longer public, user-constructed handles — `CausalStreams`
  owns one of each internally. Graceful causal drain is now unconditional and automatic: `close()` always
  waits for every task's buffer to drain, then (if `parsley.coordination.epoch-events-topic` is configured)
  permanently decommissions this instance's members before stopping the underlying `KafkaStreams` — so
  there is no restart/leave distinction for a caller to get wrong (a restart now always rejoins as a fresh
  member and waits to be re-admitted; slower, never unsafe). Evolve a running, coordinated topology through
  an epoch boundary with `CausalStreams#requestEpochTransition()`. `application.id` supplies the epoch
  member identity, as before.

  `ParsleyProcessors`, `ParsleyProcessorSupplier`, and `ParsleyBuffer` are demoted to package-private — they
  survive as `CausalStreamsBuilder`'s internal engine wiring. All prior `CausalStreams`/`ParsleyProcessors`
  capability carries over: multiple input topics with per-topic serdes, multiple named sinks, a uniform
  key-only sink partitioner, `CausalAudit`, and the startup co-partition + sink `cleanup.policy` validation
  (`parsley.topology.validation`).
- **Breaking: fail-closed causal delivery; buffer limits, eviction, and failure policies removed.** Causal
  delivery is now strictly fail-closed — there is no configuration that trades causal order for liveness.
  `CausalBufferLimit` (and `ofSize`/`ofDuration`/`first`/`unbounded`) is removed along with all buffer
  eviction: the causal buffer is unbounded and changelog-backed, so a record whose dependencies are not
  yet satisfied waits (spilling to disk) rather than being force-forwarded out of causal order.
  `addBufferStore(name, limit)` becomes `addBufferStore(name)`. The three `parsley.*.failure.policy`
  settings (`parsley.buffer.eviction.failure.policy`, `parsley.buffer.deserialization.failure.policy`,
  `parsley.clock.resolution.failure.policy`) and their `continue` mode are gone; the only remaining
  Parsley setting is `parsley.topology.validation`. An undecodable buffered record, or an undecodable
  dependencies header, now fails the task closed (the record is never dropped or forwarded on an unknown
  premise); an explicit dead-letter path — removing such a record from the causal execution path rather
  than delivering it as causally valid — will replace the fail-fast behaviour in a later change. The
  `CausalAudit` eviction events (`recordViolation`, `recordEvictionLimitExceeded`) and the
  eviction/violation metrics are removed.
- **Breaking (topology epochs): block-until-drained membership; timeout eviction removed.** An epoch
  transition now blocks until every running member has published its snapshot, for an unbounded time,
  instead of evicting a silent member after a timeout. Evicting an absent member and committing a floor
  without it could strand records it still held below that floor and release them before their causes (a
  causal-safety violation); block-until-drained never does. `ParsleyCoordination.create(...)` no longer
  takes an `evictionTimeout` — it takes a `ParsleyMembershipStrategy` (default
  `ParsleyMembershipStrategy.blockUntilDrained()`), a seam for future exclusion/recovery algorithms.
  Publication of a member's frontier is now driven off the folded log rather than a one-shot in-band
  marker, so a member that restarts mid-round re-publishes and cannot deadlock the round. There are **no
  timeouts** in the coordinator: the `joinTimeout` is also gone — a joining task now blocks unbounded until
  its epoch commits rather than failing after a deadline (`create(...)` no longer takes a `joinTimeout`,
  and `DEFAULT_JOIN_TIMEOUT` is removed). Blocking never proceeds on an unknown floor, so it cannot violate
  causal safety. Consequence: a crashed member blocks the next epoch transition — and any new join — until
  it returns; ongoing current-epoch processing is unaffected. Supersedes the earlier timeout-eviction +
  concurrent-redelivery behaviour.
- **Breaking: `ParsleyMembershipStrategy`/`ParsleyBlockedRound` are no longer public.** The seam for a future
  exclusion/recovery algorithm still exists internally, but with a single implementation
  (`blockUntilDrained()`) and no external caller ever supplying one, keeping it public only advertised an
  extension point nothing used. `CausalStreams`'s public constructor is now just `(topology, props)`; the
  3-arg overload taking an explicit `ParsleyMembershipStrategy` is removed.
- **`ParsleyCoordination.leave()` now drains before departing.** A graceful decommission quiesce-drains the
  node (blocks until its causal buffer empties through the ordinary delivery path), then appends the
  `Leave`, then requests a new epoch over the remaining members in which it is no longer a member — so a
  leave never strands un-drained buffered records ("only a drained node is excluded"). It returns without
  waiting for that epoch to commit, so a decommission is not coupled to the other members' liveness.
  Contract: stop feeding the node new input before decommissioning.
- **Breaking (semantics): strict completeness gate across all input channels.** A record is now
  delivered only once *every* input channel of the processor has confirmed *every* coordinate the
  record depends on. The delivery gate is a single check, `completeness().dominates(deps)`, where
  `completeness()` is the per-coordinate minimum across all input channels (each channel's advertised
  dependencies plus its own contiguous delivered position). The previous model scoped a dependency
  out when the processor did not consume that coordinate (treating it as vacuously satisfied); that
  scoping is removed, because it was unsound at a reconvergence point and let a lagging or recovering
  branch introduce an earlier-ordered record after the fact.

  This imposes a **topology contract**: every input branch of a node must observe (consume and
  watermark) every coordinate any branch's records depend on, or records depending on an unconfirmed
  coordinate are held indefinitely. In particular, a join of fully independent sources will hold a
  record depending on a coordinate an unrelated input never observes, and **a node must not consume
  both a topic and a topic derived from it** (the ancestor channel can never confirm the descendant).
  See `docs/internals/causal-consistency.md`.
- **Breaking (wire).** Forwarded records carry the producing node's completeness frontier in the
  `parsley-causal-dependencies` header. Nodes emit *protocol watermark* records — a null value, keyed
  with the triggering input record's key, marked with a `_parsley_watermark` header carrying that
  frontier — for every consumed message that produces no business output (a dropped/buffered record
  that advances completeness, or a delivered record the delegate did not forward), so completeness
  propagates contiguously through layers that produce no business output. The watermark reuses the
  triggering record's key so it routes to the same partition that record's output would, keeping
  completeness propagation correct across a sink boundary; it is identified only by the header, never
  by its key. A non-Parsley consumer of such a topic sees tombstone-shaped records and must skip them. The per-input-channel clocks that back `completeness()` are stored alongside the
  contiguous frontier clock in the single `"f"` value of the existing `{ns}-frontier` store, so no
  additional changelog topic is introduced.
- Documentation reframed to describe Parsley's guarantee as causal delivery order for Kafka
  Streams processors, given specific conditions (co-partitioned topics, closed processor effects).
  The previous framing ("causal consistency for Kafka") overstated the scope of the guarantee.

### Added
- `ParsleyCoordination` — the public handle that turns on **topology-epoch coordination**, so a causal
  topology can evolve (add/replace a stage, recompile) across a well-defined epoch boundary without a
  new node dragging obsolete pre-epoch history into causal time. Create one over a shared
  single-partition epoch-events log topic and register it with every participating stage via
  `CausalStreams.Builder#withCoordination` / `ParsleyProcessors.Builder#withCoordination` (mirroring
  `withQuiesce`); call `requestEpochTransition()` to evolve the running topology through a boundary,
  and `close()` in shutdown. The coordination is **leaderless**: every instance folds the totally
  ordered epoch-events log identically (a per-round elected owner computes each epoch's floor as the
  min over running members' completeness), and the floor propagates **in-band** via markers that
  relay edge-by-edge through the DAG, so each node adopts it through the overlapping-epoch transition.
  Entirely **optional** — without a `ParsleyCoordination` a topology runs in epoch 0, exactly as
  before. A node **deployed into an already-running** topology blocks at startup until an epoch
  computed without it commits, then adopts that floor and replays its inputs from the start with
  pre-epoch history stripped, so it never drags the shared floor down; on a configurable timeout it
  fails to retry rather than proceed on an unknown floor. A **gone** member (a decommissioned or
  crashed app) cannot freeze the domain: a round that waits too long for it **evicts** it through the
  log after a configurable timeout, and — since a complete round is committed by any node, not a
  single owner — a gone owner cannot freeze it either. A clean decommission uses
  `ParsleyCoordination.leave()`; a restart keeps the member in the domain and returns. The topology's
  **external source topics** (entry points produced outside the topology, on which no in-band marker
  arrives) are **derived from the log**, not configured: every stage declares its input channels and
  sink topics on join, and a topic some member consumes but no member produces is an external source.
  Declare sink topics via `CausalStreams.addSink(...)` (automatic) or the new
  `ParsleyProcessors.Builder#sinkTopics(...)` on the low-level path.
- `CausalStreams` — the topology-owning high-level causal API (Layer 2), composing
  `ParsleyProcessors` internally rather than reimplementing the causal engine. Builds a `Topology`
  for a single causal stage — one or more `ParsleyBuffer` sources feeding a causal-decorated
  processor, forwarding to one or more named sinks — so it drops straight into
  `new KafkaStreams(topology, props)`. Use it instead of the low-level `ParsleyProcessors` decorator
  whenever a topology needs sink-side guarantees the decorator alone cannot provide: a uniform sink
  partitioner, co-partitioning validation across sinks (not just inputs), and a `cleanup.policy`
  check (below). Path integrity — no non-Parsley processor spliced between causal nodes — holds by
  construction: the builder exposes no way to add one.
  - `CausalStreams.Builder#withPartitioner` applies one `StreamPartitioner` uniformly to every sink
    a stage declares (default: Kafka's own key-hash partitioner), so causal sinks in the same stage
    can never drift onto different partitioners. Must read only the key — a watermark carries a
    null value and reuses its triggering record's key, so a value-based partitioner cannot route it.
  - A delivered record the delegate forwards to only one named sink still has its stand-in
    watermark (emitted when the delegate forwards nothing for a given input) reach every sink
    connected to the processor node — Kafka Streams' own broadcast behaviour for an unqualified
    `context.forward`, now exercised through a real multi-sink topology.
  - `ParsleyProcessors.builder(...)` rejects a `userSupplier` that is already a
    `ParsleyProcessorSupplier` with an `IllegalArgumentException`, instead of silently building a
    nested double-decoration that would buffer and stamp every record twice and corrupt the
    frontier. The guard lives at this single entry point, so `CausalStreams` (which calls it
    internally) is protected with no separate check.
- `parsley.topology.validation` — startup validation of topology misconfigurations a causal
  processor can detect: its causal input topics not sharing a partition count, which makes
  co-partitioning impossible, and, when built through `CausalStreams`, that stage's sink topics too
  — both their partition counts (folded into the same parity check) and their `cleanup.policy`
  (checked for `compact`, since a protocol watermark is a null-value record wire-indistinguishable
  from a compaction tombstone and can be compacted away before a slow consumer reads it). `warn`
  (default) logs a mismatch and continues, `strict` fails the task fast, `off`
  disables the checks entirely (no admin round-trip). A bare `ParsleyProcessors` decorator only ever
  sees its own input topics, so the sink-side checks apply only through `CausalStreams`. Each sink
  is resolved independently, so one sink that cannot be described (e.g. not yet created) never
  masks a genuine misconfiguration on a different sink in the same stage, even under `strict`.
  `ParsleyTopicAdmin` gained a `cleanupPolicies` method to support this.
- `ParsleyQuiesce` — a shared handle for coordinating graceful shutdown across every causal task in
  one application instance. Register it with `ParsleyProcessors.Builder#withQuiesce` /
  `CausalStreams.Builder#withQuiesce`; call `requestQuiesce()` from your own shutdown path and poll
  `isSafeToClose()` before calling `KafkaStreams#close`. A registered task keeps processing exactly
  as it does today — it only reports itself drained once its buffer empties through the ordinary
  delivery path (a held record's dependency becoming satisfied by a later message), never by
  fabricating completeness. This is a stall-avoidance optimization, not a correctness requirement:
  every held record is already changelog-backed and survives an ungraceful stop regardless.
- `CausalDependencies.isWatermark(ConsumerRecord)` — identifies a protocol watermark so a plain
  Kafka client consuming a Parsley-produced topic can fold its carried completeness frontier with
  `observe` while skipping it as a business record. `observe` now folds a watermark's carried
  frontier only and never its own position, matching engine-side handling so client and engine
  frontiers stay consistent.
- `CausalBufferLimit.unbounded()` — a new limit that never evicts. Records are held until their
  causal dependencies are satisfied regardless of depth or wait time. Intended for deployments
  where uncoordinated producers make bounded limits impractical and causal ordering must never be
  violated. Callers must monitor buffer depth; if a dependency can never be satisfied (e.g. the
  producing topic was deleted), records accumulate without bound on the RocksDB state store and
  the Kafka changelog.
- `CausalBoundedBufferLimit` — a new public sealed interface that refines `CausalBufferLimit` and
  is implemented by all evicting limit types (`ofSize`, `ofDuration`, `first`). The `first()`
  factory now accepts `CausalBoundedBufferLimit` arguments rather than `CausalBufferLimit`,
  making `first(unbounded())` a compile error.

### Changed
- The "holding" debug log line now identifies the held record by topic UUID rather than topic name,
  and logs the actual frontier value (restricted to the record's dependency coordinates) rather than
  a per-record shortfall. The frontier is monotonically non-decreasing for a given coordinate, so
  it can be tracked across log lines without confusion. `deps` and `frontier` use the same UUID
  format and coordinate set so they can be read in parallel. Example:
  `Holding UUID-0 @2 (buffer depth: 1, deps: ParsleyClock{UUID-0@8}, frontier: ParsleyClock{UUID-0@7})`

### Tests
- Added two `ParsleyEngineTest` cases verifying that a contiguous-frontier jump releases every
  buffered record whose dependency falls anywhere in the jumped range — not just records waiting on
  the final boundary offset. One test covers five records each waiting on a distinct intermediate
  offset (8–12) within a 4→12 jump; the other is a minimal reproduction with a single record
  waiting on offset 10.

### Fixed
- When a Kafka topic is dropped and recreated its UUID changes, causing the old UUID to leave the
  processor's `consumedTopicIds`. Buffered records whose only dependencies named the old UUID had
  empty effective dependencies after restart and were skipped by `drainRestoredSatisfied()`. Since
  no new records arrive on the dropped topic the drain path was never retriggered, leaving those
  records stuck in the buffer indefinitely. They are now released immediately — empty effective
  dependencies mean all raw deps are out-of-scope and therefore vacuously satisfied.
- The persisted frontier (`ParsleyClock`) accumulated entries for every topic UUID ever observed and
  was never pruned. Stale entries for topics that no longer exist grew the stored clock
  unboundedly. On startup the restored frontier is now filtered to the current `inScope` predicate,
  keeping it compact across restarts.
- After a restart under `at_least_once` processing, buffered records whose in-scope causal
  dependencies were already satisfied by the restored frontier could become permanently stuck and
  eventually evicted as spurious causal violations with an empty gap (`gap: ParsleyClock{}`). The
  root cause was the engine constructor re-indexing restored buffer entries using raw stored
  dependencies instead of effective dependencies (in-scope filtering and self-reference stripped),
  causing records to be indexed only under out-of-scope dead-end coordinates that the release path
  never visits. The constructor now uses effective dependencies, and a `drainRestoredSatisfied()`
  pass runs at startup to release any record already satisfied by the restored frontier.
- Causal dependencies on coordinates a processor does not consume — a topic outside its registered
  buffers, or a partition its task does not own — are now treated as vacuously satisfied instead of
  holding the record until eviction. A producer stamps a clock spanning every coordinate it
  consumes, so a downstream processor routinely sees dependencies it can never observe; these no
  longer block, evict, or fail the task. A dependency on a coordinate the processor *does* consume
  but has not yet observed still blocks, as before.
- Multi-layer causal ordering is now sound across nodes that fan in or reconverge. A node's output
  was previously stamped with only its own frontier, discarding the inbound record's transitive
  ancestry: in a multi-hop topology (T1 → Node A → T2 → Node B → T3), a downstream node C
  subscribing to T1 and T3 could not enforce T1@x → T3@z because Node B silently dropped the T1
  coordinate. Nodes now stamp their completeness frontier — their own contiguous frontier
  max-merged with a per-coordinate minimum across every input channel's last-seen clock — so
  transitive ancestry flows through intermediate nodes, and a fan-in advertises completeness only
  up to its slowest branch rather than over-claiming with a maximum (the interim per-record
  max-merge stamp, never released, was correct only when no two input branches shared an ancestor).
  Downstream nodes still apply `effectiveDependencies` filtering, and the delivery gate gained a
  second part (`ancestorsSettled`) that holds a record until every sibling input branch that knows a
  shared out-of-scope ancestor has caught up to it — closing a reconvergence race where two branches
  sharing an ancestor could deliver out of order.
- Fixed a mutual deadlock between two sibling records that each depend on a shared ancestor and each
  arrive before the other is delivered. The engine now records a record's carried frontier on its
  source channel at receipt time, before gating, so a shared ancestor is confirmed by a sibling
  branch's business record (not only by a watermark) without either record waiting on the other.
