# Changelog

All notable changes to this project are documented in this file. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
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
