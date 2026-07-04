# Changelog

All notable changes to this project are documented in this file. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
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
  causal-safety violation); block-until-drained never does. `CausalCoordination.create(...)` no longer
  takes an `evictionTimeout` — it takes a `CausalMembershipStrategy` (default
  `CausalMembershipStrategy.blockUntilDrained()`), a seam for future exclusion/recovery algorithms.
  Publication of a member's frontier is now driven off the folded log rather than a one-shot in-band
  marker, so a member that restarts mid-round re-publishes and cannot deadlock the round. There are **no
  timeouts** in the coordinator: the `joinTimeout` is also gone — a joining task now blocks unbounded until
  its epoch commits rather than failing after a deadline (`create(...)` no longer takes a `joinTimeout`,
  and `DEFAULT_JOIN_TIMEOUT` is removed). Blocking never proceeds on an unknown floor, so it cannot violate
  causal safety. Consequence: a crashed member blocks the next epoch transition — and any new join — until
  it returns; ongoing current-epoch processing is unaffected. Supersedes the earlier timeout-eviction +
  concurrent-redelivery behaviour.
- **`CausalCoordination.leave()` now drains before departing.** A graceful decommission quiesce-drains the
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
- `CausalCoordination` — the public handle that turns on **topology-epoch coordination**, so a causal
  topology can evolve (add/replace a stage, recompile) across a well-defined epoch boundary without a
  new node dragging obsolete pre-epoch history into causal time. Create one over a shared
  single-partition epoch-events log topic and register it with every participating stage via
  `CausalStreams.Builder#withCoordination` / `CausalProcessors.Builder#withCoordination` (mirroring
  `withQuiesce`); call `requestEpochTransition()` to evolve the running topology through a boundary,
  and `close()` in shutdown. The coordination is **leaderless**: every instance folds the totally
  ordered epoch-events log identically (a per-round elected owner computes each epoch's floor as the
  min over running members' completeness), and the floor propagates **in-band** via markers that
  relay edge-by-edge through the DAG, so each node adopts it through the overlapping-epoch transition.
  Entirely **optional** — without a `CausalCoordination` a topology runs in epoch 0, exactly as
  before. A node **deployed into an already-running** topology blocks at startup until an epoch
  computed without it commits, then adopts that floor and replays its inputs from the start with
  pre-epoch history stripped, so it never drags the shared floor down; on a configurable timeout it
  fails to retry rather than proceed on an unknown floor. A **gone** member (a decommissioned or
  crashed app) cannot freeze the domain: a round that waits too long for it **evicts** it through the
  log after a configurable timeout, and — since a complete round is committed by any node, not a
  single owner — a gone owner cannot freeze it either. A clean decommission uses
  `CausalCoordination.leave()`; a restart keeps the member in the domain and returns. The topology's
  **external source topics** (entry points produced outside the topology, on which no in-band marker
  arrives) are **derived from the log**, not configured: every stage declares its input channels and
  sink topics on join, and a topic some member consumes but no member produces is an external source.
  Declare sink topics via `CausalStreams.addSink(...)` (automatic) or the new
  `CausalProcessors.Builder#sinkTopics(...)` on the low-level path.
- `CausalStreams` — the topology-owning high-level causal API (Layer 2), composing
  `CausalProcessors` internally rather than reimplementing the causal engine. Builds a `Topology`
  for a single causal stage — one or more `CausalBuffer` sources feeding a causal-decorated
  processor, forwarding to one or more named sinks — so it drops straight into
  `new KafkaStreams(topology, props)`. Use it instead of the low-level `CausalProcessors` decorator
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
  - `CausalProcessors.builder(...)` rejects a `userSupplier` that is already a
    `CausalProcessorSupplier` with an `IllegalArgumentException`, instead of silently building a
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
  disables the checks entirely (no admin round-trip). A bare `CausalProcessors` decorator only ever
  sees its own input topics, so the sink-side checks apply only through `CausalStreams`. Each sink
  is resolved independently, so one sink that cannot be described (e.g. not yet created) never
  masks a genuine misconfiguration on a different sink in the same stage, even under `strict`.
  `ParsleyTopicAdmin` gained a `cleanupPolicies` method to support this.
- `CausalQuiesce` — a shared handle for coordinating graceful shutdown across every causal task in
  one application instance. Register it with `CausalProcessors.Builder#withQuiesce` /
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
