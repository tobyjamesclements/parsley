# Changelog

All notable changes to this project are documented in this file. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
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
- `CausalStreams` — the topology-owning high-level causal API (Layer 2), composing
  `CausalProcessors` internally rather than reimplementing the causal engine. Builds a `Topology`
  for a single causal stage from registered `CausalBuffer` sources and named sinks, so it drops
  straight into `new KafkaStreams(topology, props)`. This first cut wires sources, one
  causal-decorated processor, and sinks; `cleanup.policy` assertion, heartbeats, and quiesce follow
  in later increments.
- `parsley.topology.validation` now also covers a `CausalStreams` stage's sink topics, folded into
  the same partition-count parity check as the causal input topics (previously input-only, since
  the decorator alone cannot see its sinks). A sink topic whose partition count cannot be resolved
  (e.g. not yet created) is skipped for this check rather than failing the task, even under
  `strict` — unlike a registered input buffer, a sink is not required to exist before the stage
  starts.
- `CausalStreams.Builder#withPartitioner` — applies one `StreamPartitioner` uniformly to every sink
  a causal stage declares (default: Kafka's own key-hash partitioner), so two causal sinks in the
  same stage can never drift onto different partitioners. Must read only the key — a watermark
  carries a null value and reuses its triggering record's key, so a value-based partitioner cannot
  route it.
- `parsley.topology.validation` — startup validation of the one co-partitioning precondition a
  processor can observe, that its causal input topics share a partition count. `warn` (default) logs a
  mismatch and continues, `strict` fails the task fast, `off` disables the check. Output-side
  conditions such as a watermark-bearing topic's `cleanup.policy` are not checked here, because the
  processor does not know its sink topics.
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
