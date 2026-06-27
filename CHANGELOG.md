# Changelog

All notable changes to this project are documented in this file. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
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
- Outgoing stamps now carry the full transitive causal ancestry. Previously, a node's output was
  stamped with only its own frontier, discarding the inbound record's dependency coordinates. In a
  multi-hop topology (T1 → Node A → T2 → Node B → T3), a downstream node C subscribing to T1 and
  T3 had no way to enforce the transitively-derived ordering T1@x → T3@z because the T1 coordinate
  was silently dropped by Node B. The stamp is now the node's frontier snapshot merged with the
  inbound record's dependencies, so transitive ancestry flows through intermediate nodes as opaque
  carry-through. Downstream nodes still apply `effectiveDependencies` filtering: only coordinates
  on topics they directly subscribe to gate admission; transitive coordinates on unsubscribed topics
  remain vacuously satisfied.
