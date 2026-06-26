# Changelog

All notable changes to this project are documented in this file. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
