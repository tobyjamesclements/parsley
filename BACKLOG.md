# Backlog

Findings from an automated correctness review of the causal engine
(`ParsleyClock`/`ParsleyFrontier`/`ParsleyEngine`) and the topology-epoch coordination protocol
(`ParsleyEpochLog`/`ParsleyEpochState`/`ParsleyCoordination`/`ParsleyEpochRuntime`), reviewed with
Fable 5, plus findings from the follow-up verification review of the four fix commits
`e0109eb..5dc1d04`. Ranked most-severe first. GitHub Issues are disabled on this repo, so these are
tracked here instead.

## [LOW] The handoff grace cache is in-memory only, losing a departing topic's grace cycle across a crash

**Where:** `pollEpochCoordination`'s `lastAdoptedExternalSourceTopicIds`/`lastAdoptedEpoch` fields

**Narrowed twice now.** The originally-reported registry-timing repro was fixed by the grace-cache/
pre-seed-removal work (regression test:
`ParsleyProcessorSourceLayerTest#outgoingSelfAdopterStillInjectsTheHandoffEpochsBoundaryAfterATopicLeavesTheLiveRegistry`).
The broader finding that marker relay was *key-routed* at all — so a source-layer task with no business
key yet (notably right after a restart) silently skipped its relay and stalled every downstream lane
until its first post-restart business record — is now fixed too: `ParsleyMarkerPartitioner`/
`ParsleyMarkerPartition` route every marker relay to the forwarding task's own owned partition directly,
never depending on a key at all, so `injectSnapshot`/`adoptAndInjectBoundary` relay unconditionally now
(regression tests: `ParsleyProcessorSourceLayerTest#sourceLayerTaskInjectsTheSnapshotMarkerEvenWithNoBusinessKeyYet`/
`#outgoingSelfAdopterInjectsTheBoundaryEvenWithNoBusinessKeyYet`).

**What's still open.** `lastAdoptedEpoch`/`lastAdoptedExternalSourceTopicIds` reset on restart, so a
crash inside the handoff window (between a topic leaving the live external-source registry and this
task's next adoption cycle) loses the departing topic's grace cycle; if the departed topic was the
task's only external input, the post-restart poll sees empty adoption targets and silently advances
`lastAdoptedEpoch` with no relay ever sent for the handoff epoch. Redundantly covered by the new
producer's own admitting-epoch relay (the pre-seed-removal fix) for the common case, but that
redundancy is itself untested.

**Impact:** conservative-safe (never causally unsafe); a permanent per-epoch floor-advance gap for one
specific coordinate, only when a crash lands inside a narrow handoff window.

**Coverage:** not covered by any existing test.

## [LOW] In-process write ordering cannot fully guarantee tear direction across separate changelog topics

**Where:** the ordering fixes in `ParsleyEngine#propagate`/`drainSatisfied` (frontier before buffer
removal) and `ParsleyFrontier#deliver` (persist before prune), and their Javadoc claims that a crash
"always tears toward" the benign side

The reordering eliminates the deterministic mid-interval tear (before the fix, a crash between the two
calls could only land the dangerous way; now the benign write is enqueued first). But the two writes
go to *different changelog topics*, and producer acks carry no cross-topic ordering: a crash during
the commit-time flush can ack the later-enqueued topic's batch and lose the earlier one's. That torn
state only materialises when local RocksDB state is also lost (task migration / wipe, forcing a pure
changelog restore) — under a plain restart the local store retains both writes in program order — so
the fix narrows the window from "every crash in the window" to "crash during commit flush ∧ unlucky
cross-topic ack interleave ∧ state restored purely from changelogs". Genuinely rare, but the "always"
in the code comments overclaims; the residual is only truly closed by EOS (transactional changelog
writes) or by co-locating the two facts in one store/topic-partition.

**Also a coverage note:** the engine-side ordering (deliver-before-remove) is not pinned by any test —
`ParsleyEngineTest#aCrashBetweenFrontierPersistAndBufferRemoval…` and
`ParsleyFrontierTest#aCrashBetweenPersistAndUnmark…` both pass with the ordering reverted (verified
mechanically); only `ParsleyFrontierTest#deliverPersistsTheNewFrontierValueBeforePruning…` genuinely
fails without its fix. An ordering probe on the engine path (e.g. a buffer store whose `remove`
asserts the frontier already reflects the delivery) would pin it.

**Impact:** documentation accuracy plus a rare residual wedge window; LOW.

## [LOW] Minor observations

- **Stale below-watermark forwarded-index entries are never pruned.** The `5dc1d04` guard stops new
  at-or-below-watermark marks, but entries already leaked below the watermark (e.g. by the
  acknowledged benign tear direction in `deliver`: frontier persisted, unmark lost) linger forever —
  the absorb walk only scans strictly above the watermark. A cheap sweep (delete entries ≤ watermark
  on restore or on epoch promotion) would clean both classes. Purely cosmetic store growth.
- **`injectSnapshot` republishes to the coordination log on every poll tick while `lastSeenKey` is
  null and a round is open** (the retry guard intentionally doesn't advance `lastSnapshotRoundEpoch`,
  but `snapshotPublisher.publish` runs unconditionally inside `injectSnapshot`). Bounded by the round's
  duration and idempotent to fold, but appends one log event per 200ms tick per key-less source-layer
  task for the life of the round.
