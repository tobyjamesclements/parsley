# Backlog

Findings from an automated correctness review of the causal engine
(`ParsleyClock`/`ParsleyFrontier`/`ParsleyEngine`) and the topology-epoch coordination protocol
(`ParsleyEpochLog`/`ParsleyEpochState`/`ParsleyCoordination`/`ParsleyEpochRuntime`), reviewed with
Fable 5, plus findings from the follow-up verification review of the four fix commits
`e0109eb..5dc1d04`. Ranked most-severe first. GitHub Issues are disabled on this repo, so these are
tracked here instead.

## [LOW-MEDIUM] Marker relay is key-routed and key-gated, so epoch markers can be withheld in more cases than the idle-partition one

**Where:** `forwardEpochBoundary`/`forwardEpochSnapshot` (via `injectSnapshot`/`adoptAndInjectBoundary`
and `handleEpochBoundary`/`handleEpochSnapshot`'s downstream relay), all of which route the outbound
marker on `lastSeenKey`/`record.key()` — a single business key, not an explicit per-partition broadcast

**Narrowed from the original finding, then re-broadened by the verification review.** The
originally-reported registry-timing repro is fixed (`lastAdoptedExternalSourceTopicIds` grace cache,
fresh-joiner pre-seed removal, relay-skip retry guards; regression test:
`ParsleyProcessorSourceLayerTest#outgoingSelfAdopterStillInjectsTheHandoffEpochsBoundaryAfterATopicLeavesTheLiveRegistry`).
The verification review confirmed the grace-cache timing argument, including the coalescing case
(two commits between polls): `ParsleyEpochState#onBoundary`'s supersede semantics (a higher-epoch
marker replaces a pending lower one) plus monotone floors make a skipped intermediate epoch safe at
every hop, and the cache stays in the adoption targets until the first *successful* adoption cycle.

**What's still open — broader than "an idle partition among several".** The underlying problem is
that a marker relay needs a business key to route on, and local settling does not wait for it:

- *A promoted transition does not imply its markers were relayed.* That implication holds only for
  markers received in-band (`handleEpochBoundary` relays unconditionally on receipt, and the relay is
  producer-flushed before the marker's source offset commits, so a crash replays and re-relays). A
  source-layer task's own channels get their markers **self-injected** by `adoptAndInjectBoundary`,
  which records them on every channel (and can promote, via `onEpochBoundary` → `tryAdvanceEpoch`, in
  the same poll) while its single downstream relay is skipped whenever `lastSeenKey` is null. Concrete
  repro: a source-layer task restarts (`lastSeenKey` is in-memory and wiped) with its restored
  completeness already dominating a floor committed while it was down; the first 200ms punctuator poll
  self-adopts, records markers on all channels, and promotes — settled, with zero downstream relays.
  Downstream transitions on *every* lane (single-partition case included) then stall until this task's
  first post-restart business record finally triggers the retried relay. Conservative-safe, but the
  stall length is the input topic's idle time, unbounded.
- *The handoff grace cache is in-memory only.* `lastAdoptedEpoch`/`lastAdoptedExternalSourceTopicIds`
  reset on restart, so a crash inside the handoff window loses the departing topic's grace cycle; if
  the departed topic was the task's only external input, the post-restart poll sees empty adoption
  targets and silently advances `lastAdoptedEpoch` with no relay ever sent for the handoff epoch.
  Redundantly covered by the new producer's own admitting-epoch relay (the pre-seed-removal fix), but
  that path is itself key-gated and currently untested.
- A note on the reverted "settle re-broadcast" fix: the revert's *reasoning* ("promote implies every
  marker was already relayed at receipt") is falsified by the self-adoption scenario above, but the
  reverted fix would not have closed it either — a key-routed re-broadcast still has no key at settle
  time. The real fix is the same one this item has always pointed at: explicit broadcast to every
  owned output partition (or a persisted relay obligation with key-independent routing), not another
  client-side cache.

**Impact:** conservative-safe (never causally unsafe) but a real, sometimes-long floor-advance stall
for downstream tasks; permanent per-epoch for a genuinely idle lane.

**Coverage:** the grace cache is pinned by the regression test above. The fresh-joiner pre-seed
removal and the relay-skip retry guards are **not pinned by any test** (verified by mutation: re-adding
the pre-seed and reverting both guards passes the full suite). The settle-without-relay scenarios are
not covered by any test.

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
