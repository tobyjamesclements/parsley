# Backlog

Findings from an automated correctness review of the causal engine
(`ParsleyClock`/`ParsleyFrontier`/`ParsleyEngine`) and the topology-epoch coordination protocol
(`ParsleyEpochLog`/`ParsleyEpochState`/`ParsleyCoordination`/`ParsleyEpochRuntime`), reviewed with
Fable 5. Ranked most-severe first. GitHub Issues are disabled on this repo, so these are tracked here
instead.

## [MEDIUM-LOW] onRecord's proven-impossible early return skips the completeness re-drain it just enabled

**Where:** `ParsleyEngine.java:323-327` (early return on proven-impossible), which sits before the
`channelAdvanced` drain (`ParsleyEngine.java:359-361`) and the epoch-advance drain
(`ParsleyEngine.java:364-366`)

A record's admission always updates its channel's clock first (`channelUpdate` at
`ParsleyEngine.java:312-314`), which can advance completeness. If that same record then turns out to
be proven-impossible, `onRecord` returns early (line 327) before checking whether the channel advance
it just performed made *other* buffered records deliverable.

**Concrete repro sequence:** Fan-in node, channels C and D. R is held with deps {C@5}. A record X
arrives on D whose header advertises C@5 (so completeness now satisfies R), but X itself depends on an
orphaned coordinate → `isProvenImpossible` → early return. R stays buffered even though it's now
deliverable. If X was the last inbound record and no watermark arrives on this node afterward
(watermarks are emitted per upstream record, not on a timer), R is held indefinitely; an in-flight
epoch transition window that this advance would have closed also stays pending.

**Impact:** liveness only (not a causal-safety violation), but the stall is permanent in an otherwise-
quiescent stream.

**Coverage:** Not covered by any existing test.

## [LOW-MEDIUM] New sink joins and idle upstream lanes cause epoch floor stalls via marker-reachability gaps

**Where:** `ParsleyEpochLog.java:54-61` and `:135-144` (`externalSourceTopics()` flips a topic to
"internal" immediately at `JoinRequested`, one full round before the declaring producer can actually
relay markers); consumer side `ParsleyProcessor.java:705-720`; joiner side
`ParsleyProcessor.java:253-255` (`lastAdoptedEpoch` initialised to the already-committed epoch, so the
joiner never injects the boundary for the epoch that admitted it)

**Concrete repro sequence:** Topic T is consumed by member B and produced by nobody (external — B
self-adopts floors for T from the log). New member P joins declaring T as a sink. The instant P's
`JoinRequested` folds, T leaves `externalSourceTopics()`; P is still blocked in `awaitJoinCommit` and
produces nothing on T yet. P's join opens the round; epoch E+1 commits. B no longer log-adopts for T
(it's no longer external) and no in-band `EPOCH_BOUNDARY` ever arrives on channel T for E+1 (P unblocks
with `lastAdoptedEpoch = E+1`, so it doesn't re-inject that boundary). B stays settled at E —
conservative, so causally safe — until epoch E+2's markers flow through P and the nested-boundary
collapse in `ParsleyEpochState.onBoundary` (`ParsleyEpochState.java:117-133`) catches B up. This is a
one-round (or permanent, if no further transition ever happens) floor stall, caused by construction of
the very round that admits the new producer.

`ParsleyEpochLogTest.externalSourceTopicsCountPendingDeclarationsAndSurviveACommit` asserts the
immediate-flip behaviour as intended, but nothing tests the marker-reachability consequence
downstream.

**Related, same family:** `injectSnapshot`/`adoptAndInjectBoundary` skip the downstream relay when
`lastSeenKey == null` (`ParsleyProcessor.java:741-746, 764-766`), and boundary/snapshot markers are
injected once per round (guarded by `lastSnapshotRoundEpoch`/`lastAdoptedEpoch`). A downstream task
with one idle upstream partition lane therefore never receives that channel's marker, so its pending
window (`tryAdvanceEpoch` requires a marker on *every* channel — `ParsleyFrontier.java:284-288`) never
closes for that epoch; the floor lags until a later epoch's markers arrive after the lane has seen
traffic.

**Impact:** conservative-safe (never causally unsafe) but a real, sometimes-permanent-per-epoch
liveness stall.

## [LOW] Replayed already-delivered offsets leak permanent forwarded-index entries

**Where:** `ParsleyFrontier.java:196-214` (`deliver`)

`deliver` unconditionally `mark()`s the offset, but the absorb walk only scans
`forwardedAfter(frontier)`. On an at-least-once replay of an already-delivered record (crash after the
frontier flush but before the offset commit), `deliver(C,k)` with `frontier(C) >= k` marks `k` below
the current watermark, and nothing ever unmarks it.

**Impact:** harmless to gating — queries into the forwarded index start at `frontier + 1` — but each
such entry lives in the changelog-backed store forever, i.e. unbounded, purely cosmetic growth.

**Suggested fix:** a one-line guard, e.g. `if (offset <= watermark) return watermark;` before marking.

**Coverage:** No test covers redelivery of an already-frontier-absorbed offset.
