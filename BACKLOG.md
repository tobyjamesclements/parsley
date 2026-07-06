# Backlog

Findings from an automated correctness review of the causal engine
(`ParsleyClock`/`ParsleyFrontier`/`ParsleyEngine`) and the topology-epoch coordination protocol
(`ParsleyEpochLog`/`ParsleyEpochState`/`ParsleyCoordination`/`ParsleyEpochRuntime`), reviewed with
Fable 5. Ranked most-severe first. GitHub Issues are disabled on this repo, so these are tracked here
instead.

## [HIGH] propagate() can both forward and dead-letter the same record in one cascade pass

**Where:** `ParsleyEngine.java:556-616` (`propagate()`), `ParsleyEngine.java:637-661` (`orphan()`),
`ParsleyEngine.java:674-687` (`fetchForDeadLetter`)

`propagate()` scans candidates and collects deliverable entries into a `releasable` list in a first
phase, then removes/forwards them in a second phase. If a *later* candidate in the same scan is poison
and triggers the orphan cascade (`deadLetterRoot` → `orphan()`), that cascade calls
`fetchForDeadLetter`, which finds an already-collected `releasable` entry **still present in the
buffer** (it's only removed in the later release loop) and dead-letters it there. The release loop then
still forwards it anyway: `buffer.remove` is a silent no-op on an already-removed sequence, but
`frontier.deliver(...)` and `out.add(entry.record())` run unconditionally on the stale collected entry.

**Concrete repro sequence** (dead-lettering enabled; fan-in channels C, D, E):
1. Deliver C@0-4 and D@0-1 (no deps). Frontier: C=4, D=1.
2. R = D@3, deps {C@5, E@7} → held (seq 0), indexed on (C,5) and (E,7).
3. P = C@5, deps {E@7} → held (seq 1), indexed on (E,7). The buffer value for seq 1 later becomes
   undecodable (poison).
4. C@6, deps {C@5} → held; its receipt-time `channelUpdate` makes channel C advertise C@5.
5. E@7 arrives, deps {C@5}: channel E now advertises C@5, and channel D already advertises C@5 via R's
   deps. completeness(C)=5, so E@7 is deliverable → `frontier.deliver(E,7)` → `propagate(E)`.
6. `findCandidates(E,7)` returns R (seq 0) then P (seq 1). R passes `isDeliverable` → added to
   `releasable`. P then throws on `buffer.get` → dead-lettered POISON → `orphan(C, 0, floor 5)` →
   `findCandidatesRequiringAtLeast(C,5)` finds R → R is still in the buffer → removed and dead-lettered
   ORPHAN_CASCADE, and D@3 is recorded as forever-unreachable via `markOrphaned`.
7. Release loop then forwards R anyway (`out.add`) and marks D@3 forwarded via
   `frontier.deliver(D,0,3)`.

**Impact:** R appears in both `Outcome.delivered` and `Outcome.deadLettered`, violating the `Outcome`
record's own "disjoint, never both" contract (`ParsleyEngine.java:215-224`). The orphan index
permanently claims D can never reach offset 3 while the forwarded index says D@3 was delivered — every
future record depending on D@3+ is wrongly dead-lettered at ingest via `isProvenImpossible`.

**Related gap, same root cause:** `propagate()` never calls `isProvenImpossible` at all (unlike
`onRecord` L323 and `drainSatisfied` L423), so a buffered record that is simultaneously "deliverable"
and "proven impossible" gets a path-dependent disposition depending on whether `drainSatisfied` or a
`propagate` cascade reaches it first.

**Coverage:** `ParsleyEngineDeadLetterTest.poisonCascadesToEveryBufferedDependentInOnePass` exercises
the cascade, but with the poison record as the *first* candidate scanned and with
`trackChannels=false`, so the collect-then-cascade interleaving above is unreachable in that test. This
is a code gap, not just a coverage gap.

## [HIGH] awaitJoinCommit can deadlock an instance when a joiner shares a StreamThread with a running member

**Where:** `ParsleyCoordination.java:132-145` (`awaitJoinCommit`), invoked from `ParsleyProcessor.init`
(`ParsleyProcessor.java:237-245`)

The join block spins on the task thread inside `init()`. Round completion requires every running
member to publish, and publication only happens from task threads (`pollEpochCoordination`,
`ParsleyProcessor.java:695-701`, and `handleEpochSnapshot`) — the epoch-runtime poll thread never
publishes. Kafka Streams runs all of a `StreamThread`'s tasks, including their `init()` calls and
punctuators, on that one thread; while one task's `init()` blocks, no sibling task on that thread can
process or punctuate.

**Concrete repro sequence:** A topology with committed epoch ≥ 1 is redeployed with a new stage — the
epoch system's headline use case. Single instance, `num.stream.threads=1` (the default). The new
stage's task joins as a fresh member → `requestSnapshot` → blocks in `awaitJoinCommit`. The existing
stage's task — a running member whose publication the round needs — lives on the same `StreamThread`
and can never run `pollEpochCoordination` to publish. The round never completes; the joiner blocks
forever; the whole instance is wedged. Init order doesn't matter: even a running member that
initialised first cannot publish once the joiner's `init()` blocks the thread.

**Coverage:** `ParsleyCoordinationTest.awaitJoinCommitDoesNotBlockAtEpochZero` /
`...ForAnAlreadyRunningMember` only test the non-blocking branches;
`ParsleyCoordinationTopologyTest.runningTopologyEvolvesThroughAnEpochTransition` drives the runtime
synchronously. Nothing exercises a blocking join co-hosted with a running member on the same thread.
Code gap.

## [MEDIUM-HIGH] ParsleyEpochRuntime.unregisterMember is never called: leave() can evict active members or hang close()

**Where:** `ParsleyEpochRuntime.java:101-104` (`unregisterMember`, defined but never called anywhere in
`src` — verified by grep), `ParsleyProcessor.close()` (`ParsleyProcessor.java:403-412`, unregisters
from quiesce but not from the epoch runtime), `ParsleyCoordination.leave()`
(`ParsleyCoordination.java:202-224`)

**Concrete repro sequence:** Task `app/0_1` is rebalanced from instance A to instance B. A's
`localMembers` still contains `app/0_1` forever, since nothing ever calls `unregisterMember`. Later,
`CausalStreams#close()` on instance A runs `leave()`:
1. Phase 1 waits on `allLocalMembersDrained()`, which still includes the departed member whose
   `reportDrained` is no longer refreshed on A — if its last report was non-empty, A's `close()` hangs
   unboundedly.
2. If it proceeds anyway, phase 2 appends `Leave(app/0_1)` for a member that is actively consuming with
   a possibly un-drained buffer on B — precisely the "excluding an un-drained member" hazard that
   `ParsleyMembershipStrategy`'s class Javadoc names as the safety invariant it exists to prevent.
3. The evicted member also stops being awaited by subsequent rounds, so floors advance without its
   publications.

**Coverage:** No test covers rebalance/task-migration interaction with `leave()`. Code gap.

## [MEDIUM] Torn changelog flush under at-least-once can permanently strand a coordinate's frontier

**Where:** `ParsleyEngine.java:451-453` (`drainSatisfied`: `buffer.remove` then `frontier.deliver`) and
`ParsleyEngine.java:599-602` (`propagate`, same shape); frontier persistence in
`ParsleyFrontier.java:342-346`

The buffer store, frontier store, and forwarded-index store are three separate changelog topics with
no cross-store atomicity. Under at-least-once (which `docs/streams.md:91-94` explicitly documents as
safe), a crash between the buffer-removal changelog commit and the frontier/forwarded-index changelog
commit for the same release leaves:

- the record gone from the buffer (so `drainAfterRestore` can't re-deliver it — its source offset
  commit already advanced past the point where it was originally buffered), but
- the frontier still showing it undelivered, with no forwarded-index mark.

`mergeForward` (`ParsleyFrontier.java:330-340`) can then never cross that offset again, so every future
dependency on that coordinate at or past it holds forever. The `drainAfterRestore` Javadoc
(`ParsleyEngine.java:399-403`) documents only the benign opposite tear (frontier ahead of buffer →
duplicate delivery); this direction is a permanent wedge, not a duplicate.

**Impact:** the "safe under at-least-once" guarantee documented for the delivery semantics does not
actually hold for this specific tear. EOS avoids it.

**Coverage:** No test covers a cross-store tear between the buffer changelog and the
frontier/forwarded-index changelog. Code/documentation gap.

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

**Where:** `ParsleyFrontier.java:330-340` (`mergeForward`)

`mergeForward` unconditionally `mark()`s the offset, but the absorb walk only scans
`forwardedAfter(frontier)`. On an at-least-once replay of an already-delivered record (crash after the
frontier flush but before the offset commit), `deliver(C,k)` with `frontier(C) >= k` marks `k` below
the current watermark, and nothing ever unmarks it.

**Impact:** harmless to gating — queries into the forwarded index start at `frontier + 1` — but each
such entry lives in the changelog-backed store forever, i.e. unbounded, purely cosmetic growth.

**Suggested fix:** a one-line guard, e.g. `if (offset <= watermark) return watermark;` before marking.

**Coverage:** No test covers redelivery of an already-frontier-absorbed offset.
