# Backlog

Findings from an automated correctness review of the causal engine
(`ParsleyClock`/`ParsleyFrontier`/`ParsleyEngine`) and the topology-epoch coordination protocol
(`ParsleyEpochLog`/`ParsleyEpochState`/`ParsleyCoordination`/`ParsleyEpochRuntime`), reviewed with
Fable 5, plus findings from the follow-up verification review of the four fix commits
`e0109eb..5dc1d04`, plus a third-pass review (also Fable 5) of the five fix commits
`02e08fb..3658689` that closed every finding from that verification review. Ranked most-severe first.
GitHub Issues are disabled on this repo, so these are tracked here instead.

## [HIGH] `tryAdvanceEpoch` gates on the DAG-wide floor, so an epoch transition can never settle at a non-terminal node

**Where:** `ParsleyFrontier.java:325` (`completeness().dominates(pendingFloor)`), fed by
`ParsleyEpochLog.proposeCommit` (`ParsleyEpochLog.java:204-214`).

The committed `lowerBounds` is the `mergeMin` of every member's published completeness, and a
member's completeness includes its **own input-topic coordinates** plus everything advertised
upstream. So the floor for epoch *e* contains coordinates for every topic in the DAG — including
topics *downstream of* (or parallel to) any given node. `tryAdvanceEpoch` then requires the node's
local `completeness()` to dominate that whole clock. A node's completeness only ever contains its own
and upstream coordinates (`dominates` fails on any absent coordinate), so the check is permanently
false at every node whose output topic appears in the floor — i.e. **every non-sink-most stage**.

**Concrete repro:** the `ParsleyCoordinationMultiAppIT` topology (t1 → A → mid → B → out). B publishes
{t1, mid}; the committed floor contains `mid`; A's completeness is {t1} forever. A's transition for
epoch 1 goes pending and never closes; every later epoch supersedes the pending one and also never
closes. A's `settledEpochId` stays 0 for the life of the topology.

**Impact:** conservative for causal safety (the effective floor just never rises), but the epoch
feature's core mechanism — per-coordinate floor advance, below-floor dependency stripping, orphan
pruning on promotion — is silently inert everywhere except terminal stages. A forced floor advance
intended to recover a stuck lane will not release anything buffered at an upstream node, and
`pruneStaleOrphans` never runs there. Also note the asymmetry: a *fresh* task settles directly at the
full committed floor in `ParsleyProcessor.init`, so joiners get floors that established members can
never reach.

**Fix shape:** restrict the dominance check to the coordinates this node can ever observe — e.g.
`pendingFloor` filtered to coordinates present in `completeness()` (or in the channel scope), the same
scoping `pruneToScope` already applies to the frontier. Needs a design decision on whether
"never-yet-advertised upstream coordinate in the floor" should hold the window (conservative) or be
out of scope (matches the fresh-joiner settle-directly semantics).

**Coverage:** `ParsleyProcessorEpochBoundaryTest` uses a floor covering only the node's own consumed
coordinate (`ParsleyProcessorEpochBoundaryTest.java:81`); `ParsleyCoordinationMultiAppIT` asserts only
that commits land on the log (line 111), never that any node's local epoch state settles. Code gap.

## [MEDIUM] `orphan()`'s scan gating is floor-blind: a second worklist task on the same coordinate with a *lower* floor skips its scan

**Where:** `ParsleyEngine.java:737` (`scannedCoordinates.add(new Coord(...))` → `continue`), the set
introduced by the d7a852e decoupling.

`scannedCoordinates` keys on `(topicId, partition)` only. Two victims on the same coordinate can be
discovered via *different* parent coordinates in one cascade, higher offset first: root orphans Z;
scanning Z dead-letters V1=(D@d) and V2=(E@e); scanning D dead-letters C@10 (enqueue task (C,10));
scanning E dead-letters C@5 (enqueue (C,5)). The FIFO pops (C,10) first — scans dependents requiring
C ≥ 10 — then pops (C,5) and **skips the scan entirely**, so a buffered record requiring C@7 is never
scanned in this pass. (Same-coordinate victims can't produce this via one parent — Kafka FIFO gives
ascending buffer sequences — but cross-branch discovery order is unconstrained.)

**Impact:** combined with the inverted floor monotonicity above (the durable floor also stays 10),
the C@7 dependent is stranded permanently. With the floor fixed to min, the miss degrades to "not
dead-lettered until the next full drain" — a liveness delay that is unbounded on an idle topology.
This is the answer to the review question "did the decoupling open a zero-scan gap": yes, for the
`[lowerFloor, scannedFloor)` range of an already-scanned coordinate.

**Fix shape:** replace the set with `Map<Coord, Long> lowestScannedFloor` and rescan when
`task.floor() < recorded`, recording the min.

**Coverage:** no cascade test produces two floors for one coordinate in one `orphan()` call.

## [LOW] Minor observations

- **Stale below-watermark forwarded-index entries are never pruned.** The `5dc1d04` guard stops new
  at-or-below-watermark marks, but entries already leaked below the watermark (e.g. by the
  acknowledged benign tear direction in `deliver`: frontier persisted, unmark lost) linger forever —
  the absorb walk only scans strictly above the watermark. A cheap sweep (delete entries ≤ watermark
  on restore or on epoch promotion) would clean both classes. Purely cosmetic store growth. Also now
  largely moot: `exactly_once_v2` (required as of the write-ordering fix) means the acknowledged benign
  tear direction that caused this leak can no longer happen at all.
- **The orphan cascade never prunes the stale candidate-index entries it discovers.** `orphan()`'s
  `letter == null` branch (`ParsleyEngine.java:744`) drops a stale candidate without `prune`, and —
  unlike `propagate`'s stale handling — an *orphaned* coordinate never advances, so `findCandidates`
  never revisits it either: entries indexed on an orphaned coordinate for since-removed records live
  in the changelog-backed store forever. Same class as the pruned forwarded-index observation above;
  a `candidateIndex.prune(candidate)` on the null branch closes it.
- **Torn read of the `committedEpochId`/`committedLowerBounds` volatile pair.**
  `pollEpochCoordination` reads the id (`ParsleyProcessor.java:756`) and the bounds (line 761) as two
  separate volatile reads while `runOnce` writes id-then-bounds (`ParsleyEpochRuntime.java:273-274`);
  a commit landing in between yields a boundary stamped `(N+1, F_N)`, which is then relayed DAG-wide
  and never re-adopted (the per-epoch guard advances). The stale floor is strictly lower, so every
  consumer is merely conservative until epoch N+2, but a single volatile record holding both (or
  reading through one snapshot object) would remove the tear entirely.
- **Epoch floors are not actually monotonic across epochs, contrary to `ParsleyEpochState.onBoundary`'s
  Javadoc.** A member admitted at epoch *e* that consumes from `earliest` publishes completeness far
  behind `F_e` at round *e+1*, and `proposeCommit`'s `mergeMin` drags `F_{e+1}` below `F_e` on shared
  coordinates. Every use is conservative-safe on regression *except* `pruneStaleOrphans`
  (`ParsleyFrontier.java:341-348`): an orphan entry pruned under a floor that later regresses below it
  leaves dependents on the orphaned coordinate held forever again. Exotic today (requires a promote,
  which the DAG-wide-floor bug mostly prevents upstream), but worth a guard once that bug is fixed —
  e.g. clamp `mergeMin` per coordinate to the previous floor at commit time.
- **Marker routing assumes the sink topic has at least `taskId().partition() + 1` partitions.**
  `ParsleyMarkerPartitioner` returns the task's own partition unconditionally; with mismatched
  partition counts (a co-partitioning violation the parity check only *warns* about by default) the
  produce fails and the task crash-loops rather than surfacing the misconfiguration. Consider failing
  the parity check hard when coordination is configured, or clamping with a clear error.
