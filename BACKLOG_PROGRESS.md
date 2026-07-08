# BACKLOG.md fix-session progress

Recovery checkpoint for the overnight run fixing every item in `BACKLOG.md`. If resuming: read this
file top to bottom, check `git log` against the commit hashes recorded below, and pick up at the
first item not yet marked done.

## Order of attack (6 commits covering the 7 backlog bullets)

1. **[HIGH]** `tryAdvanceEpoch` DAG-wide floor bug — `ParsleyFrontier.java`
2. **[MEDIUM]** `orphan()` floor-blind scan gating — `ParsleyEngine.java`
3. **[LOW pair]** Stale forwarded-index entries + stale candidate-index entries on the orphan null
   branch — these two are explicitly called out in BACKLOG.md as "same class", fixed together.
4. **[LOW]** Torn read of `committedEpochId`/`committedLowerBounds` — `ParsleyEpochRuntime.java` /
   `ParsleyProcessor.java`
5. **[LOW]** Epoch floors not actually monotonic across epochs — `ParsleyEpochLog.java`
6. **[LOW]** Marker routing assumes sink partition count — `ParsleyProcessor.java` partition-parity
   check

Each is its own commit (or tight pair, for #3), full test suite green before each commit.

## Design decision for the HIGH item (recorded before starting, per instructions)

**Question:** `tryAdvanceEpoch`'s dominance check must be scoped to "the coordinates this node can
ever observe" rather than the whole DAG-wide committed floor. Two sub-choices were on the table:

- (a) Filter the pending floor to `channels.keySet()` — this node's own registered input
  coordinates (structural scope: the topics it actually consumes) — keeping an entry even if that
  channel hasn't advertised anything yet for it.
- (b) Filter the pending floor to the coordinate set currently present in `completeness()`'s
  result — i.e. drop anything not yet advertised, whether or not it's structurally in scope.

**Decision: (a).** A coordinate that's structurally in this node's channel scope but not yet
advertised by that channel will *naturally* still fail `dominates()` (an absent coordinate reads as
observed = -1), so filtering by (a) still **holds the window** for it — conservative, matches
"causal safety is inviolable: block over guess". Filtering by (b) instead would silently **drop**
that coordinate from the check the moment it's not yet present in completeness(), permissively
letting the transition close without ever having waited on it — that's the same permissive shape as
the fresh-joiner settle-directly semantics, which is deliberately *not* what an established member
should get (an established member has in-flight state to reconcile; a fresh joiner does not).

Coordinates *not* in `channels.keySet()` at all (e.g. a downstream node's own input, folded into the
floor via `mergeMin` over every member) are genuinely unobservable by this node — this node's
topology can never advance them regardless of how long it waits — so dropping those specific entries
is not a conservatism trade, it's just removing a check that could never pass. That's what actually
unblocks the repro in BACKLOG.md (node A stuck on the `mid` coordinate it never consumes).

Implementation: reuse the existing `ParsleyClock.retaining(CoordinatePredicate)` method (already used
by `pruneToScope` for the identical "this node's own channel coordinates" concept) to filter
`pendingFloor` before the `dominates` check.

## Status

- [ ] 1. HIGH — tryAdvanceEpoch scope fix. Commit: (pending)
- [ ] 2. MEDIUM — orphan() lowestScannedFloor fix. Commit: (pending)
- [ ] 3. LOW pair — stale forwarded-index sweep + candidate-index prune on orphan null branch. Commit: (pending)
- [ ] 4. LOW — torn committedEpochId/committedLowerBounds read. Commit: (pending)
- [ ] 5. LOW — epoch floor monotonicity clamp. Commit: (pending)
- [ ] 6. LOW — marker routing / partition-parity strictness under coordination. Commit: (pending)
- [ ] Final: BACKLOG.md cleared, progress file retired. Commit: (pending)

Update each line with the commit hash immediately after committing, before moving to the next item.
