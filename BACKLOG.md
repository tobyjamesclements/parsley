# Backlog

Findings from an automated correctness review of the causal engine
(`ParsleyClock`/`ParsleyFrontier`/`ParsleyEngine`) and the topology-epoch coordination protocol
(`ParsleyEpochLog`/`ParsleyEpochState`/`ParsleyCoordination`/`ParsleyEpochRuntime`), reviewed with
Fable 5. Ranked most-severe first. GitHub Issues are disabled on this repo, so these are tracked here
instead.

## [LOW-MEDIUM] Marker relay rides a business key, so an idle upstream partition lane can starve a channel's epoch marker

**Where:** `forwardEpochBoundary`/`forwardEpochSnapshot` (via `injectSnapshot`/`adoptAndInjectBoundary`
and `handleEpochBoundary`/`handleEpochSnapshot`'s downstream relay), all of which route the outbound
marker on `lastSeenKey`/`record.key()` — a single business key, not an explicit per-partition broadcast

**Narrowed from the original finding.** The originally-reported repro (a new member's `JoinRequested`
flipping a topic's `externalSourceTopics()` membership one full round before the declaring producer
could relay anything, plus a fresh joiner's `lastAdoptedEpoch` being pre-seeded to the epoch that
admitted it) is fixed: `ParsleyProcessor` now tracks `lastAdoptedExternalSourceTopicIds` and injects a
newly-adopted epoch's boundary onto `live ∪ lastAdopted`, giving a topic that stops being external
mid-round exactly one more adoption cycle from its outgoing self-adopter before the cache catches up
to the live registry; the fresh-joiner pre-seed is gone (harmless locally — the joiner's own
`ParsleyEpochState` already settles directly at the admitting floor — but was silently dropping that
joiner's own downstream relay); and the `lastAdoptedEpoch`/`lastSnapshotRoundEpoch` guards no longer
advance past an epoch/round whose relay was skipped for lack of a `lastSeenKey`, so a task that has not
forwarded anything yet keeps retrying instead of forfeiting its one chance forever. Regression test:
`ParsleyProcessorSourceLayerTest#outgoingSelfAdopterStillInjectsTheHandoffEpochsBoundaryAfterATopicLeavesTheLiveRegistry`.

**What's still open.** All of the above relay mechanisms route the outbound marker to wherever
`record.key()`/`lastSeenKey` happens to hash on this task's *own* output partitions — there is no
explicit "broadcast to every partition this task owns" step. A downstream task consuming multiple
partitions of the same topic, one of which is genuinely idle (no key ever routes there because nothing
causally requires it), never receives that partition's marker; its pending transition window
(`tryAdvanceEpoch` requires a marker on *every* channel) never closes for that epoch until a later
epoch's traffic happens to touch that partition. This is a different, deeper question than the
registry-timing gap above — whether epoch markers should be explicitly broadcast per owned output
partition rather than piggybacked on business-record key routing — and needs its own design pass, not
a client-side caching fix.

**Impact:** conservative-safe (never causally unsafe) but a real, sometimes-permanent-per-epoch
liveness stall for a topic with a genuinely idle partition.

**Coverage:** Not covered by any existing test.
