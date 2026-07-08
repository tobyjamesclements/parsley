# Backlog

Findings from an automated correctness review of the causal engine
(`ParsleyClock`/`ParsleyFrontier`/`ParsleyEngine`) and the topology-epoch coordination protocol
(`ParsleyEpochLog`/`ParsleyEpochState`/`ParsleyCoordination`/`ParsleyEpochRuntime`), reviewed with
Fable 5, plus findings from the follow-up verification review of the four fix commits
`e0109eb..5dc1d04`. Ranked most-severe first. GitHub Issues are disabled on this repo, so these are
tracked here instead.

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
