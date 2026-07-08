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
  on restore or on epoch promotion) would clean both classes. Purely cosmetic store growth. Also now
  largely moot: `exactly_once_v2` (required as of the write-ordering fix) means the acknowledged benign
  tear direction that caused this leak can no longer happen at all.
