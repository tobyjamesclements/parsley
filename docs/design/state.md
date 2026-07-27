# State and recovery

All causal state a node persists lives under per-channel keys in one keyed store, committed in
the same EOS transaction as the delivery that mutated it and the sends it caused. Only entries
that changed are written on an advance. The full byte layouts are in
[wire format](../reference/wire-format.md).

| Key | Holds |
|---|---|
| `scope` | The declared input channels and sink topics the state was written under |
| `f/<channel>` | The channel's contiguous delivered frontier offset |
| `cc/<channel>` | The channel's advertised clock: folded clocks of records received on it |
| `ca` | Carried ancestry: delivered past on channels no longer consumed |
| `q/<channel>/m` | Hold-queue indices: head and one-past-tail |
| `q/<channel>/e/<index>` | One held record, verbatim: offset, timestamp, clock, key, value |

Two clocks deliberately do **not** persist. `ownOutputs` is re-derived at every init from sink
end offsets, which dominate anything a persisted copy could hold
([architecture](architecture.md#the-stamp)). The position-known watermark (everything below is
fetched-or-skipped) is re-reported by the host's position advances after restart.

## Density adaptation

A `read_committed` consumer's view of a partition is not dense — transaction markers and
aborted records occupy offsets that are never returned, and retention means consumption need
not start at zero. Three receive-side mechanisms, one invariant: the frontier equals the
highest offset below which everything fetchable was delivered and everything else was
skipped.

- **Seed**: the first record ever received on a channel folds everything below it into the
  frontier. History below first sighting is a baseline, not a gap.
- **Bridge**: a received offset that jumps past the previous position proves the run between
  was consumer-skipped; with an empty hold queue the frontier folds the run.
- **Position advance**: the trailing case — skips with no record after them — reported from
  the consumer's position ([liveness](../foundations/liveness.md#position-advance-bridging)).

Seed and bridge apply only when the channel's hold queue is empty; while records are held, the
frontier advances through deliveries alone, and a drained queue folds any clean prefix the
position has revealed since.

## Crash recovery

Everything committed is a consistent cut: frontier, advertised clocks, carried ancestry, hold
queues, consumed offsets, and produced records all move in one transaction. Recovery is
therefore a pure restore — read the store, rebuild the queues from their indices, re-seed
`ownOutputs` from sink end offsets, and resume. An aborted transaction leaves aborted records
at real offsets; consumers bridge them like any other skip. Init-time writes are idempotent,
so a crash between init and the first commit re-runs the same reconciliation.

The verification suite drives this path hard: crash injection both between steps and
mid-transaction, with the oracle's own state rolled back in mirror, and completeness checked
after recovery ([verification](verification.md)).

## Scope changes

A redeploy can change a stage's declared inputs while its state survives. The recorded `scope`
is diffed against the current declaration at init. The governing rule: **the causal past a
node has delivered or carried may be skipped, but never dropped and never re-entered.**

- **Shrink** — an input leaves scope: its frontier entry and advertised clock max-merge into
  carried ancestry, which every later stamp folds forever (truncation aside). Dropping them
  instead would let a third party reorder an effect against its retired-channel cause. A held
  record on the departing channel fails init loudly: it can be neither delivered (no source)
  nor discarded (fail closed) — redeclare the input or perform a full reset.
- **Growth** — a new input: its frontier seeds at the node's carried knowledge of the channel
  (never log-start), and the host resumes fetching one past the seed — skip what was already
  ignored. An operator who wants that history processed performs a full reset. Replayed
  prefixes below the seed are dropped by the replay guard.
- **Former sinks**: their last claims must survive in the stamp even though `ownOutputs` is
  memory-only. Init folds each former sink's end offsets into carried ancestry (the same
  delay-only over-claim as the seed); a topic that no longer exists is unclaimable by any
  receiver and is skipped.

## Truncation

`truncate(stability)` removes entries at or below the bound from carried ancestry and the
advertised clocks — the two stamp-side clocks whose width otherwise grows monotonically with
the node's transitive upstream. The frontier and `ownOutputs` stay: their width is bounded by
the node's own channels. The shipped stability source is the log-start offset
([architecture](architecture.md#truncation-log-start-stability)): retention-deleted records
are below every reachable baseline, so the bound needs no coordination and no membership. The
simulator verifies both that full-retention truncation empties the stamp-side clocks with
later traffic staying causal, and that a from-earliest joiner arriving after truncation is
correctly ordered (its baseline is the log start).
