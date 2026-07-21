# Incremental adoption

Adopting Parsley in a cluster where some producers do not yet stamp the `parsley-causal-clock`
header requires no special configuration. A record with a missing header is treated as having an
empty, vacuously satisfied dependency set and is forwarded immediately. Because such a record is
never buffered, there is no adoption setting to choose and nothing to tighten later. A producer that
stamps nothing claims nothing, so its records are causally minimal by definition: safe to deliver at
once, and correctly ordered below anything that later depends on them.

## Phase 1: introduce a causal topology and tolerate unstamped producers

Build the topology with `CausalStreamsBuilder` against the full topic set immediately. Records from
producers that do not yet stamp the `parsley-causal-clock` header pass straight through,
unbuffered, because their empty dependency set is vacuously satisfied. Records that already arrive
stamped are held until their dependencies are satisfied. The two kinds of producer coexist on the
same topics without any configuration distinguishing them.

## Phase 2: migrate producers one service at a time

Have each producer stamp its records — accumulate a `CausalClock` with `using`/`observe` (or
`builder`/`require`) and attach it with `.stamp(record)` — one service at a time. As a service
migrates, its records start arriving with a valid header and are held until their dependencies are
satisfied. The remaining unmigrated services continue to pass through unbuffered.

To track adoption progress, inspect the `parsley-causal-clock` header directly on records
from a given topic, for example with a side consumer or a temporary log. A topic with no producers
left unstamped has fully adopted the guarantee.

## Phase 3: done once every producer stamps

Once every producer on every relevant topic stamps the header, the guarantee applies uniformly.
There is no follow-up configuration step, because no setting was ever gating the pass-through of
unstamped records. Note the resulting contract: causal order is guaranteed only along paths where
every intermediate processor stamps, so a service that consumes stamped topics and re-produces
unstamped output severs the causal chain for everything downstream of it. See the
[participation precondition](streams.md#preconditions) and environmental assumption E3 of the
[causal consistency model](internals/causal-consistency.md#environmental-assumptions).

## Notes

- A record with no header still feeds the frontier exactly like any other delivery. Records buffered
  downstream of a still-unmigrated producer therefore catch up once that coordinate's gap closes, and
  they are not permanently stalled.
- A present but undecodable header indicates a genuine bug rather than an adoption artefact. It
  unconditionally fails the task (`CausalVectorClockResolutionException`), never forwarding the
  record on an unknown premise. Alert on it separately from missing-header records, because it
  indicates corruption rather than an unmigrated producer. See
  [Troubleshooting](troubleshooting.md).
