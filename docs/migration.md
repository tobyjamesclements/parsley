# Migration

Adopting Parsley in a cluster where some producers do not yet stamp the
`parsley-causal-clock` header requires no special configuration. A record with a missing
header is treated as having an empty, vacuously satisfied dependency set and is forwarded immediately.
Because such a record is never buffered, there is no migration setting to choose and nothing to
tighten later.

## Recommended migration strategy

### Phase 1: introduce a causal topology and tolerate unstamped producers

Build the topology with `CausalStreamsBuilder` against the full topic set immediately. Records from
producers that do not yet stamp the `parsley-causal-clock` header pass straight through,
unbuffered, because their empty dependency set is vacuously satisfied. Records that already arrive
stamped are held until their dependencies are satisfied. The two kinds of producer coexist on the same
topics without any configuration distinguishing them.

### Phase 2: migrate producers one service at a time

Have each producer stamp its records — accumulate a `CausalClock` with `using`/`observe` (or
`builder`/`require`) and attach it with `.stamp(record)` — one service at a time. As a service
migrates, its records start arriving with a valid header and are held until their dependencies are
satisfied. The remaining unmigrated services continue to pass through unbuffered.

To track migration progress, inspect the `parsley-causal-clock` header directly on records
from a given topic, for example with a side consumer or a temporary log. A topic with no producers
left stamping it has fully adopted the guarantee.

### Phase 3: done once every producer is migrated

Once every producer on every relevant topic stamps the header, the guarantee applies uniformly. There
is no follow-up configuration step, because no setting was ever gating the pass-through of unstamped
records.

## Notes

- A record with no header still feeds the frontier exactly like any other delivery. Records buffered
  downstream of a still-unmigrated producer therefore catch up once that coordinate's gap closes, and
  they are not permanently stalled.
- A present but undecodable header indicates a genuine bug rather than a migration artefact — it
  unconditionally fails the task at `ERROR`, never forwards the record on an unknown premise. Alert on
  it separately from missing-header records, because it indicates corruption rather than an unmigrated
  producer. See [Troubleshooting](troubleshooting.md).
