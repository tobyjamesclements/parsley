# Migration

Adopting Parsley in a cluster where some producers do not yet stamp the
`parsley-causal-dependencies` header requires no special configuration: a record with a missing or
undecodable header is treated as having an empty, vacuously satisfied dependency set, and is
forwarded immediately, stamped `SATISFIED`. There is no policy to choose and nothing to tighten
later — the always-forward model has only one behaviour.

## Recommended migration strategy

### Phase 1 — introduce Parsley processors, tolerate unstamped producers

Turn on `CausalProcessors` against the full topic set immediately. Records from producers that don't
yet stamp the `parsley-causal-dependencies` header pass straight through, unbuffered, stamped
`SATISFIED`. Records that already arrive stamped are held until their dependencies are satisfied and
benefit from the guarantee right away — the two kinds of producer coexist on the same topics without
any configuration distinguishing them.

### Phase 2 — migrate producers one service at a time

Have each producer stamp its records with `CausalDependencies.stamp` one service at a time. As each
service migrates, its records start arriving with a valid header and are held until their
dependencies are satisfied. The rest of the cluster benefits from the guarantee immediately;
remaining unmigrated services continue to pass through unbuffered.

To track migration progress, check the `parsley-causal-dependencies` header directly on records
from a given topic (e.g. via a side consumer or a temporary log) — a topic with no producers left
stamping it has fully adopted the guarantee.

### Phase 3 — done once every producer is migrated

Once every producer on every relevant topic stamps the header, the guarantee applies uniformly —
there's no follow-up configuration step, since there was never a policy gating it.

## Notes

- A record with no header (or an absent-but-vacuously-satisfied dependency set) still feeds the
  frontier exactly like any other delivery, so records buffered downstream of a still-unmigrated
  producer catch up once that coordinate's gap closes — they are not permanently stalled.
- A present-but-undecodable header (a genuine bug, not a migration artefact) is logged at `WARN`
  and also treated as an empty, vacuously satisfied dependency set — it does not block delivery,
  but is worth alerting on separately from missing-header records, since it indicates corruption
  rather than an unmigrated producer.
