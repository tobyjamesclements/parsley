# Environmental assumptions

The [delivery gate](delivery-gate.md) is sound as an algorithm. The guarantee it delivers rests, in
addition, on three stated assumptions about the environment. None is enforced by coordination. The
first two are enforced in code where that is possible, and all three are operational constraints to
design for.

## E1 — stable channel identity

A coordinate must never rebind to a different record. Kafka violates this on topic delete and
recreate, because offsets restart under a new incarnation, so identity is keyed by topic UUID and
bound once per process lifetime, and a background topic-identity poll fails the application fast when
a causal topic's UUID changes mid-run. Detection is bounded by the poll interval, not instantaneous,
so live recreation of a causal topic remains an operational error, loud and bounded rather than
silent. Across a restart, recreation degrades to history loss (E2's kind), never reordering: a clock
naming the old UUID can never be satisfied by the new topic's offsets. See
[Troubleshooting](../guide/troubleshooting.md#a-causal-topic-was-deleted-or-recreated-while-the-application-ran).

## E2 — retention must not destroy causally-live history

A record whose effects some running or future consumer still needs must not be expunged before every
such consumer has delivered it. No protocol can deliver a destroyed record. Parsley enforces the
detectable half in code: every causal source is configured with `AutoOffsetReset.none()`, so a
consumer whose position falls out of range fails fast rather than silently jumping past destroyed
causes, and the offset seeder seeds log-start positions only on a genuine first start, refusing when
surviving state shows the group is not new (offset expiry is not a first start). Mid-replay retention
expiry is therefore a loud crash-loop until an operator resets, a liveness stall by design, never a
reorder. The preventative half is a retention-sizing constraint on the operator: retention on causal
topics must comfortably exceed the longest consumer outage or replay you intend to survive. See the
[operations note in Streams integration](../guide/streams.md#operating-notes) and
[Troubleshooting](../guide/troubleshooting.md#retention-outran-a-causal-consumer).

## E3 — compliant participants; participation is per-path

Every producer that stamps clocks is assumed to stamp truthfully and transitively completely, and to
preserve received causal history. Producers that stamp nothing are causally minimal *sources*, safe
by construction, no declaration needed. The subtle consequence: **causal order is guaranteed only
along paths where every intermediate processor stamps.** A service that consumes stamped topics and
re-produces unstamped output silently severs the custody chain. Its outputs are causally minimal by
definition, and the severance is undetectable at runtime, because an unstamped record is
indistinguishable from a genuine external event. This is an architectural constraint of the same kind
as E2's retention constraint: place every processor that sits between causally related topics inside
the stamping boundary, a Parsley stage, or a client using the `CausalClock` edge operations.
Byzantine stamping, forged clocks or invented offsets, defeats any causal-broadcast algorithm and is
out of scope.
