# Verification

Parsley's primary correctness gate is a deterministic simulator with a ground-truth oracle,
in the test tree of the `parsley` package. Broker integration tests exercise plumbing; the
simulator exercises the protocol, because only a simulator controls interleavings, injects
crashes at chosen points, and knows the real causal history to judge deliveries against.

## The simulated world

`SimBroker` models partitions with real offset occupancy: business records, transaction
markers, and aborted records all consume offsets, and a `read_committed` fetch returns only
business records — so the density adaptation is exercised on essentially every run.
Transactions are step-atomic: one simulation step is one fetch-process-commit cycle, appending
the step's records and markers together, which models EOS faithfully at the granularity that
matters (consumers see marker/abort holes; no transaction spans an observation point).

`SimNode` hosts the real `CausalNode` exactly as the Streams adapter does: staged store
mutations, staged consumer positions, sends stamped at the single site, acknowledgements
delivered asynchronously by the scheduler, and a commit-or-abort at step end. A crash aborts
the in-flight step — appended records become aborted records at their offsets, staged state
discards, and the node later restarts from committed state through the full init path
(restore, end-offset seed, rescope). `EdgeProducer` scripts plain clients: sequential
producers whose sends claim their previous acknowledgements, and observers that fold consumed
records before producing.

The scheduler draws from every enabled action — fetch, position advance, acknowledgement
delivery, producer op, crash, restart — with a seeded generator, so every run is a
reproducible interleaving and every seed a different one.

## The oracle

The oracle never reads a vector clock. It tracks **real** happened-before ancestry: each
business emission records the emitting actor's causal past (everything delivered there plus
its own prior sends, transitively closed), staged and rolled back in mirror with the node's
transaction so an aborted step teaches the oracle nothing. On every delivery it checks:

- **Causal order** — every ancestor on a consumed channel at or above the node's baseline was
  delivered there first.
- **Per-channel FIFO** — delivered offsets strictly increase.
- **No duplicates** — a record is delivered at most once per node (committed).

At drain it checks **completeness**: every business record above baseline on a consumed
channel was delivered. A world that cannot drain within its step budget fails outright — that
is the wedge and livelock detector.

## Obligations

| # | Obligation | Enforced by |
|---|---|---|
| V1 | Causal delivery under random interleavings | Oracle, on every delivery |
| V2 | Per-channel FIFO, no loss, no duplicates | Oracle sequence checks + completeness |
| V3 | Crash/restart preserves V1–V2 | Crash-injecting runs, both idle and mid-transaction |
| V4 | Markers and aborts never wedge the frontier | Marker-dense logs on every run; filter scenarios |
| V5 | Liveness: every world drains, queues empty | Drain budget + completeness at drain |
| V6 | Cycles drain; trailing markers resolve | Damped-feedback and filter scenarios |
| V7 | Truncation below a true stability bound is invisible | Truncate-then-continue scenarios |
| V8 | The oracle catches a broken protocol | `OracleSelfTest`: an eager, dependency-ignoring protocol must be flagged |

V8 is the keystone: it runs a deliberately broken implementation and requires the oracle to
flag it. A harness that cannot fail is not a harness; if V8 ever breaks, every green result is
meaningless.

## Anti-vacuity

Every scenario asserts that the machinery it targets actually fired: records were held at the
gate, crashes were injected, position advances ran. A race that never occurs proves nothing,
so the suite refuses to pass on quiet schedules. Indicatively, the widest scenario across its
hundred seeds drives on the order of ten thousand scheduler steps, five thousand
oracle-checked deliveries, hundreds of crash injections, and a thousand-plus position
advances.

## Scenario coverage

Shared-input races (a stage's output racing its input at a shared consumer), transitive claims
through a hop that does not consume the origin, edge-producer cross-topic ordering, observed
causality from a plain consumer, crash-recovery chains, filter stages with quiet sinks and
trailing markers, multi-partition sink ordering through sequence claims (including with
acknowledgements never delivered), the late-joiner sequence-claim wedge probe, damped feedback
cycles, truncation mid-history, log-start stability (full-retention truncation emptying the
stamp-side clocks, and a from-earliest joiner arriving after truncation), scope shrink across
a restart, and the wide soak combining most of the above.

## Mutation testing

PIT runs over the whole package inside `mvn verify`, at four threads with a 60% floor. Four
threads is measured verdict-identical to a serial run on this suite; the hazard being checked
is that a timeout under CPU contention counts as a kill, so the comparison is repeated
(one `-Dpitest.threads=1` run, diff `mutations.xml`) whenever the suite slows appreciably.
The delivery gate itself has no surviving mutants; the
survivors that remain cluster where randomized schedules structurally under-discriminate —
persistence (killed instead by the directed `CausalNodePersistenceTest`), redundancy-masked
paths (removing acknowledgement folding survives because sequence claims soundly cover it),
and operational transitions like rescope, which dedicated scenarios now drive. One survivor
class is accepted by design: mutants whose only effect is degrading an optimisation the
protocol does not rely on for safety.

## What the simulator does not cover

Real broker behaviour outside the model: rebalances and task migration, consumer-group
protocol edge cases, the adapter's client-supplier wiring under a live
cluster, and timing that depends on actual I/O. Those belong to broker integration tests,
which this cut does not yet include; `TopologyTestDriver` smoke tests cover the adapter's
plumbing (hold-and-release, stamp content, fail-closed corrupt headers) without a broker.
