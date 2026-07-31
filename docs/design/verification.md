# Verification

Parsley's primary correctness gate is a deterministic simulator with a ground-truth oracle,
in the test tree of the `parsley` package. Broker integration tests exercise plumbing; the
simulator exercises the protocol, because only a simulator controls interleavings, injects
crashes at chosen points, and knows the real causal history to judge deliveries against.

The simulator and oracle also ship, as the
[conformance kit](../reference/conformance-kit.md) — the `test-jar` artifact of the library's
coordinates — so a vendored copy or an alternative host can run this same verification
outside this repository.

## The simulated world

`SimBroker` models partitions with real offset occupancy: business records, transaction
markers, and aborted records all consume offsets, and a `read_committed` fetch returns only
business records — so the density adaptation is exercised on essentially every run.
Transactions are step-atomic: one simulation step is one fetch-process-commit cycle, appending
the step's records and markers together, which models EOS faithfully at the granularity that
matters (consumers see marker/abort holes; no transaction spans an observation point).

`SimNode` hosts the real `CausalNode` exactly as the Streams adapter does: staged store
mutations, staged consumer positions, sends stamped at the single site, and a commit-or-abort
at step end. Nothing hands a send's offset back to the node that made it, because the adapter
does not fold producer acknowledgements either. A crash aborts
the in-flight step — appended records become aborted records at their offsets, staged state
discards, and the node later restarts from committed state through the full init path
(restore, end-offset seed, rescope). `EdgeProducer` scripts plain clients: sequential
producers whose sends claim their previous acknowledgements, and observers that fold consumed
records before producing.

The scheduler draws from every enabled action — fetch, position advance, tick emission,
producer op, crash, restart — with a seeded generator, so every run is a reproducible
interleaving and every seed a different one.

Reproducibility is a property the protocol has to cooperate with, not one the scheduler can
supply alone. `NodeConfig` normalises its channel sets with `Set.copyOf`, and the JDK's
immutable sets randomise iteration order per JVM; iterating one directly would make the
cascade's cross-channel delivery order vary between runs, and the host runs user logic per
delivery in list order, so that order reaches the broker as send order and from there into
every offset downstream. `CausalNode` therefore iterates a sorted copy wherever set order can
escape — the cascade, `rescope`, and the scope record — and `CausalNodeDirectedTest` pins
both. Without it a seed does not determine its run, and PIT verdicts drift between runs of an
unchanged tree.

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

The oracle judges deliveries by **coordinate**, never by content: a delivery is the record at
`(channel, offset)`, and what the protocol hands the user processor at that coordinate is
outside its verdict. Nothing the simulator does can catch a restore that returns the right
record with the wrong bytes, which is why the payload of a held record is pinned by directed
assertions instead (V10).

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
| V9 | The kit catches a broken host | `HostContractSelfTest`: a host that breaks step-atomicity, or withholds the position-advance signal, must be flagged |
| V10 | A restored record is the record that was fetched | `CausalNodePersistenceTest`: key bytes, value bytes, and timestamp survive the hold-queue round trip — the one fact about a delivery the oracle does not judge |
| V11 | The guards fail closed rather than guess | `CausalNodeFailClosedTest`, one construction per guard |

V8 is the keystone: it runs a deliberately broken implementation and requires the oracle to
flag it. A harness that cannot fail is not a harness; if V8 ever breaks, every green result is
meaningless. V9 is its host-side analogue: `SimNode` can shirk one host duty at a time
(`HostFault`), and the suite requires the shirked duty to be caught — which is what makes the
harness meaningful as a [conformance kit](../reference/conformance-kit.md) for hosts other
than the shipped adapter.

## Anti-vacuity

Every scenario asserts that the machinery it targets actually fired: records were held at the
gate, crashes were injected, position advances ran, a crash landed while records were held. A
race that never occurs proves nothing, so the suite refuses to pass on quiet schedules.
Indicatively, the widest scenario across its hundred seeds drives on the order of ten thousand
scheduler steps, five thousand oracle-checked deliveries, hundreds of crash injections, and a
thousand-plus position advances.

These assertions count **seeds, not events**. A sum over a hundred seeds passes when one seed
happened to be interesting, which is the vacuity they exist to refuse; the floor is a minimum
number of seeds that reached the state (`SEED_FLOOR`, currently twenty against measured rates
of forty to seventy). The distinction is not academic — the sums were what hid the fact that
the hold-queue restore path ran on five node incarnations out of nearly seven thousand.

Two interleavings the scheduler will not reach on its own are bought explicitly, because an
unweighted draw spends its budget before the interesting state exists:

- **A crash while records are held**, the only route into the hold-queue restore path. The
  last crash of a world's budget is reserved for a node that is actually holding, and a
  holding node's crash actions are drawn at double weight. Every interleaving an unweighted
  draw could produce stays reachable.
- **A consumer whose baseline sits above a channel's earlier history**, which is what makes a
  stamp's claims load-bearing rather than redundant with per-channel FIFO. Scenarios that
  target a particular stamp term retire the earlier history with retention, or join the
  consumer after it, so the term under test is the only thing that can order the delivery.

## Scenario coverage

Ticking stages (the tick channel is both consumed and produced, so a tick is stamped, lands on
the emitting task's own partition, and returns through the gate carrying the node's causal past
— including under crash injection, and across a restart that drops one of the stage's other
sinks while the tick self-loop still holds claims on it),
shared-input races (a stage's output racing its input at a shared consumer), transitive claims
through a hop that does not consume the origin, edge-producer cross-topic ordering, observed
causality from a plain consumer, crash-recovery chains, filter stages with quiet sinks and
trailing markers, multi-partition sink ordering through sequence claims, which a node's own
sends are always claimed in until a restart's end-offset seed converts them to offsets — the
simulator never delivers a producer acknowledgement, because neither the protocol nor the
Streams adapter folds one. The late-joiner sequence-claim wedge probe, damped feedback
cycles (with a consumer of both sides of the loop, since the loop's own nodes cannot gate each
other), truncation mid-history, log-start stability (full-retention truncation emptying the
stamp-side clocks, and a from-earliest joiner arriving after truncation), scope shrink across
a restart, a restart's own prior-incarnation sends ordered against each other at a consumer of
both sinks, and the wide soak combining most of the above.

The scenarios covering the stamp's four terms are each built so that the term is what orders
the delivery: the shrink scenario retires the dropped input's channel history from t2 before
its consumer joins (leaving carried ancestry as the only claim on the t1b past), and the
restart scenario joins its consumer after the restart (so the two sinks race, and only the
end-offset seed relates them). Both were rebuilt after the terms were found to be removable
with the suite green — the earlier shapes let per-channel FIFO supply the ordering for free.

## Mutation testing

PIT runs over the whole package inside `mvn verify`, serially, with a 75% mutation floor and
an 80% line-coverage floor — both well below the standing scores (**681 of 738 mutations
killed, 92%; line coverage 1345/1465, 92%; test strength 95%**) so refactoring noise does not
flake the build.

The gate runs on one thread deliberately. The only timing-sensitive verdict PIT produces is
`TIMED_OUT`, which scores as a kill, so a threaded run under CPU contention can inflate the
score. One thread removes that hazard rather than periodically re-checking for it, and costs
little: 506s serial against 402s at four threads. Verdicts are reproducible — five
consecutive runs (four threaded, one serial) returned identical verdicts for all 738 mutants,
compared as a multiset. That reproducibility depends on the iteration-order guarantee
described under [the simulated world](#the-simulated-world); before it, runs of an unchanged
tree disagreed.

**The delivery gate itself has no surviving mutants.** All eight mutants of `deliverable`,
the predicate every guarantee rests on, are killed.

The eight `TIMED_OUT` mutants are all structural non-termination rather than slow tests: six
in the cascade's fixpoint, plus one each in `advanceFrontier` and `frontierOf`. Each stops the
frontier advancing while leaving the loop's `changed` flag set, so the fixpoint never settles.
They are the same eight on every run.

Of the 33 survivors, 20 sit in `CausalNode`, and every one carries a written equivalence
argument rather than an assumption:

- **Redundancy-masked paths**, the largest group. The density adaptation reaches the same
  frontier fold from two sites — a receive-time bridge and the cascade's own trailing-run
  fold, both fed by the same recorded position — so removing either alone is invisible while
  the other stands. These are not gaps in the scenarios; they are places where the protocol is
  deliberately belt-and-braces, and a mutation that removes one brace changes nothing.
- **Stamp normalisation of a node's own echoed claims**, provably equivalent: `prepareSend`
  merges both `ownOutputs` and carried ancestry into the stamp *before* `normalize` runs, and
  the upgrade target is drawn from exactly those two clocks, so upgrading a self-claim to
  offset space can never add anything the stamp does not already carry. Normalisation of
  *another* sender's claim is not equivalent, and is killed.
- **Equivalent mutants**, accepted: the length guards in the held-record envelope
  (`if (keyLen > 0) put(key)` versus `>= 0` — writing a zero-length array is a no-op), the
  half-open bound of the hold-queue restore window (the store never holds an entry at
  `tailIndex`), and mutants whose only effect is degrading an optimisation the protocol does
  not rely on for safety.

The remaining 13 survivors sit outside the protocol, in the adapter and the probes, and need a
seam that does not exist: a started runtime, a log appender, a multi-partition sink
`TopologyTestDriver` cannot provide, or a task migration.

Uncovered mutants (24) are led by the Admin-backed seam (`AdminBrokerOffsets` and the admin
resolver in `TopicIds`, 18 between them), which runs only against a real cluster and so falls
outside PIT's measurement: the seam stays thin, the logic behind it is driven through
`BrokerOffsets` by the simulator, and the broker smoke suite exercises it against a live
broker. The remaining six are `CausalStreams` and `Parsley` members reachable only from a
started runtime, and one branch each in `Stage$Adapter`, `ContractProbes`, and
`KafkaStateStore`. None is in the protocol core.

Two of the surviving mutants are findings about `src/main` rather than about the tests, and
are recorded as such: the tick-topic branch of `ContractProbes.runTopology` cannot produce a
finding under any input, and the `0xFF` carry loop in `KafkaStateStore.forEachPrefix` is
unreachable because store keys are UTF-8 strings and UTF-8 never emits that byte.

## The broker smoke suite

The `broker-it` Maven profile runs the `*IT` test classes under failsafe against a real
single-node broker in a Docker container (Testcontainers), pinned to the minimum supported
broker version so the stated floor is the one actually tested. It carries the three
obligations only a live cluster can discharge:

- **The broker side of the seam contract.** `AdminBrokerOffsetsIT` asserts that a real
  cluster answers the two `BrokerOffsets` queries as the protocol assumes: end offsets per
  partition with empty partitions included, log starts advanced by record deletion, and
  definitive absence by topic ID that survives recreating a topic under the same name.
- **The assembled runtime on a live cluster.** `ParsleyStreamsIT` runs `Parsley.streams`
  end to end under exactly-once: records flow from source to sink carrying parseable clock,
  sender, and sequence headers on the wire, and a restart of the same application on the
  same state directory resumes without loss or duplication.
- **A task migrating between two live instances.** `ParsleyStreamsIT` runs two instances of
  one application over a two-partition source and drives a task across two rebalances — one
  when the second instance joins, one when the first stops cleanly. Each partition's records
  arrive exactly once in produced order, and each task's stamps carry the same sender
  identity on its new host as on its old, since identity is derived from the application id,
  the stage name, and the partition rather than from the incarnation. The rebalances are
  asserted rather than assumed: each phase waits on the instances' reported active-task
  counts, so a run that silently never migrated fails at the wait instead of passing as a
  single-instance run.

The suite is a smoke test of plumbing, not a correctness gate: a real broker offers no
control over interleavings, so every causal-order obligation stays with the simulator. It
sits outside the default `mvn clean verify` gate (it needs Docker) and outside PIT's
measurement, and CI runs it as a separate workflow whose failure does not block a snapshot
publish.

## What the simulator does not cover

Real broker behaviour outside the model: rebalances, consumer-group protocol edge cases, and
timing that depends on actual I/O. The broker smoke suite covers the seam contract, the
assembled plumbing, and a clean task migration on a live cluster; `TopologyTestDriver` smoke
tests cover the adapter's plumbing (hold-and-release, stamp content, fail-closed corrupt
headers, the buffered-batch position guard) without a broker.

Task migration is covered only in its benign shape: a task moving between two live instances
across a clean join and a clean stop, with state small enough that the joining instance is
inside `acceptable.recovery.lag` and takes the task on the first rebalance. Migration away
from an instance that dies mid-transaction, standby warm-up and the probing rebalances that
move a task with large state, and migration under sustained load all remain uncovered.

Two further boundaries are worth naming, because a scenario list reads as a coverage claim:

- **Ticks are modelled at the protocol level, not the adapter's.** The simulator drives the
  self-loop that matters — stamp, own-partition append, return through the gate — with the
  scheduler standing in for the wall clock, which is strictly more adversarial than a real
  timer. The punctuator scheduling, the partition-encoded key, and the sink partitioner stay
  `TopologyTestDriver`-only, as plumbing rather than protocol.
- **Some shapes are pinned by directed tests rather than reached by random schedules.** A
  skipped run behind a held head needs a claim naming an offset inside that specific gap; the
  randomized topologies reach it only by coincidence, so `CausalNodeDirectedTest` constructs it
  deterministically instead. The drain check would catch a regression in any world that happens
  to build the shape, but the directed test is what guarantees it is built at all.
