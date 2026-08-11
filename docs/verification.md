# Verification

The suite runs under `./mvnw verify` in roughly four minutes and requires no Docker.

## Layers

**Pure core.** Codec round-trip tests, decision-unit table tests, and engine unit tests. A
purity test scans the `core` sources and fails on any reference to a clock, the network, or
the substrate.

**Simulation.** A simulated substrate and host honouring the host obligations drives many
engines over randomised topologies, interleavings, gaps, aborted-transaction runs, crashes,
restarts and read-position reports. Runs are seeded and deterministic, so a failure
reproduces exactly from its seed. `CausalOrderPropertyTest` sweeps seeds 1 to 300.

An oracle tracks real happened-before outside the engine and asserts causal order, absence of
duplicates, FIFO per channel, and quiescent liveness, meaning everything received is
eventually delivered. Causal order is checked twice: over delivered pairs at the end of a
run, and at the moment of each delivery — every true cause must already be delivered,
settled by evidence the simulated world corroborates, or lie within the delivered past the
engine is entitled to drop behind. The delivery-time check exists for the case the pair
check cannot see: a premature delivery whose cause never delivers, because the delivery
itself advanced the clamp that later drops the cause.

**Sabotage meta-tests.** The same suite runs against deliberately broken engines through a
test-only hook, asserting that the oracle fails. The `Sabotage` modes each disable one
guarantee: the dependency check, the FIFO hold, the duplicate drop on refeed, the treatment
of undecodable metadata, persistence of held messages, truncation handling, and others.

This is the evidence that the tests would catch a violation. `SabotageMetaTest` pins one
targeted case per mode — for the refusal-disarming modes, both that the sabotage disarms the
refusal and a pinned seed on which the oracle catches the resulting violation — and a
randomised sweep records the margin by which the oracle catches each broken engine. One mode
is the exception: `DELIVER_PAST_DEAD_HOLDS` is reached by no random seed (calibrated at 0 in
300), so its oracle evidence is a deterministic scenario constructing the causal inversion,
and it carries no sweep floor.

**Streams wiring.** `TopologyTestDriver` tests for the header format on the wire, byte-exact
key and value pass-through, Schema-Registry-format serdes, and punctuator fact ingestion
through an injected facts source.

**Integration.** Embedded KRaft broker tests for commit and abort behaviour under
exactly-once semantics, restart with state restore, restart after a state-dir wipe with the
ordering state rebuilt entirely from its changelog, migration of a task holding an
undelivered effect between two live instances, a full broker bounce with a held message
neither lost nor freed by the outage, aborted-transaction gaps and trailing
runs, log truncation, and a plain `read_committed` consumer decoding output with application
serdes alone.

## Standard of evidence

`EVIDENCE.md` records, per specification criterion, what would catch a violation of it. Each
cell names the test that fails when the behaviour breaks. A test that stays green when the
behaviour breaks is treated as worse than an empty cell.

There is no mutation-testing gate. `Sabotage` is this project's mutation testing, with the
mutations chosen against specification criteria rather than syntax. `DECISIONS.md` D67
records that decision, and the gaps found before the gate was removed.
