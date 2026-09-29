# Verification

The suite runs under `./mvnw verify` in roughly eleven minutes and requires no Docker.

## Layers

**Pure protocol.** Codec round-trip tests, decision-unit table tests, and engine unit
tests. `ProtocolPurityTest` fails on any reference to a clock, randomness, the network or
the substrate in a protocol source, on a protocol source naming a Kafka Streams runtime
type, and on a main source belonging to neither list.

**Simulation.** A simulated substrate and host honouring the host obligations drives many
engines over randomised topologies, interleavings, gaps, aborted-transaction runs, crashes
and restarts, with start positions reported at each execution start. The simulated host
refuses a fetch below the log start — the process fails closed `POSITIONS_DISCARDED_UNREAD`,
as under `auto.offset.reset=none` — and re-initialises the receivers of a topic that is
killed or recreated, so the identity report runs. Runs are seeded and deterministic, so a
failure reproduces exactly from its seed. `CausalOrderPropertyTest` sweeps seeds 1 to 300.

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
of undecodable metadata, persistence of held messages, and others.

This is the evidence that the tests would catch a violation. `SabotageMetaTest` pins one
targeted case per mode — for the refusal-disarming modes, both that the sabotage disarms the
refusal and a pinned seed on which the oracle catches the resulting violation — and a
randomised sweep records the margin by which the oracle catches each broken engine. One mode
is the exception: `DELIVER_PAST_DEAD_HOLDS` is reached by no random seed (calibrated at 0 in
300), so its oracle evidence is a deterministic scenario constructing the causal inversion,
and it carries no sweep floor. A host fault rather than an engine mode,
`RESET_PAST_LOG_START`, models an `auto.offset.reset=earliest` host resetting past discarded
positions, and the sweep runs it against the honest engine with its own floor. Its catch is
Safety 8 judged from world truth the host cannot launder — a committed record the process
owed between where it first read the channel and where it committed reading to, never fed to
it — or, where a message depending on a skipped record is later delivered, the delivery-time
causal-order check. A recreation is judged the same way, at the moment a process commits a
step while receiving a dead incarnation whose name is bound to a live other id.

**Streams wiring.** `TopologyTestDriver` tests for the header format on the wire, byte-exact
key and value pass-through, Schema-Registry-format serdes, the identity report at task
initialisation through an injected identity source, and the punctuation.

**Integration.** Embedded KRaft broker tests for commit and abort behaviour under
exactly-once semantics, restart with state restore, restart after a state-dir wipe with the
ordering state rebuilt entirely from its changelog, migration of a task holding an
undelivered effect between two live instances, a full broker bounce with a held message
neither lost nor freed by the outage, aborted-transaction gaps and a cause naming an aborted
position held and visible until a later record settles it, log truncation refused at the
fetch, a held message retention discarded delivering from the changelog, and a plain
`read_committed` consumer decoding output with application serdes alone.

## Standard of evidence

Each test is meant to fail when the behaviour it pins breaks. A test that stays green when
the behaviour breaks is treated as worse than no test.

There is no mutation-testing gate. `Sabotage` is this project's mutation testing, with the
mutations chosen against specification criteria rather than syntax.
