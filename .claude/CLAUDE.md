# Parsley

Parsley provides causal delivery order for Kafka stream processing: a record reaches a
processor only after every cause that processor consumes has been delivered locally. Single
Maven module, single package `io.github.tobyjamesclements.parsley`, Java 21, brokers 3.7.0
or newer. `kafka-clients` is the only required runtime dependency; `kafka-streams` is
optional and needed only by the Streams adapter.

## The one rule that overrides everything

Causal safety is inviolable: a record is never delivered before a real cause, and there is
no timeout guessing. When records appear "stuck", the gate is doing its job — a cause is
missing, lagging, or unstamped. Diagnose why the cause has not arrived; never "fix" blocking
by reordering, skipping, or adding a timeout. The delivery invariant is
`docs/foundations/causal-model.md`; liveness (why every wait terminates) is
`docs/foundations/liveness.md`.

## Map

- `docs/foundations/` — the causal model, the delivery gate, liveness. Authoritative for
  what is guaranteed and why.
- `docs/design/` — architecture, state and recovery, verification obligations.
- `docs/guide/` — getting started, topology shapes, codecs, error handling, ticks, the
  application contract, plain clients.
- `docs/reference/wire-format.md` — the on-wire contract (headers `vc`, `vc-sender`,
  `vc-seq`) and state-store layouts. This is the compatibility surface between
  applications; never change it in a vendored or modified copy.
- `src/main/java/.../package-info.java` — codifies the public API tier. The public surface
  is the functional core (`Topic`, `Codec`, `Message`, `Emission`, `Handler`, `Fold`,
  `Step`, `Tick`, `TickHandler`, `TickFold`) plus the imperative edges (`Stage`, `Parsley`,
  `CausalStreams`, `CausalClock`, `CausalHeaders`, `CorruptClockException`); everything
  else is package-private.

## Verifying anything

- `mvn verify` is the full gate: the unit suite, the deterministic simulator with its
  ground-truth causal oracle (`CausalNodeSimTest`, `SimWorld`, `Oracle`), and PIT mutation
  testing. `-DskipPitest=true` skips mutation testing for quick iteration.
- The whole protocol runs without a broker or Docker: `Parsley.testTopology()` under Kafka
  Streams' `TopologyTestDriver`. Use this to demonstrate or test causal gating end to end.
- A broker smoke suite runs under `-Pbroker-it` (needs Docker; Testcontainers).
- Simulator tests are seeded and deterministic; a failure reproduces from its seed.

## Using it from an application

Start from `docs/guide/getting-started.md`. The runtime enforces
`processing.guarantee=exactly_once_v2` and `isolation.level=read_committed`, and must be
started through `Parsley.streams` (it installs the position-capturing client supplier the
protocol's liveness depends on). Emissions may only name declared sinks, and a stage with
ticks needs its tick topic (`vc-<stage>-ticks`) created with the partition count of the
stage's widest source topic. The full application contract is `docs/guide/expectations.md`.

## Vendoring this library

Copying the source into another codebase is permitted (MIT) and workable at this size, with
four rules:

1. Copy verbatim; do not re-synthesize or restyle the protocol classes. `CausalNode`,
   `VectorClock`, and the wire format encode invariants that fail silently when paraphrased.
2. Bring the verification with you: `CausalNodeSimTest`, `SimWorld`, `Oracle`, and the
   protocol tests must run in your CI, or your copy is unverified.
3. Never change the wire format (`docs/reference/wire-format.md`); it is what keeps your
   copy interoperable with every other Parsley application.
4. Record the upstream commit you copied from, so your copy can be diffed against upstream
   when fixes land there.

Renaming the package and classes is fine; no runtime string carries the library name.

## Conventions if you modify the code

- No mock frameworks; hand-rolled test doubles behind the narrow seams (`BrokerOffsets`,
  `StateStore`).
- Every test that builds a Kafka Streams instance takes its `state.dir` from a JUnit
  `@TempDir`; shared directories contend on one RocksDB lock.
- camelCase test method names, Javadoc on every `@Test`, assertion messages.
- Documentation describes the code as it stands; design history stays out of the docs.
