# Parsley

Guidance for AI coding agents working on Parsley, or using it from an application. It is
written to be read by any agent or assistant; nothing here is specific to one tool.

Parsley provides causal delivery order for Kafka Streams processors. Kafka orders records within a
topic-partition and orders nothing between partitions. Parsley supplies the missing
cross-channel guarantee: **if message A is a cause of message B, every process that delivers
both delivers A first**, across restarts and for the whole lifetime of a process.

Single Maven module, Java 21, Kafka 4.3.1, packages under
`io.github.tobyjamesclements.parsley`. `kafka-streams`, `kafka-clients` and `slf4j-api` are
its only declared dependencies; everything else on the classpath arrives with Kafka. This
tree is `io.github.tobyjamesclements:parsley:0.3.0-SNAPSHOT`, and the current release is
0.2.0.

> **This tree is a from-spec reimplementation.** Its API shares no type with 0.1.0.
> `Stage`, `CausalStreams`, `Fold`, `Tick` and `Codec` no longer exist, and the wire format
> differs. Do not carry 0.1.0 examples, docs or assumptions into it. The `pre-rewrite` tag
> marks the last commit of the previous implementation.

## The one rule that overrides everything

Causal safety is inviolable: a message is never delivered before a real cause, and there is
no timeout guessing. When messages appear "stuck", the gate is doing its job: a cause is
missing, lagging, or unstamped. Diagnose why the cause has not arrived; never "fix" blocking
by reordering, skipping, or adding a timeout. Where the guarantee cannot be upheld, Parsley
**fails closed**: it stops delivering, and stays down until an operator intervenes.

## Authority, in this order

1. `SPEC.md`, the complete specification, and the sole authority on correctness. Criteria
   are cited throughout the code and docs as e.g. "Safety 9", "Structural 13",
   "Operational 4". Treat it as read-only.
2. `docs/wire-format.md`, the **frozen** wire format of the causal metadata. Any change to the
   grammar needs a new version byte and a documented migration; prefer no change.
3. `docs/model.md`, how the pieces satisfy the spec, and why.

Code and docs cite decision records as "D<n>" (D115, D67 and so on). The decision log those
numbers index was removed from the tree; the records are in the git history of
`DECISIONS.md` before its deletion, and the numbers are kept in the citations as pointers
into it.

## Map

- `…/parsley/core`, the host-independent protocol: the causal frontier (`Causes`), its wire
  codec (`CausesCodec`), the hold-back buffer and the pure deliverability decision
  (`Deliverability.decide`), driven by `ProcessEngine` over an `OrderingStore`. This package
  names no host type, and `CorePurityTest` enforces it by scanning the directory: no clock,
  no network, no Kafka (SPEC Structural 9). Keep it that way.
- `…/parsley/api`, the public, statically-typed declaration surface: `Parsley`,
  `ParsleyConfig`, `Process`, `Channel`, `Store`, `Handler`, `Delivery`,
  `Effects`, `State` and `ProcessStatus`.
- `…/parsley/kafka`, the Kafka Streams adapter: byte topologies (`ProcessTopology`,
  `ProcessNode`), topic identity at task initialisation (`TopicIdentitySource`,
  `AdminTopicIdentitySource`), the store over a Streams state store
  (`StreamsOrderingStore`), and the EOS lifecycle (`StreamsRuntime`).

`Sabotage` lives in `core` but is package-private on purpose: the public API offers no way
to construct an engine with a mode enabled (SPEC Structural 9). It exists so the suite can
prove it catches each violation class.

## Verifying anything

- `./mvnw verify` is the full gate: **the whole suite, green, roughly five minutes** (the
  surefire summary prints the count; it was 716 at D113 and 698 at D115, after the facts
  round's suites went with the round). It must be green at every commit, and it grows. It
  shrinks only when a mechanism is deleted with its pins, and the record that deletes it
  says so.
- Three layers. Unit tests over the pure core. A **simulation harness** driving real engines
  under a simulated host that honours the spec's Host obligations, over randomised topologies,
  interleavings, gaps from aborted transactions, crashes, restarts and offset rewinds,
  checked against a happened-before `Oracle` maintained outside the engine. And integration tests
  against an **embedded KRaft broker** (real EOS, real aborted transactions, real
  truncation). No Docker required.
- The suite also runs against deliberately sabotaged engines and asserts it catches each
  violation class (`SabotageMetaTest`, and a randomised sweep with a measured margin). That
  is the evidence the tests would fail if the behaviour broke.
- Simulator runs are seeded and deterministic: `CausalOrderPropertyTest` sweeps seeds 1 to 300,
  and a failure reproduces exactly from its seed.
- There is **no mutation-testing gate**; `Sabotage` is this project's mutation testing, aimed
  at spec criteria rather than syntax. D67 records why, and the three gaps a pitest run found
  before it was removed.

## Using it from an application

Each declared process runs as its own Kafka Streams application under `exactly_once_v2` with
`read_committed`; none of the safety-bearing configuration can be overridden. The seam hands
application logic exactly the delivered message and its application state, and accepts
effects only through the returned value: no timers, no producer, no clock.

```java
var shipper = Process.named("shipper")
    .receives(orders, (delivery, state) -> Effects.builder()
        .put(inventory, delivery.key(), remaining)
        .send(shipments, delivery.key(), Shipment.of(delivery.value()))
        .build())
    .sends(shipments)
    .stores(inventory)
    .build();

try (Parsley parsley = Parsley.start(config, shipper)) {
    parsley.awaitStopped(); // returns when a process stops; a process that fails closed stays down
    parsley.status().forEach((name, status) -> log.info("{}: {}", name, status));
}
```

`Parsley.start` returns once each process has been started, not once it is running; the
wait is what keeps the application up, and `status()` afterwards says what stopped and why.

`docs/` carries the fuller version. `ProcessStatus` and `OrderingStateInspector` are the
diagnosis surface when a process has stopped or is holding, and `docs/runbooks.md` says what
an operator does with that diagnosis, one runbook per refusal reason. A reason added to
`FailClosedException.Reason` needs a runbook there and a trigger row in
`docs/failing-closed.md`; `RunbookCoverageTest` fails until it has both.

## Conventions if you modify the code

- Keep `core` pure. `CorePurityTest` will tell you if you did not.
- No mock frameworks; hand-rolled test doubles behind the narrow seams (`OrderingStore`,
  `TopicIdentitySource`).
- Every test that builds a Kafka Streams instance takes its `state.dir` from a JUnit
  `@TempDir`; shared directories contend on one RocksDB lock.
- camelCase test method names, Javadoc on every `@Test`, assertion messages.
- Record what you decided, and what would catch its failure, in the commit message and in
  the Javadoc of the code that carries it.
