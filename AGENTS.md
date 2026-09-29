# Parsley

Guidance for AI coding agents working on Parsley, or using it from an application. It is
written to be read by any agent or assistant; nothing here is specific to one tool.

Parsley provides causal delivery order for Kafka Streams processors. Kafka orders records within a
topic-partition and orders nothing between partitions. Parsley supplies the missing
cross-channel guarantee: **if message A is a cause of message B, every process that delivers
both delivers A first**, across restarts and for the whole lifetime of a process.

Single Maven module, Java 21, Kafka 4.3.1, one package,
`io.github.tobyjamesclements.parsley`. `kafka-streams`, `kafka-clients` and `slf4j-api` are
its only declared dependencies; everything else on the classpath arrives with Kafka. This
tree is `io.github.tobyjamesclements:parsley:0.4.0-SNAPSHOT`, and the current release is
0.3.0.

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

## Map

Everything lives in one package, `io.github.tobyjamesclements.parsley`. Kafka Streams is
the only host the spec allows (SPEC Substrate 1 and 2), so the declaration surface is
written in Kafka's own terms — topics, Serdes, Streams properties — by design, and there is
no seam for a second runtime.

Thirteen types are public. Ten are the declaration surface: `Parsley`, `ParsleyConfig`,
`Process`, `Topic`, `Store`, `Handler`, `Delivery`, `Effects`, `State` and
`ProcessStatus`. Three more an application handles rather than declares: `Header`,
`FailClosedException`, and `Channel`, the topic-partition by identity that a `Delivery`
names as where it arrived.

The rest is package-private, and divides in two:

- **The protocol.** The causal frontier (`Causes`), its wire codec (`CausesCodec`), the
  hold-back buffer and the pure deliverability decision (`Deliverability.decide`), driven
  by `ProcessEngine` over an `OrderingStore`. It names no clock, no thread and no Kafka
  type, which is what lets the simulator drive the real engine with no broker (SPEC
  Structural 7). Keep it that way.
- **The runtime.** Byte topologies (`ProcessTopology`, `ProcessNode`), topic identity at
  task initialisation (`TopicIdentitySource`, `AdminTopicIdentitySource`), the store over a
  Streams state store (`StreamsOrderingStore`), and the EOS lifecycle (`StreamsRuntime`).

`ProtocolPurityTest` holds the line the package used to. It declares which sources are
protocol and which are runtime, then checks that no protocol source names a host facility,
that none names a runtime type, and that every source is classified — so a file added to
the package is fenced as protocol until someone deliberately says otherwise. When you add a
main source, put it in one of those two lists.

`Sabotage` is package-private on purpose: the public API offers no way to construct an
engine with a mode enabled (SPEC Structural 9). It exists so the suite can prove it catches
each violation class.

## Verifying anything

- `./mvnw verify` is the full gate: **the whole suite, green, roughly eleven minutes** (the
  surefire summary prints the count, 670 after the packages collapsed into one, which added the fence's checks and the two suites
  that exercise the public surface from outside it). It must be green at every commit, and
  it grows. It shrinks only when a mechanism is deleted with its pins, and the record that
  deletes it says so.
- Three layers. Unit tests over the pure protocol. A **simulation harness** driving real engines
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
  at spec criteria rather than syntax.

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

`docs/` carries the fuller version. `ProcessStatus` is the diagnosis surface when a process
has stopped, and `docs/runbooks.md` says what an operator does with that diagnosis, one
runbook per refusal reason. A reason added to `FailClosedException.Reason` needs a runbook
there and a trigger row in `docs/failing-closed.md`; `RunbookCoverageTest` fails until it
has both.

The Javadoc of `main` is the project's published site, at
https://tobyjamesclements.github.io/parsley/. `.github/workflows/javadoc.yml` rebuilds and
redeploys it on every push to `main`. A broken `{@link}` or `@see` fails the build, which
`./mvnw verify` does not check, so CI runs `./mvnw javadoc:javadoc` as its own job on every
push. There is no other site: the Markdown under `docs/` is read in the repository.

## Conventions if you modify the code

- Keep the protocol pure, and classify every main source you add. `ProtocolPurityTest` will
  tell you if you did not.
- No mock frameworks; hand-rolled test doubles behind the narrow seams (`OrderingStore`,
  `TopicIdentitySource`).
- Every test that builds a Kafka Streams instance takes its `state.dir` from a JUnit
  `@TempDir`; shared directories contend on one RocksDB lock.
- camelCase test method names, Javadoc on every `@Test`, assertion messages.
- Record what you decided, and what would catch its failure, in the commit message and in
  the Javadoc of the code that carries it.
