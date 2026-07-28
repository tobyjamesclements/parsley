# Parsley

Causal delivery order for Kafka stream processing: a record reaches your processor only after
every cause your processor consumes has been delivered locally. The guarantee is the
causal-consistency model of the distributed-systems literature — Lamport's happened-before
relation, realised with vector clocks — instantiated on Kafka coordinates: when A causally
precedes B, every processor that subscribes to both of their topics processes A before B.

Causal safety is inviolable. Parsley blocks or fails; it never reorders, drops, or guesses on
a timeout. Joining a running topology needs no coordination: a new application starts
consuming, self-gates into causal order during replay, and its stamps make its outputs
correctly gated everywhere from its first emission.

## How it works

Parsley implements causal delivery with head-of-line blocking per channel, riding Kafka's
own semantics throughout. A channel is one partition of one topic, identified by the topic's stable UUID;
each channel has a FIFO hold queue, and only queue heads are gated against the node's
contiguous delivered frontier. A delivery advances the frontier and cascades releases across
channels to fixpoint.

Outbound records carry their stamp in record headers — a vector clock folded from the
frontier, the per-channel advertised clocks, ancestry carried across scope changes, and the
node's own sends, plus a sender tag. Stamping happens at one site and never blocks: a node
claims its own in-flight sends in its own send-sequence space (assigned synchronously), and
receivers resolve those claims from the sender tag each record carries. Beyond ordinary
fetch, broker offset facts (end offsets, log starts) are all the node ever asks of Kafka.

Kafka's non-density under exactly-once — transaction markers and aborted records occupy
offsets a `read_committed` consumer never returns — is repaired at receive time by seeding and
bridging, and at the trailing edge by **position-advance bridging**: the consumer's position
moving past markers is the protocol's entire liveness mechanism. Parsley's whole on-wire
footprint is the headers on your own records, so plain consumers need no Parsley awareness
at all.

All causal state persists under per-channel keys and commits in the same EOS transaction as
the delivery that mutated it, so crash recovery is a pure restore. Hold queues are unbounded
and disk-backed: a lagging cause channel grows state, never heap, and is bounded by the same
retention economics that bound the causal history itself.

## Verification

Parsley's primary correctness gate is a deterministic simulator with a ground-truth causal
oracle. The simulator models partitions with real marker and aborted offsets, step-atomic EOS
transactions, `read_committed` fetch, node crashes with
transactional rollback, and seeded random interleavings. The oracle tracks real
happened-before ancestry entirely outside the protocol and checks every delivery for causal
order, per-channel FIFO, and duplicates, plus completeness and drain at the end of every run.
Scenarios assert that the machinery under test actually fired — gate holds, crash injections,
position advances — and a self-test proves the oracle flags a deliberately broken protocol.

```
mvn verify
```

## Using it

The API is a functional core with imperative edges. Topics are typed values declared once;
your logic is a pure function from a causally delivered `Message` to `Emission` values —
with per-key state, a pure fold that also returns the next state — testable with plain
equality and no runtime. The runtime enforces `exactly_once_v2` and wires position capture
and topic identity:

```java
Topic<String, Order>   orders      = Topic.of("orders", Codec.utf8(), orderCodec);
Topic<String, Payment> payments    = Topic.of("payments", Codec.utf8(), paymentCodec);
Topic<String, Settled> settlements = Topic.of("settlements", Codec.utf8(), settledCodec);

Stage settlement = Stage.named("settlement")
        .state(balanceCodec, Balance::zero)
        .on(orders,   (bal, m) -> Step.of(bal.minus(m.value().total()),
                                          settlements.send(m.key(), settle(bal, m))))
        .on(payments, (bal, m) -> Step.of(bal.plus(m.value().amount())))
        .into(settlements)
        .build();

try (CausalStreams app = Parsley.of(settlement).streams(props)) {
    app.start();
    // messages reach each handler in causal order; emissions are stamped automatically
}
```

Stages compose into pipelines within one application — a hop is the same `Topic` appearing
as one stage's sink and another's source, an ordinary causal channel:

```java
Parsley.of(enrichment, settlement).streams(props).start();
```

Plain producers stamp with a `CausalClock` — `observe` consumed records, `recordProduced` your
acknowledgements, `stamp` outbound headers. Adoption is incremental: a producer that stamps
nothing claims nothing, so you stamp the producers whose ordering matters, one at a time,
with no flag day.

Requires Java 21 or later and brokers that serve topic IDs (Kafka 3.7+). Build from source
with `mvn verify`.

## Documentation

The docs site under `docs/` (mkdocs) covers the model and the design:

- **Foundations** — [the causal model](docs/foundations/causal-model.md),
  [the delivery gate](docs/foundations/delivery-gate.md), and
  [liveness](docs/foundations/liveness.md).
- **Design** — [architecture](docs/design/architecture.md),
  [state and recovery](docs/design/state.md), and
  [verification](docs/design/verification.md).
- **Guide** — [getting started](docs/guide/getting-started.md) and
  [plain clients](docs/guide/clients.md).
- **Reference** — [wire format](docs/reference/wire-format.md).

The verification obligations the test suite enforces are catalogued in
[verification](docs/design/verification.md).

## Current limitations

- Non-Parsley headers on held records are not carried through delivery.
- Sequence claims carry a late-joiner caveat: consumers joining at the log end should
  baseline at the last stable offset (see the liveness page).
- Vector-clock truncation advances exactly as fast as retention, with zero coordination:
  retention-deleted records sit below every reachable baseline, so stamp width is
  garbage-collected at retention speed — and no faster.
- Correctness under a live broker's rebalances and task migration is exercised only by the
  adapter's design, not yet by broker integration tests.
