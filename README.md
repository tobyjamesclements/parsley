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

A **protocol core with no Kafka dependency** implements causal delivery with head-of-line
blocking per channel; its host seam exists so the deterministic simulator can drive it, and
Kafka's semantics remain load-bearing throughout. A channel is one partition of one topic, identified by the topic's stable UUID;
each channel has a FIFO hold queue, and only queue heads are gated against the node's
contiguous delivered frontier. A delivery advances the frontier and cascades releases across
channels to fixpoint.

Outbound records carry a single header, `parsley-clock` — a vector clock folded from the
frontier, the per-channel advertised clocks, ancestry carried across scope changes, and the
node's own sends. Stamping happens at one site and never blocks: a node claims its own
in-flight sends in its own send-sequence space (assigned synchronously), and receivers resolve
those claims from the sender tag each record carries. There is no acknowledgement feed
anywhere in the node: broker offset facts (end offsets, log starts) are all it ever asks for.

Kafka's non-density under exactly-once — transaction markers and aborted records occupy
offsets a `read_committed` consumer never returns — is repaired at receive time by seeding and
bridging, and at the trailing edge by **position-advance bridging**: the consumer's position
moving past markers is the protocol's entire liveness mechanism. Business topics carry **no
protocol records**; plain consumers need no Parsley awareness at all.

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

A stage declares sources, an ordinary Streams `Processor`, and sinks; the runtime enforces
`exactly_once_v2` and wires position capture and topic identity:

```java
CausalStage<String, String, String, String> stage =
        CausalStage.<String, String, String, String>builder()
                .source("orders", Serdes.String(), Serdes.String())
                .source("payments", Serdes.String(), Serdes.String())
                .processor(SettlementProcessor::new)
                .sink("settlements", Serdes.String(), Serdes.String())
                .build();

try (CausalStreams app = CausalStreams.start(stage, props)) {
    // records reach SettlementProcessor in causal order; forwards are stamped automatically
}
```

Plain producers stamp with a `Clock` — `observe` consumed records, `recordProduced` your
acknowledgements, `stamp` outbound headers. Adoption is incremental: a producer that stamps
nothing claims nothing, so you stamp the producers whose ordering matters, one at a time,
with no flag day.

Requires Java 21 or later and brokers that serve topic IDs (Kafka 3.7+). Build from source
with `mvn verify`.

## Documentation

The docs site under `docs/` (mkdocs) covers the model and the design:

- **Foundations** — [the causal model](docs/foundations/causal-model.md),
  [the delivery gate](docs/foundations/delivery-gate.md), and
  [liveness without gossip](docs/foundations/liveness.md).
- **Design** — [architecture](docs/design/architecture.md),
  [state and recovery](docs/design/state.md), and
  [verification](docs/design/verification.md).
- **Guide** — [getting started](docs/guide/getting-started.md) and
  [plain clients](docs/guide/clients.md).
- **Reference** — [wire format](docs/reference/wire-format.md).

The verification obligations the test suite enforces are catalogued in
[verification](docs/design/verification.md).

## Current limitations

- One causal stage per Streams topology; the protocol core itself has no such limit.
- Non-Parsley headers on held records are not carried through delivery.
- Sequence claims carry a late-joiner caveat: consumers joining at the log end should
  baseline at the last stable offset (see the liveness page).
- Clock truncation is driven by log-start stability: retention-deleted records sit below
  every reachable baseline, so stamp width is garbage-collected as fast as retention
  advances, with zero coordination. Faster-than-retention truncation would need a membership
  protocol, which is deliberately not included.
- Correctness under a live broker's rebalances and task migration is exercised only by the
  adapter's design, not yet by broker integration tests.
