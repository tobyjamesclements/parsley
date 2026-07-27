# Parsley (fresh start)

Causal delivery order for Kafka stream processing: a record reaches your processor only after
every cause your processor consumes has been delivered locally. No membership, no admission
barrier, no timeouts, no reordering — block or fail, never guess.

This branch is a ground-up rewrite. `DESIGN.md` is the specification; the deterministic
simulator under `src/test/java/.../sim` is the primary correctness gate.

## Architecture in five sentences

A **transport-agnostic core** (`parsley.core`) implements causal delivery with head-of-line
blocking per channel: each `(topicId, partition)` channel has a FIFO hold queue, only queue
heads are gated against the contiguous delivered frontier, and delivery cascades to fixpoint.
Outbound records carry one header — a vector clock over channels — folded from the frontier,
the per-channel advertised clocks, carried ancestry from scope changes, and the node's own
acknowledged sends. Kafka's non-density (transaction markers, aborted records, retention) is
repaired at receive time by seeding and bridging, and its trailing edge by **position-advance
bridging**: the consumer's position moving past markers is the entire liveness mechanism, so
there are **no protocol records on any topic**. Persistence is keyed and incremental,
committing in the same EOS transaction as the delivery that mutated it. A Kafka Streams
adapter (`parsley.kafka`) hosts the core behind a single-serialization-point processor;
`EdgeClock` gives plain producers and consumers the same stamps.

## Verification

The simulator models partitions, EOS step-atomic transactions with real marker and aborted
offsets, `read_committed` fetch, asynchronous acknowledgements, node crashes with
transactional rollback, and seeded random interleavings. A **ground-truth oracle** tracks real
happened-before ancestry entirely outside the protocol and checks every delivery for causal
order, per-channel FIFO, and duplicates, plus completeness and drain at end of run. Scenarios
carry anti-vacuity assertions (gate holds, crashes, and position advances must actually fire),
and an oracle self-test proves the harness flags a deliberately broken protocol.

```
mvn verify
```

## Using it

```java
CausalStage<String, String, String, String> stage = CausalStage.<String, String, String, String>builder()
        .source("orders", Serdes.String(), Serdes.String())
        .source("payments", Serdes.String(), Serdes.String())
        .processor(MyProcessor::new)          // an ordinary Processor<K, V, KO, VO>
        .sink("settlements", Serdes.String(), Serdes.String())
        .build();

try (CausalStreams app = CausalStreams.start(stage, props)) {   // enforces exactly_once_v2
    // records reach MyProcessor in causal order; forwards are stamped automatically
}
```

Plain producers stamp with `EdgeClock`: `observe` consumed records, `recordProduced` your
acknowledgements, `stamp` outbound headers. Plain consumers need nothing at all — business
topics carry no protocol records, only a `parsley-clock` header they may ignore.

## What changed from the original architecture

| Original | Fresh start |
|---|---|
| Out-of-order intra-partition delivery: two delivered-vector projections, forwarded index, absorb walk, candidate index | Head-of-line blocking per channel: one frontier, a FIFO queue per channel, gate the head only |
| Gossip layer: in-band null messages, relay rule, trigger timestamps, retention coupling | Deleted — position-advance bridging plus claims-name-real-offsets cover liveness; topics are protocol-silent |
| One persisted frontier blob, rewritten O(C·w) per advance | Keyed incremental persistence; only changed entries written |
| Persisted own-outputs clock plus former-sink heal machinery | Memory-only own outputs; the init end-offset seed re-covers every restart case |
| Monotone-growing stamps forever | `truncate(stability)` hook drops entries below a global stability bound |
| Unbounded hold buffer | `pauseWanted` backpressure: pause the flooding channel, causes arrive on other channels |
| Broker integration tests as the correctness gate | Deterministic simulator with a ground-truth causal oracle, crashes and EOS faults included |

Known limits of this cut: non-Parsley headers on held records are not carried through
delivery; one causal stage per topology in the Streams adapter (the core itself has no such
limit); the Streams adapter's crossing wait conservatively awaits all pending sends.
