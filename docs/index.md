# Parsley

Parsley provides causal delivery order for Kafka stream processing: a record reaches your
processor only after every cause your processor consumes has been delivered locally. The
guarantee is the causal-consistency model of the distributed-systems literature — Lamport's
happened-before relation, realised with vector clocks — instantiated on Kafka coordinates.
When A causally precedes B, every processor that subscribes to both of their topics processes
A before B.

Causal safety is inviolable: Parsley blocks or fails, and never reorders, drops, or guesses on
a timeout. Joining a running topology needs no coordination — a new application starts
consuming, self-gates into causal order during replay, and its stamps make its outputs
correctly gated everywhere from its first emission.

## Shape of the system

Parsley is a small public API — pure user logic over typed topics, run by a Kafka Streams
adapter — around a package-private causal delivery protocol. The protocol implementation
carries no compile-time Kafka dependency, but it is not transport-agnostic — Kafka's
semantics (broker-assigned offsets, transactional visibility, consumer positions) are
load-bearing throughout. That freedom exists so the deterministic simulator can host the
protocol, not so other transports can.

The protocol ([architecture](design/architecture.md)) gates delivery with head-of-line blocking
per channel: each `(topicId, partition)` channel has a FIFO hold queue, only queue heads are
evaluated against the contiguous delivered frontier, and a delivery cascades releases to
fixpoint. Outbound records carry a single header — a vector clock over channels — computed at
one stamping site. All causal state persists under per-channel keys in the same transaction as
the delivery that mutated it.

Parsley's entire on-wire footprint is record headers on business topics. Liveness rides the
consumer's own position advance past transaction markers — see
[liveness](foundations/liveness.md), which develops why every held record is eventually
released.

The adapter ([getting started](guide/getting-started.md)) hosts the protocol and runs user
logic as pure functions — a `Message` in, `Emission` values out — with one serialization
point on each side, and enforces `exactly_once_v2`. Plain producers and consumers use the same stamps through the
[plain-client ops](guide/clients.md).

## How it is verified

The primary correctness gate is a deterministic simulator with a ground-truth causal oracle —
not broker integration tests. The simulator models EOS transactions, marker and aborted
offsets, asynchronous acknowledgements, crashes with transactional rollback, and seeded random
interleavings; the oracle tracks real happened-before ancestry outside the protocol and checks
every delivery. [Verification](design/verification.md) states the obligations and how each is
enforced, including the self-test that proves the oracle catches a broken protocol.

## Reading order

1. [The causal model](foundations/causal-model.md) — what is promised, and against what fault
   model.
2. [The delivery gate](foundations/delivery-gate.md) — the predicate, its normalisation, and
   why ignoring unconsumed coordinates is sound.
3. [Liveness](foundations/liveness.md) — why every held record is released and nothing
   deadlocks.
4. [Architecture](design/architecture.md) and [state](design/state.md) — the classes, the
   store layout, crash recovery, scope changes, truncation.
5. [Verification](design/verification.md) — the simulator, the oracle, and the obligations.
