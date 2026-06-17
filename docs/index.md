# Parsley

Causal consistency for Kafka — producers that encode a vector clock onto every message,
consumers that deliver multiple topics in causal order, and Kafka Streams topologies built from
causally consistent processors.

## The problem

A consumer reads a price from `prices-0` at offset 27 and, on that basis, produces an order to
`orders`. A downstream consumer of `orders` processes that order while its own `prices` consumer is
only at offset 24 — acting on an order whose causal premise it has not yet seen. This is a
**causal violation**, and it happens under normal Kafka operation: consumer lag on one topic races
ahead of a write to another. Kafka's per-partition ordering does not prevent it; Parsley does.

The guarantee is **causal consistency**: if A causally precedes B, every consumer observes A before
B — stronger than eventual consistency, weaker than linearisability, with a predictable rather than
load-dependent latency cost.

## How it works

Every message carries the producer's **vector clock** as a header. When a message arrives, the
consumer checks whether its **frontier** already satisfies that clock; if so it forwards
immediately, otherwise it holds the message in a **causal buffer** until the frontier catches up.

If the frontier never catches up, a configurable **buffering policy** forwards out-of-order, drops,
or dead-letters the record — reporting a `CausalViolation` in each case.

The library is a single jar with three entry points, sharing a common vocabulary of value types:

| Entry point | Purpose |
|---|---|
| `CausalProducer` | Stamps the current causal context onto every record it sends |
| `CausalConsumer` | A `poll()`-based consumer that delivers records in causal order |
| `CausalProcessorSupplier` | Wraps a Kafka Streams `Processor` behind the causal guarantee |

## Where to go next

- [**Concepts**](concepts.md) — vector clocks, frontiers, the buffer, violation policies
- [**Getting started**](getting-started.md) — dependency setup, producer, and consumer examples
- [**Streams integration**](streams.md) — wrapping a `Processor`, preconditions, recovery
- [**Configuration**](configuration.md) — limits, policies, violation handler, DLQ headers
- [**API reference**](api/index.html) — full Javadoc
