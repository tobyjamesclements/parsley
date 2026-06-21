# Parsley

Causal consistency for Kafka — producers that encode causal dependencies onto every message,
consumers that deliver multiple topics in causal order, and Kafka Streams topologies built from
causally consistent processors.

## Motivation

As event streaming systems increasingly derive decisions from events across multiple topics, Kafka's per-partition ordering guarantees may not be enough.

Consider a stream processor processing order and discount events from different topics. The processor may process an order event before processing the discount event that was the premise upon which the order was placed. The discount event is said to have happened before the order event. A causally consistent order of events would respect this relationship and delay the processing of the order event until the discount event had been processed.

Parsley provides causal producers, consumers and Kafka Streams processors that automatically track the happens-before relationship between events and deliver them in a causally consistent order, regardless of how many topics are involved.

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

Every message carries the producer's **causal dependencies** as a header. When a message arrives, the
consumer checks whether its **frontier** already satisfies those dependencies; if so it forwards
immediately, otherwise it holds the message in a **causal buffer** until the frontier catches up.

If the frontier never catches up, the configured `CausalBufferLimit` fires and the record is
forwarded anyway, stamped `EVICTED` instead of `SATISFIED`.

The library is a single jar with three entry points, sharing a common vocabulary of value types:

| Entry point | Purpose |
|---|---|
| `CausalProducer` | Stamps the current causal context onto every record it sends |
| `CausalConsumer` | A `poll()`-based consumer that delivers records in causal order |
| `CausalProcessorSupplier` | Wraps a Kafka Streams `Processor` behind the causal guarantee |

## Where to go next

- [**Concepts**](concepts.md) — causal dependencies, frontiers, the buffer, always-forward delivery
- [**Getting started**](getting-started.md) — dependency setup, producer, and consumer examples
- [**Streams integration**](streams.md) — wrapping a `Processor`, preconditions, recovery
- [**Configuration**](configuration.md) — buffer limits, eviction, header size
- [**API reference**](api/index.html) — full Javadoc
