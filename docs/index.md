# Parsley

Parsley is a library that adds causal delivery order to Kafka Streams processors. It builds Kafka
Streams topologies from processors that enforce causal delivery order, and provides edge operations
that stamp and propagate causal dependencies to and from plain Kafka clients.

## The problem

Kafka guarantees ordering only within a single partition. When a system derives decisions from
events across several topics, that guarantee is not enough.

Consider two topics, `prices` and `orders`. A consumer reads a price from `prices-0` at offset 27
and, on the basis of that price, produces a record to `orders`. A downstream consumer of `orders`
processes that order while its own `prices` consumer has only reached offset 24. It is acting on an
order whose causal premise, the price at offset 27, it has not yet seen.

This is a causal violation, and it happens under normal Kafka operation whenever consumer lag on one
topic races ahead of a write to another. The price update at offset 27 happened before the order, so
a causally consistent system would process the price before the order regardless of which topic each
arrived on. Kafka's per-partition ordering does not prevent the violation. Parsley does.

The guarantee Parsley provides is causal delivery order: when the required conditions hold, if
event A causally precedes event B, every Kafka Streams processor that subscribes to both topics
processes A before B. In the consistency hierarchy this sits above eventual consistency and below
linearisability. See [Streams integration](streams.md) for the required conditions.

## How it works

Every record carries the producer's causal dependencies in a header. The dependencies are a snapshot
of the offsets the producer had observed when it sent the record. When a record arrives, the
consumer compares those dependencies against its own frontier, which is the set of offsets it has
already delivered on each partition. If the frontier already satisfies the dependencies, the record
is forwarded immediately. If it does not, the record is held in a causal buffer until the frontier
catches up.

If the frontier never catches up and the configured `CausalBufferLimit` fires, the outcome is
governed by `parsley.buffer.eviction.failure.policy`. By default this policy is `fail`: Parsley fails
the task and leaves the record in the buffer for retry, which preserves causal order at the cost of
availability. Setting the policy to `continue` instead forwards the held record out of causal order
once the limit fires. Either way, the event is logged with the causal gap and counted by a metric.
Register a `CausalAudit` to receive it as a per-record callback. The
[Configuration](configuration.md) page describes the policy in full.

## Public API

The library is a single jar built around one entry point and a set of edge operations. They share a
common vocabulary of value types.

| API | Purpose |
|---|---|
| `CausalProcessorSupplier` | Wraps a Kafka Streams `Processor` behind the causal guarantee. This is the core of the library. |
| `CausalDependencies.using` / `observe` / `stamp` / `merge` | Maintain a consumer-side frontier and stamp causal context onto records produced to plain Kafka clients at the topology edge. |
| `CausalTopics` | Resolves topic names to their stable Kafka UUIDs for building dependencies. |

## Where to go next

- [Concepts](concepts.md) covers causal dependencies, the frontier, the buffer, and how eviction is
  handled.
- [Getting started](getting-started.md) covers installation and stamping causal context at the edge.
- [Streams integration](streams.md) covers wrapping a `Processor`, the preconditions, and recovery.
- [Configuration](configuration.md) covers buffer limits, the eviction and deserialization policies,
  and header size.
- [Audit logging](audit-logging.md) covers routing per-record causal events to your own audit trail.
- [API reference](api/index.html) is the full Javadoc.
