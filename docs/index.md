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

The causal buffer is unbounded — there is no configuration that trades causal order for liveness. A
record whose dependencies are proven impossible to satisfy (an undecodable payload or dependencies
header, or a dependency naming a coordinate this node has no channel for) unconditionally fails the
task rather than being delivered out of order. The [Troubleshooting](troubleshooting.md) page covers
recovery, and [Configuration](configuration.md) covers the resulting metrics.

## Public API

The library is a single jar built around one topology-level entry point and a set of edge operations.
They share a common vocabulary of value types. Everything else — the processor decorator, the buffer,
graceful-shutdown quiesce — is internal machinery `CausalStreams` composes for you;
there is no low-level public entry point to build a topology around by hand.

| API | Purpose |
|---|---|
| `CausalStreamsBuilder` / `CausalTopology` | Declare a causal topology — one or more stages, each a set of source topics feeding a processor and forwarding to sink(s) — the same way `StreamsBuilder`/`Topology` declare a plain Kafka Streams one. |
| `CausalStreams` | The runtime: wraps the underlying `KafkaStreams` instance around the causal guarantee. Owns graceful causal drain on `close()`. |
| `CausalClock.using` / `observe` / `stamp` / `merge` | Maintain a consumer-side frontier and stamp causal context onto records produced to plain Kafka clients at the topology edge. Topic names are resolved to their stable Kafka UUIDs internally. |

## Where to go next

- [Concepts](concepts.md) covers causal dependencies, the frontier, and the buffer.
- [Getting started](getting-started.md) covers installation and stamping causal context at the edge.
- [Streams integration](streams.md) covers building a topology with `CausalStreamsBuilder`, the
  preconditions, and recovery.
- [Configuration](configuration.md) covers the remaining `parsley.*` keys, header size, and metrics.
- [API reference](api/index.html) is the full Javadoc.
