# Parsley

Parsley provides causal delivery order for Kafka Streams processors. Producers stamp each record
with its causal dependencies, and Parsley holds back any record whose dependencies a consumer has
not yet observed, releasing it once the consumer's frontier catches up. The guarantee is the one
from the distributed-systems literature: if event A causally precedes event B, every processor
that subscribes to both of their topics processes A before B.

This site is organised around that guarantee. [Foundations](foundations/causal-consistency.md)
develops the theory, [The three protocols](protocols/index.md) describes the layered algorithm
that delivers it, and [Using Parsley](guide/getting-started.md) covers building and operating a
topology.

## The problem

Kafka orders records only within a single partition. A system that derives decisions from events
across several topics is not protected by that guarantee.

Consider two topics, `prices` and `orders`. A consumer reads a price from `prices-0` at offset 27
and, on the basis of that price, writes a record to `orders`. A downstream consumer of `orders`
processes that order while its own `prices` consumer has only reached offset 24. It is acting on an
order whose cause, the price at offset 27, it has not yet seen.

In Lamport's terms the price update happened before the order, so a causally consistent system
processes the price first regardless of which topic each record arrived on. The violation happens
under ordinary Kafka operation whenever consumer lag on one topic races ahead of a write to
another. Per-partition ordering does not prevent it. Parsley does.

## Where the guarantee sits

Causal consistency is the strongest model that can be maintained without coordination. It sits
above eventual consistency, which constrains no order, and below linearisability, which orders
every event through a single global timeline. Parsley delivers causal consistency for the specific
setting of Kafka Streams: the ordering unit is a `(topic, partition)` coordinate rather than a
process, and dependencies are carried as broker offsets.
[Foundations](foundations/causal-consistency.md) develops Lamport's happened-before relation,
vector clocks, and this instantiation in full.

## Why Kafka Streams needs a protocol

Classical causal-broadcast algorithms rest on two assumptions, and Kafka stream processing
satisfies neither.

- **Total visibility.** Every node that must enforce causal order receives every message. A Kafka
  consumer subscribes to a subset of topics and partitions, so a dependency clock routinely names
  coordinates the consumer has no channel for.
- **Reliable FIFO channels with stable identity.** Vector-clock protocols assume dense, gap-free,
  permanently-identified channels. Kafka partitions rebind identity when a topic is recreated,
  expose offsets that commit markers and aborted records occupy but a consumer never returns,
  truncate history under retention, and, in Parsley, deliver out of order within a partition.

Parsley restores both assumptions and delivers under them, in three layers.

## The three protocols

The implementation is a stack of three protocols, each presented in the module style of Cachin,
Guerraoui, and Rodrigues: requests in, indications out, properties guaranteed. Each layer consumes
the guarantees of the one below it and offers a clean assumption set to the one above.

```
┌──────────────────────────────────────────────────────────────────┐
│ gossip              clock dissemination / liveness                │
│   Chandy–Misra–Bryant null messages, Demers epidemic spread       │
├──────────────────────────────────────────────────────────────────┤
│ causal broadcast    receive / deliver in causal order             │
│   Birman–Schiper–Stephenson CBCAST                                │
├──────────────────────────────────────────────────────────────────┤
│ channels            Kafka partition → reliable FIFO channel       │
│   Hadzilacos–Toueg reliable channels                              │
└──────────────────────────────────────────────────────────────────┘
```

- **[Channels](protocols/channels.md)** repairs the transport assumption. UUID-keyed coordinates,
  seeding and bridging for density, the contiguous frontier, and own-output tracking turn Kafka
  topic-partitions into the reliable FIFO channels the layer above assumes.
- **[Causal broadcast](protocols/causal-broadcast.md)** is the core guarantee: the
  Birman–Schiper–Stephenson delivery gate, the unbounded hold-back buffer, the release cascade, and
  the single stamping site. It repairs the visibility assumption not by restoring total visibility
  but by making the delivery predicate sound without it.
- **[Gossip](protocols/gossip.md)** keeps causal progress observable through processors that emit
  no business output, using protocol null messages, and quiesces on any topology shape including
  cycles.

The [protocols overview](protocols/index.md) gives the module boxes, the class map, and the cost
model of the stack.

## The public surface

Parsley is a single jar built around one topology-level entry point and a set of edge operations
over a shared vocabulary of value types.

| API | Purpose |
|---|---|
| `CausalStreamsBuilder` / `CausalTopology` | Declare a causal topology: one stage of source topics feeding a processor and forwarding to one or more sinks, the way `StreamsBuilder`/`Topology` declare a plain Kafka Streams one. |
| `CausalStreams` | The runtime: wraps the underlying `KafkaStreams` instance and owns graceful causal drain on `close()`. |
| `CausalClock` | Maintain a consumer-side frontier and stamp causal context onto records produced to plain Kafka clients at the topology edge. Topic names resolve to their stable Kafka UUIDs internally. |
| `CausalDeliveryException` hierarchy | The typed exceptions Parsley's fail-closed protocol throws out of a task, for an uncaught-exception handler to decide on. See [failure handling](guide/streams.md#failure-handling). |

There is no low-level public entry point to assemble a topology by hand. The processor decorator,
the buffer, and the shutdown drain are internal machinery `CausalStreams` composes.

## Reading paths

- To understand the guarantee, read [Foundations](foundations/causal-consistency.md) and then
  [The three protocols](protocols/index.md).
- To build a topology, start with [Getting started](guide/getting-started.md) and
  [Streams integration](guide/streams.md).
- To operate one, see [Configuration](guide/configuration.md) and
  [Troubleshooting](guide/troubleshooting.md).
- The [API reference](api/index.html) is the full Javadoc.
