# The three protocols

This section describes the layered protocol stack that delivers Parsley's guarantee.
[Foundations](../foundations/causal-consistency.md) develops why the stack is shaped this way; this
overview gives the module structure, the class map, the end-to-end flow, and the cost model, and
the three pages that follow describe each protocol in turn.

Parsley is built around one public, topology-level entry point, `CausalStreamsBuilder` /
`CausalTopology` / `CausalStreams`, three roles mirroring Kafka Streams' own `StreamsBuilder` /
`Topology` / `KafkaStreams`, plus stateless edge operations for stamping and propagating causal
clocks from plain Kafka clients, all backed by a shared internal implementation.

| API | Backed by |
|---|---|
| `CausalStreamsBuilder` / `CausalTopology` / `CausalStreams` | `ParsleyProcessorSupplier` / `ParsleyProcessor` (package-private) |
| `CausalClock` edge ops (`using` / `observe` / `stamp` / `merge`) | `ParsleyVectorClock` (no wrapper objects); `using`/`builder` resolve topic UUIDs internally, caching each name against a short-lived Kafka admin client opened on first use |

## The three protocol modules

Internally the implementation is organised as three layered protocols, each a package-private
module presented in the style of Cachin–Guerraoui–Rodrigues (*Introduction to Reliable and Secure
Distributed Programming*): requests in, indications out, properties guaranteed. Each layer
consumes the guarantees of the layer below and provides a clean assumption set to the layer above.

```
┌──────────────────────────────────────────────────────────────────┐
│ gossip (ParsleyGossip)             liveness / clock dissemination │
├──────────────────────────────────────────────────────────────────┤
│ causal broadcast (ParsleyCausalBroadcast)   receive / deliver     │
├──────────────────────────────────────────────────────────────────┤
│ channels (ParsleyChannels)         Kafka → reliable-FIFO-channel  │
│                                    adaptation                     │
└──────────────────────────────────────────────────────────────────┘
```

- **[Channels](channels.md)** turns Kafka topic-partitions into the reliable FIFO channels
  classical causal broadcast assumes: stable UUID-keyed coordinates, dense offset sequences
  (seeding, bridging, the contiguous frontier), the node's own acknowledged output positions, and
  normalised dependency clocks.
- **[Causal broadcast](causal-broadcast.md)** is Birman–Schiper–Stephenson causal delivery over
  those channels: the two-branch delivery gate, the hold-back buffer, the release cascade, and
  the single stamping site.
- **[Gossip](gossip.md)** keeps clock progress observable through processors that produce no
  business output, using protocol null messages, and quiesces on any topology shape including
  cycles.

Two Parsley-wide deviations from the textbook module style apply throughout, stated once in
`package-info`: indications are pulled, not pushed (deliveries come back as ordered return values,
because Kafka Streams' threading is synchronous), and the sender's clock increment is performed by
the broker (offset assignment, learned asynchronously from producer acknowledgements — which is
why an outbound stamp cannot include the record's own coordinate).

## Class map

### Public API

| Class | Role |
|---|---|
| `CausalStreamsBuilder` | Declares the topology's single causal stage: `stream(...)` source topics, `.process(supplier)`, `.to(...)` sink(s); the chain's terminal `.build()` (on `CausalProcessedStream`) produces a `CausalTopology` |
| `CausalStream` / `CausalProcessedStream` | Fluent intermediate types `CausalStreamsBuilder` returns while declaring the stage (`merge`, `process`, `to`, `withPartitioner`, `build`) |
| `CausalTopology` | The built, immutable topology specification; `assemble(props, quiesce)` produces the real Kafka Streams `Topology` |
| `CausalStreams` | The runtime: wraps the underlying `KafkaStreams` instance, owns graceful causal drain on `close()`/`close(Duration)` and the background topic-identity watch; passes through `state()`, `metrics()`, `setStateListener`, `setUncaughtExceptionHandler` |
| `CausalClock` | Public facade over a `ParsleyVectorClock`: the causal requirements stamped onto each record, plus the `using`/`observe`/`stamp`/`merge` edge operations and `isNullMessage` for skipping protocol null messages on the consumer side |
| `CausalDeliveryException` hierarchy | The fail-closed throws as public types: `CausalCoordinateException` (abstract, carries the source coordinate) over `CausalBufferDeserializationException` and `CausalVectorClockResolutionException`; `CausalTopicRecreatedException` and `CausalPendingAckException` directly under the root |

### Package-private implementation

| Class | Role |
|---|---|
| `ParsleyChannels` | The channels module: owns all persisted causal metadata — contiguous frontier, per-channel advertised clocks, carried ancestry, own outputs, highest received — self-persisting as the single `"frontier"` value; normalisation, scope changes, the crossing wait |
| `ParsleyCausalBroadcast` | The causal-broadcast module (Birman–Schiper–Stephenson): gate, buffer, cascade, fail-closed, the single stamping site. No eviction, no buffer limit, no timeout |
| `ParsleyGossip` | The gossip module: receives null messages (own offset delivered, carried clock folded stamp-side only) and builds this node's own; owns the relay rule (relay only when the carried clock advanced this node's knowledge of a channel it consumes — custody folds but never obliges a relay) |
| `ParsleyVectorClock` | The one vector clock: node frontier *and* dependency representation, keyed on `(Uuid, int)` primitives |
| `ParsleyMessage` | Typed envelope: source coordinate + dependency clock as fields, user headers separate |
| `ParsleyHeader` | A `(key, value)` header plus the header-key vocabulary (`_parsley_*`, reserved keys, factories) |
| `ParsleyProcessor` | Kafka Streams processor wrapping the user processor and driving the modules; emits and consumes protocol null messages |
| `ParsleyProcessorSupplier` | Processor factory; registers Parsley's four state stores |
| `ParsleyProcessorContext` | Stamping proxy: replaces the context given to the user processor; counts business forwards to drive null message emission |
| `ParsleyOwnOutputInterceptor` / `ParsleyOwnOutputRegistry` | Producer interceptor capturing the node's own acknowledged `(topic, partition, offset)` sends, and the registry that presents acked offsets and pending sends to the channels module (Kafka-specific: the broker performs the sender's clock increment) |
| `ParsleyTopicIdentityWatch` | Background poll of broker topic IDs against each task's init-time resolution; fails the application fast on a mid-run topic recreation (assumption E1) |
| `ParsleyOffsetSeeder` | Seeds log-start offsets on a genuine first start only, refusing when surviving state shows the group is not new (assumption E2's enforcement, with `AutoOffsetReset.none()`) |
| `ParsleyTopics` / `KafkaTopics` | Resolves topic names to their stable Kafka UUIDs (through a short-lived Kafka admin client, cached), backing `CausalClock`'s `using`/`builder` |
| `ParsleySource` | One registered causal source: topic name + the serdes the buffer round-trips held records with (the topic's UUID is resolved from the broker) |
| `ParsleyBufferStore` / `StoreBackedBufferStore` | Durable buffer of held records |
| `ParsleyCandidateIndex` / `StoreBackedCandidateIndex` | Secondary index: coordinate -> candidate record IDs |
| `ParsleyForwardedIndex` / `StoreBackedForwardedIndex` | Offsets forwarded ahead of the contiguous frontier, so the boundary stays gap-free across restarts |
| `ParsleySerializer` | Binary serde for `ParsleyMessage` (buffer store wire format) |

## End-to-end flow

```
Edge (plain Kafka producer)
  CausalClock.using(props).observe(trigger) / .observe(...) / .builder(props).require(...)
    -> CausalClock.stamp(record)
         -> stamps parsley-causal-clock header
    -> producer.send(record)

Causal processor (Streams)
  ParsleyProcessor.process(record)
    -> null message? -> ParsleyGossip.receive: deliver its own offset, fold its carried clock
                        stamp-side, relay onward only if it advanced a consumed channel
    -> ingest: wrap in ParsleyMessage, embed source coordinates
    -> gate:   ParsleyCausalBroadcast.receive()
                 frontier.dominates(consumedDeps) — this node's own contiguous delivered
                   frontier; a dependency is satisfied only by local delivery of the cause,
                   never by a claim advertised on another channel's clock (that feeds only
                   the outbound stamp)
                 satisfied     -> advance frontier, drain cascade
                 unsatisfied   -> buffer (StoreBackedBufferStore) + index (StoreBackedCandidateIndex)
                 no header     -> trivially satisfied (empty dependencies)
                 unconsumed    -> no channel for this coordinate here -> ignored, with a metric
                                  (consumed ancestry is claimed directly by the same clock)
    -> deliver: user processor receives records via ParsleyProcessorContext
                forwarded records carry the node's outbound stamp
                an input that emits no business record -> a protocol null message instead
                (Streams sinks carry the header out to the output topic)
```

## Cost model

Parsley's overhead is asserted from the structure of the implementation rather than measured, and
each row below names the protocol page that carries the mechanism. Parsley ships no benchmark
suite: absolute latency depends on hardware, storage class, and workload shape, and the costs that
dominate in practice are state persistence, protocol record volume, and producer acknowledgement
waits rather than any single clock walk. To size a deployment, measure your own topology end to end
and watch the [metrics](../guide/configuration.md#metrics).

Throughout, `w` is the width of a clock (its number of `(topic, partition)` entries), `C` is the
number of channels a node tracks, `n` is the number of records held in the buffer, `k` and `r` are
drain parameters, and `N` is the number of records a delegate forwards per input.

| Cost | Complexity | Scales with | Layer |
|---|---|---|---|
| Header parse | O(w) | Incoming clock width | [causal broadcast](causal-broadcast.md#cost) |
| Gate evaluation | O(w) | Incoming clock width | [causal broadcast](causal-broadcast.md#cost) |
| Outbound stamp | O(w) | Outbound clock width, which grows with causal history | [causal broadcast](causal-broadcast.md#cost) |
| State persistence | O(C · w) | Total channel state, once per advance | [channels](channels.md#cost) |
| Buffer drain (candidate lookup) | O(log n) | Records in buffer | [causal broadcast](causal-broadcast.md#cost) |
| Buffer drain (per released record) | O(k) | Records sharing the trigger coordinate | [causal broadcast](causal-broadcast.md#cost) |
| Cascade propagation | O(r) | Chained release depth | [causal broadcast](causal-broadcast.md#cost) |
| Channel-state restore on restart | O(C · w) | Total channel state | [channels](channels.md#cost) |
| Buffer restore on restart | O(n) | Records held at restart | [causal broadcast](causal-broadcast.md#cost) |
| Crossing-wait produce serialization | O(N) waits | Forwards per invocation × (linger + replication RTT) | [causal broadcast](causal-broadcast.md#cost) |
| Gossip volume | up to one null message per silent input per sink | Sink count, share of inputs with no business output | [gossip](gossip.md#cost) |

The dominant per-record term is usually the O(C · w) channel-state persist, because
`ParsleyChannels` rewrites its whole persisted value on every frontier advance and a store write
costs far more than any clock walk. Each protocol page develops its own rows under its Cost
section.

## Further reading

- [Causal consistency](../foundations/causal-consistency.md),
  [the delivery gate](../foundations/delivery-gate.md), and
  [environmental assumptions](../foundations/assumptions.md) give the model, the gate's soundness
  argument, and E1–E3.
- [Named invariants](../foundations/invariants.md) is the I1–I9 catalogue that Javadoc and tests
  cite.
- [The channels module](channels.md) covers coordinates, density, own outputs, and scope changes.
- [The causal-broadcast module](causal-broadcast.md) covers the buffer, candidate index, drain
  cascade, fail-closed delivery, and the stamping site.
- [The gossip module](gossip.md) covers null messages, emission, and the relay rule.
- [The processor](../reference/processor.md) covers processor init, state store wiring, and the
  stamping proxy.
- [Wire format](../reference/wire-format.md) gives the binary layouts for all headers and state
  stores.
- [Naming](../reference/naming.md) covers the visibility convention, the academic naming test, and
  the decision register.
