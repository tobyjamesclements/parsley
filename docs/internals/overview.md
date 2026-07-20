# Internals overview

Parsley is built around one public, topology-level entry point — `CausalStreamsBuilder` /
`CausalTopology` / `CausalStreams`, three roles mirroring Kafka Streams' own `StreamsBuilder` /
`Topology` / `KafkaStreams` — plus stateless edge operations for stamping and propagating dependencies
from plain Kafka clients, all backed by a shared internal implementation.

| API | Backed by |
|---|---|
| `CausalStreamsBuilder` / `CausalTopology` / `CausalStreams` | `ParsleyProcessorSupplier` / `ParsleyProcessor` (package-private) |
| `CausalDependencies` edge ops (`using` / `observe` / `stamp` / `merge`) | `ParsleyVectorClock` (no wrapper objects); `using`/`builder` resolve topic UUIDs internally, caching each name against a short-lived Kafka admin client opened on first use |

All share a common set of value types and a single causal engine.

## Class map

### Public API

| Class | Role |
|---|---|
| `CausalStreamsBuilder` | Declares one or more causal stages: `stream(...)` source topics, `.process(supplier)`, `.to(...)` sink(s); `.build()` produces a `CausalTopology` |
| `CausalStream` / `CausalProcessedStream` | Fluent intermediate types `CausalStreamsBuilder` returns while declaring a stage (`merge`, `process`, `to`, `withPartitioner`) |
| `CausalTopology` | The built, immutable topology specification; `assemble(props, quiesce, coordination)` produces the real Kafka Streams `Topology` |
| `CausalStreams` | The runtime: wraps the underlying `KafkaStreams` instance, owns graceful causal drain on `close()` and, when configured, topology-epoch coordination |
| `CausalDependencies` | Public facade over a `ParsleyVectorClock`: the causal requirements stamped onto each record, plus the `using`/`observe`/`stamp`/`merge` edge operations and `isWatermark` for skipping protocol watermarks on the consumer side |

### Package-private implementation

| Class | Role |
|---|---|
| `ParsleyTopics` / `KafkaTopics` | Resolves topic names to their stable Kafka UUIDs (through a short-lived Kafka admin client, cached), backing `CausalDependencies`'s `using`/`builder` |
| `ParsleySource` | One registered causal source: topic name + the serdes the buffer round-trips held records with (the topic's UUID is resolved from the broker) |
| `ParsleyEngine` | Causal buffer engine: classify, buffer, cascade, fail-closed. No eviction, no buffer limit, no timeout |
| `ParsleyVectorClock` | The one vector clock: node frontier *and* dependency representation, keyed on `(Uuid, int)` primitives |
| `ParsleyMessage` | Typed engine envelope: source coordinate + dependency clock as fields, user headers separate |
| `ParsleyHeader` | A `(key, value)` header plus the header-key vocabulary (`_parsley_*`, reserved keys, factories) |
| `ParsleyProcessor` | Kafka Streams processor wrapping the user processor and driving the engine; emits and consumes protocol watermarks and epoch markers |
| `ParsleyProcessorSupplier` | Processor factory; registers Parsley's four state stores |
| `ParsleyProcessorContext` | Stamping proxy: replaces the context given to the user processor; counts business forwards to drive watermark emission |
| `ParsleyBufferStore` / `StoreBackedBufferStore` | Durable buffer of held records |
| `ParsleyCandidateIndex` / `StoreBackedCandidateIndex` | Secondary index: coordinate -> candidate record IDs |
| `ParsleyForwardedIndex` / `StoreBackedForwardedIndex` | Offsets forwarded ahead of the contiguous frontier, so the boundary stays gap-free across restarts |
| `ParsleyChannels` | Owns all causal metadata a node persists — the contiguous delivered frontier clock (the delivery gate's clock), the per-input-channel clocks, and `completeness()` (the max-merge of the frontier and every channel's advertised clock — the outbound stamp, never the gate) — self-persisting as the single `"f"` value of the frontier store; holds the forwarded index as a collaborator |
| `ParsleySerializer` | Binary serde for `ParsleyMessage` (buffer store wire format) |

## End-to-end flow

```
Edge (plain Kafka producer)
  CausalDependencies.using(props).observe(trigger) / .observe(...) / .builder(props).require(...)
    -> CausalDependencies.stamp(record)
         -> stamps parsley-causal-dependencies header
    -> producer.send(record)

Causal processor (Streams)
  ParsleyProcessor.process(record)
    -> watermark/epoch marker? -> advance channel clock, drain, relay onward only if it genuinely advanced
    -> ingest: wrap in ParsleyMessage, embed source coordinates
    -> gate:   ParsleyEngine.receive()
                 frontier.dominates(deps) — this node's own contiguous delivered frontier; a
                   dependency is satisfied only by local delivery of the cause, never by a claim
                   advertised on another channel's clock (that feeds only the outbound stamp)
                 satisfied     -> advance frontier, drain cascade
                 unsatisfied   -> buffer (StoreBackedBufferStore) + index (StoreBackedCandidateIndex)
                 no header     -> trivially satisfied (empty dependencies)
                 unconsumed    -> no channel for this coordinate here -> ignored, with a metric
                                  (consumed ancestry is claimed directly by the same clock)
    -> deliver: user processor receives records via ParsleyProcessorContext
                forwarded records carry the node's completeness frontier
                an input that emits no business record -> a protocol watermark instead
                (Streams sinks carry the header out to the output topic)
```

## Further reading

- [Wire format](wire-format.md) — binary layouts for all headers and state stores
- [The engine](engine.md) — buffer, candidate index, drain cascade, fail-closed delivery
- [Streams integration](streams.md) — processor init, state store wiring, stamping proxy
