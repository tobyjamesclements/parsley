# Internals overview

Parsley is built around one public entry point — the Streams causal processor — plus stateless edge
operations for stamping and propagating dependencies from plain Kafka clients, all backed by a shared
internal implementation.

| API | Backed by |
|---|---|
| `CausalProcessorSupplier` | `ParsleyProcessorSupplier` + `ParsleyProcessor` |
| `CausalDependencies` edge ops (`using` / `observe` / `stamp` / `merge`) | `ParsleyClock` (no wrapper objects) |
| `CausalTopics` | A caller-owned Kafka `Admin` (name → UUID, cached) |

All share a common set of value types and a single causal engine.

## Class map

### Public API

| Class | Role |
|---|---|
| `CausalDependencies` | Public facade over a `ParsleyClock`: the causal requirements stamped onto each record, plus the `using`/`observe`/`stamp`/`merge` edge operations and `isWatermark` for skipping protocol watermarks on the consumer side |
| `CausalTopics` | Resolves topic names to their stable Kafka UUIDs through a caller-owned `Admin` (used to build `CausalDependencies`) |
| `CausalBuffer` | Registers one causal source on the processor builder: topic name + the serdes the buffer round-trips held records with (the topic's UUID is resolved from the broker) |
| `CausalBufferLimit` | When to evict held records: `ofDuration`, `ofSize`, `first` |

### Package-private implementation

| Class | Role |
|---|---|
| `ParsleyEngine` | Causal buffer engine: classify, buffer, cascade, evict |
| `ParsleyClock` | The one vector clock: node frontier *and* dependency representation, keyed on `(Uuid, int)` primitives |
| `ParsleyMessage` | Typed engine envelope: source coordinate + dependency clock as fields, user headers separate |
| `ParsleyHeader` | A `(key, value)` header plus the header-key vocabulary (`_parsley_*`, reserved keys, factories) |
| `ParsleyProcessor` | Kafka Streams processor wrapping the user processor and driving the engine; emits and consumes protocol watermarks |
| `ParsleyProcessorSupplier` | Processor factory; registers Parsley's five state stores |
| `ParsleyProcessorContext` | Stamping proxy: replaces the context given to the user processor; counts business forwards to drive watermark emission |
| `ParsleyBufferStore` / `RocksBufferStore` | Durable buffer of held records |
| `ParsleyCandidateIndex` / `RocksCandidateIndex` | Secondary index: coordinate -> candidate record IDs |
| `ParsleyForwardedIndex` / `RocksForwardedIndex` | Offsets forwarded ahead of the contiguous frontier, so the boundary stays gap-free across restarts |
| `ParsleyChannelClockStore` / `RocksChannelClockStore` | Per-input-channel clock (own delivered position + observed dependencies); the completeness frontier is the per-coordinate min across all input channels |
| `ParsleySerializer` | Binary serde for `ParsleyMessage` (buffer store wire format) |

## End-to-end flow

```
Edge (plain Kafka producer)
  CausalDependencies.using(topics).observe(trigger) / .observe(...) / .builder(topics).require(...)
    -> CausalDependencies.stamp(record)
         -> stamps parsley-causal-dependencies header
    -> producer.send(record)

Causal processor (Streams)
  ParsleyProcessor.process(record)
    -> watermark? -> ParsleyEngine.onWatermark(): advance channel clock, drain, re-emit
    -> ingest: wrap in ParsleyMessage, embed source coordinates
    -> gate:   ParsleyEngine.onRecord()
                 completeness().dominates(deps) — every input channel has confirmed every
                   depended coordinate (per-coordinate min across all input channels)
                 satisfied   -> advance frontier, drain cascade
                 unsatisfied -> buffer (RocksBufferStore) + index (RocksCandidateIndex)
                 no header   -> trivially satisfied (empty dependencies)
    -> deliver: user processor receives records via ParsleyProcessorContext
                forwarded records carry the node's completeness frontier
                an input that emits no business record -> a protocol watermark instead
                (Streams sinks carry the header out to the output topic)
```

## Further reading

- [Wire format](wire-format.md) — binary layouts for all headers and state stores
- [The engine](engine.md) — buffer, candidate index, drain cascade, eviction
- [Streams integration](streams.md) — processor init, state store wiring, stamping proxy
