# Internals overview

Parsley is built around one public entry point — the Streams causal processor — plus stateless edge
operations for stamping and propagating dependencies from plain Kafka clients, all backed by a shared
internal implementation.

| API | Backed by |
|---|---|
| `CausalProcessorSupplier` | `ParsleyProcessorSupplier` + `ParsleyProcessor` |
| `CausalDependencies` edge ops (`stamp` / `from` / `merge`) | `ParsleyClock` (no wrapper objects) |
| `CausalTopics` | A caller-owned Kafka `Admin` (name → UUID, cached) |

All share a common set of value types and a single causal engine.

## Class map

### Public API

| Class | Role |
|---|---|
| `CausalDependencies` | Public facade over a `ParsleyClock`: the causal requirements stamped onto each record, plus the `stamp`/`from`/`merge` edge operations |
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
| `ParsleyProcessor` | Kafka Streams processor wrapping the user processor and driving the engine |
| `ParsleyProcessorSupplier` | Processor factory; registers the three Parsley state stores |
| `ParsleyProcessorContext` | Stamping proxy: replaces the context given to the user processor |
| `ParsleyBufferStore` / `RocksBufferStore` | Durable buffer of held records |
| `ParsleyCandidateIndex` / `RocksCandidateIndex` | Secondary index: coordinate -> candidate record IDs |
| `ParsleySerializer` | Binary serde for `ParsleyMessage` (buffer store wire format) |

## End-to-end flow

```
Edge (plain Kafka producer)
  CausalDependencies.from(topics, trigger) / .merge(...) / .builder(topics).require(...)
    -> CausalDependencies.stamp(record)
         -> stamps parsley-causal-dependencies header
    -> producer.send(record)

Causal processor (Streams)
  ParsleyProcessor.process(record)
    -> ingest: wrap in ParsleyMessage, embed source coordinates
    -> gate:   ParsleyEngine.onRecord()
                 satisfied   -> advance frontier, drain cascade
                 unsatisfied -> buffer (RocksBufferStore) + index (RocksCandidateIndex)
                 no header   -> trivially satisfied (empty dependencies)
    -> deliver: user processor receives records via ParsleyProcessorContext
                forwarded records carry the stamped frontier dependencies
                (Streams sinks carry the header out to the output topic)
```

## Further reading

- [Wire format](wire-format.md) — binary layouts for all headers and state stores
- [The engine](engine.md) — buffer, candidate index, drain cascade, eviction
- [Streams integration](streams.md) — processor init, state store wiring, stamping proxy
