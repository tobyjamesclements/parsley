# Internals overview

Parsley has three public entry points backed by a shared internal implementation.

| Entry point | Backed by |
|---|---|
| `CausalProducer` | `ParsleyProducer` |
| `CausalConsumer` | `ParsleyConsumer` + Kafka Streams + outbox topic |
| `CausalProcessorSupplier` | `ParsleyProcessorSupplier` + `ParsleyProcessor` |

All three share a common set of value types and a single causal engine.

## Class map

### Public API

| Class | Role |
|---|---|
| `CausalDependencies` | Public facade over a `ParsleyClock`: the causal requirements stamped by the producer onto each record |
| `CausalTopic` | A topic's stable causal identity: name + Kafka UUID (used for registration and `require`) |
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
| `ParsleyConsumer` | Poll-based consumer backed by a Streams topology and outbox topic |
| `ParsleyProducer` | Decorator that stamps `parsley-causal-dependencies` on every send |
| `ParsleyBufferStore` / `RocksBufferStore` | Durable buffer of held records |
| `ParsleyPositionIndex` / `RocksPositionIndex` | Secondary index: coordinate -> candidate record IDs |
| `ParsleySerializer` | Binary serde for `ParsleyMessage` (buffer store wire format) |

## End-to-end flow

```
Producer
  ParsleyProducer.send(record, deps)
    -> stamps parsley-causal-dependencies header
    -> sends to Kafka

Consumer (CausalConsumer path)
  Kafka Streams topology
    ParsleyProcessor.process(record)
      -> ingest: wrap in ParsleyMessage, embed source coordinates
      -> gate:   ParsleyEngine.onRecord()
                   satisfied   -> advance frontier, drain cascade
                   unsatisfied -> buffer (RocksBufferStore) + index (RocksPositionIndex)
                   no header   -> trivially satisfied (empty dependencies)
      -> deliver: for each admitted record
                   stamp frontier onto parsley-causal-dependencies
                   save ORIGINAL_DEPENDENCIES, forward to outbox topic

  Outbox topic (internal, bytes)

  ParsleyConsumer.poll()
    -> reads from outbox topic
    -> restores original producer dependencies from ORIGINAL_DEPENDENCIES
    -> deserialises key/value by source topic
    -> reconstructs ConsumerRecord at source partition/offset
    -> returns ConsumerRecords grouped by source TopicPartition

CausalProcessorSupplier path (Streams-native)
  Same ParsleyProcessor; no outbox topic
  User processor receives records via ParsleyProcessorContext
  Forwarded records carry stamped frontier dependencies
```

## Further reading

- [Wire format](wire-format.md) — binary layouts for all headers and state stores
- [The engine](engine.md) — buffer, position index, drain cascade, eviction
- [Streams integration](streams.md) — processor init, state store wiring, stamping proxy
- [Consumer](consumer.md) — outbox pattern, poll() path, frontier merging
