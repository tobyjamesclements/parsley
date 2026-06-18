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
| `CausalDependencies` | Immutable set of causal requirements stamped by the producer onto each record |
| `CausalFrontier` | Immutable per-consumer horizon: highest observed offset per (topicId, partition) |
| `CausalPosition` | A single coordinate: `(topicId, partition, offset)` |
| `CausalViolation` | Snapshot of a violation: reason, frontier, required dependencies, gap |
| `CausalViolationReason` | Enum: `MISSING_HEADER`, `UNRESOLVABLE_DEPENDENCIES`, `LIMIT_REACHED` |

### Package-private implementation

| Class | Role |
|---|---|
| `ParsleyEngine` | Causal buffer engine: classify, buffer, cascade, evict |
| `ParsleyRecord` | Internal record envelope carrying source coordinate and dependency headers |
| `ParsleyAttributes` | String constants for all header keys and state-store keys |
| `ParsleyProcessor` | Kafka Streams processor wrapping the user processor and driving the engine |
| `ParsleyProcessorSupplier` | Processor factory; registers the three Parsley state stores |
| `ParsleyProcessorContext` | Stamping proxy: replaces the context given to the user processor |
| `ParsleyConsumer` | Poll-based consumer backed by a Streams topology and outbox topic |
| `ParsleyProducer` | Decorator that stamps `parsley-causal-dependencies` on every send |
| `ParsleyBufferStore` / `RocksBufferStore` | Durable buffer of held records |
| `ParsleyWaitIndex` / `RocksWaitIndex` | Secondary index: coordinate -> candidate record IDs |
| `ParsleySerializer` | Binary serde for `ParsleyRecord` (buffer store wire format) |

## End-to-end flow

```
Producer
  ParsleyProducer.send(record, deps)
    -> stamps parsley-causal-dependencies header
    -> sends to Kafka

Consumer (CausalConsumer path)
  Kafka Streams topology
    ParsleyProcessor.process(record)
      -> ingest: wrap in ParsleyRecord, embed source coordinates
      -> gate:   ParsleyEngine.onRecord()
                   satisfied   -> advance frontier, drain cascade
                   unsatisfied -> buffer (RocksBufferStore) + index (RocksWaitIndex)
                   no header   -> MISSING_HEADER violation, apply policy
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
- [The engine](engine.md) — buffer, wait index, drain cascade, eviction
- [Streams integration](streams.md) — processor init, state store wiring, stamping proxy
- [Consumer](consumer.md) — outbox pattern, poll() path, frontier merging
