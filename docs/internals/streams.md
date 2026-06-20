# Streams integration

The Kafka Streams entry point is `CausalProcessorSupplier`, built via `CausalProcessors.builder(...)`. It composes three package-private classes: `ParsleyProcessorSupplier`, `ParsleyProcessor`, and `ParsleyProcessorContext`.

## `ParsleyProcessorSupplier`

Implements `CausalProcessorSupplier<KIn,VIn,KOut,VOut>`.

**`get()`** returns a new `ParsleyProcessor` instance per call (one per task).

**`stores()`** returns the union of the user's declared stores and the three Parsley stores:

| Store | Key serde | Value serde |
|---|---|---|
| `{ns}-frontier` | `String` | `byte[]` |
| `{ns}-buffer` | `Long` | `byte[]` |
| `{ns}-position-index` | `byte[]` | `byte[]` |

All three are created with `Stores.persistentKeyValueStore(...)`, so they are changelog-backed and durable across restarts.

## `ParsleyProcessor` init sequence

1. Retrieve the three stores from the processor context by name.
2. Read frontier from `{ns}-frontier` at key `"f"`. Start from `CausalFrontier.empty()` if absent.
3. Publish the restored frontier to the `CausalFrontierListener`.
4. Construct `ParsleyEngine` with:
    - A `FrontierCallback` that writes the new frontier to the frontier store, appends a snapshot to a local list, and fires the `CausalFrontierListener`.
    - A `RocksBufferStore` wrapping the buffer store and a `ParsleySerializer`.
    - A `RocksPositionIndex` wrapping the position-index store.
5. Wrap the real context in a `ParsleyProcessorContext` (stamping proxy).
6. Call `delegate.init(wrappedContext)`.
7. If `engine.evictionInterval()` is present, schedule a punctuator to call `evict()` on that interval.

## `process()` path

```
process(Record<KIn,VIn>)
  ingest(record)
    reads source metadata from context.recordMetadata()
    resolves topicId from topicUuids map or CausalPosition.deriveUuid()
    returns ParsleyRecord.of(record, source, offset, topicId)

  gate(parsleyRecord)
    clears snapshot list
    calls engine.onRecord(parsleyRecord)
    returns list of admitted records + collected snapshots

  deliver(admittedRecords, snapshots)
    for each (record, snapshot):
      set stampFrontier = snapshot
      set deliveryMetadata = record's source coordinate
      delegate.process(record.toConsumerRecord() as Record)
    restore stampFrontier = engine.frontier()
    clear deliveryMetadata
```

The snapshot list collects the frontier state after each individual advance inside `engine.onRecord()`. During delivery, each admitted record is paired with its corresponding snapshot so that the stamped dependencies reflect the frontier at the exact moment the record was admitted, not the final post-cascade frontier.

## `ParsleyProcessorContext`

Wraps `ProcessorContext<KOut,VOut>`. Intercepts `forward()` to stamp the causal frontier onto outgoing records.

**`forward(Record<K,V>)`:**

1. Build a new `RecordHeaders` from the record's existing headers, excluding any `parsley-causal-dependencies` header.
2. Add `parsley-causal-dependencies` from `frontier.get().toBytes()` (the `Supplier<CausalFrontier>` is read at call time).
3. Call `delegate.forward(record.withHeaders(stamped))`.

The original record object is never mutated. The frontier is read live: a `forward()` during admit sees the post-admit frontier; a `forward()` from a punctuator sees the frontier at punctuator fire time.

**`recordMetadata()`:** Returns the source coordinate metadata of the record currently being delivered (set by `deliver()` above) when inside a delivery call. Returns the delegate's `recordMetadata()` otherwise. This gives the user processor access to the causal position of the triggering record.

All other `ProcessorContext` methods delegate verbatim.

## State store namespace

The default namespace is `"parsley"`. Set it with `CausalProcessors.builder(...).storeName(ns)`. Use a unique namespace per `CausalProcessorSupplier` instance within a topology that contains more than one. The store names are embedded in the changelog topic names, so they must be stable across deployments.
