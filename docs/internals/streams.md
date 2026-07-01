# Streams integration

The Kafka Streams entry point is `CausalProcessorSupplier`, built via `CausalProcessors.builder(...)`. It composes three package-private classes: `ParsleyProcessorSupplier`, `ParsleyProcessor`, and `ParsleyProcessorContext`.

## `ParsleyProcessorSupplier`

Implements `CausalProcessorSupplier<KIn,VIn,KOut,VOut>`.

**`get()`** returns a new `ParsleyProcessor` instance per call (one per task).

**`stores()`** returns the union of the user's declared stores and the four Parsley stores:

| Store | Key serde | Value serde |
|---|---|---|
| `{ns}-frontier` | `String` | `byte[]` |
| `{ns}-buffer` | `Long` | `byte[]` |
| `{ns}-candidate-index` | `byte[]` | `byte[]` |
| `{ns}-forwarded-index` | `byte[]` | `byte[]` |

All four are created with `Stores.persistentKeyValueStore(...)`, so they are changelog-backed and durable across restarts. The `{ns}-frontier` store's single `"f"` value holds both the contiguous frontier clock and the per-input-channel clocks (see [Wire format](wire-format.md#the-ns-frontier-f-value)).

## `ParsleyProcessor` init sequence

0. Resolve each registered `CausalBuffer` topic's stable UUID from the broker via a `ParsleyTopicAdmin` built from `context.appConfigs()` (the topology decorator has no broker config until init), populating the `topicUuids` map. Closed immediately after.
1. Retrieve the state stores from the processor context by name.
2. Construct a `ParsleyFrontier` over the `{ns}-frontier` store: it loads the frontier clock and channel clocks from the single `"f"` value (empty if absent) and self-persists that value on every change. Prune it to the current in-scope coordinates, then seed a channel entry for every consumed input topic-partition so a silent channel holds its own coordinate in the completeness fold.
3. Construct `ParsleyEngine` with:
    - The `ParsleyFrontier` (which owns the forwarded index and self-persists — no separate frontier callback).
    - A `RocksBufferStore` wrapping the buffer store and a `ParsleySerializer`.
    - A `RocksCandidateIndex` wrapping the candidate-index store.
4. Wrap the real context in a `ParsleyProcessorContext` (stamping proxy).
5. Call `delegate.init(wrappedContext)`.
6. Schedule a self-cancelling, one-shot `WALL_CLOCK_TIME` punctuation that calls `engine.evictOverflow()` on its
   first firing and cancels itself immediately after — enforces the size limit once against a buffer restored
   from a changelog (see [The engine: `evictOverflow()`](engine.md#evictoverflow-size-limit) for why this can't
   run synchronously inside `init()`).
7. If `engine.evictionInterval()` is present, schedule a punctuator to call `evict()` on that interval.

## `process()` path

```
process(Record<KIn,VIn>)
  if isWatermark(record):                 # carries the _parsley_watermark header
    handleWatermark(record); return

  ingest(record)
    reads source metadata from context.recordMetadata()
    resolves topicId from topicUuids map (broker-resolved at init for each registered CausalBuffer; throws IllegalStateException if absent)
    returns ParsleyMessage.from(record, source, offset, topicId)

  gate(parsleyRecord)
    returns engine.onRecord(parsleyRecord)   # list of admitted records

  deliver(admittedRecords)
    for each record:
      set stampFrontier = engine.completeness()
      set deliveryMetadata = record's source coordinate
      stampingContext.resetForwardCount()
      delegate.process(record.toConsumerRecord() as Record)
      if stampingContext.forwardCount() == 0:   # delegate emitted nothing
        forwardWatermark()                       # null/null record carrying engine.completeness()
    clear deliveryMetadata
    set stampFrontier = engine.completeness()

  # a consumed record that was buffered (nothing delivered) emits a heartbeat watermark only if
  # its receipt-time channel update advanced completeness, so progress still propagates downstream
  if admittedRecords.isEmpty() and completeness() advanced:
    forwardWatermark()
```

Each admitted record is stamped with `engine.completeness()` — the per-coordinate minimum across every input channel (each channel's advertised dependencies plus its own contiguous delivered position; see [Causal consistency model](causal-consistency.md#the-completeness-frontier)). A record is delivered only once every input channel has confirmed every coordinate it depends on; the same value is stamped on forwarded records and watermarks.

When the delegate forwards no business record for a delivered input (`forwardCount() == 0`), the processor emits a protocol watermark in its place: a null-key/null-value record carrying `engine.completeness()` and the `_parsley_watermark` header, so a non-emitting node still advances downstream completeness. On the receiving side `handleWatermark()` decodes the carried frontier, calls `engine.onWatermark(topicId, partition, frontier)` to update that source channel's clock and drain any newly releasable records, then re-emits a watermark downstream.

## `ParsleyProcessorContext`

Wraps `ProcessorContext<KOut,VOut>`. Intercepts `forward()` to stamp the causal frontier onto outgoing records.

**`forward(Record<K,V>)`:**

1. Increment the business-forward counter (read by `ParsleyProcessor` after each `delegate.process(...)`; a count of zero means the delegate emitted nothing and triggers watermark emission).
2. Build a new `RecordHeaders` from the record's existing headers, excluding any `parsley-causal-dependencies` header.
3. Add `parsley-causal-dependencies` from `frontier.get().toBytes()` (the `Supplier<ParsleyClock>` is read at call time, and supplies the completeness frontier).
4. Call `delegate.forward(record.withHeaders(stamped))`.

The original record object is never mutated. The frontier is read live. A `forward()` during admit sees the post-admit frontier, and a `forward()` from a punctuator sees the frontier at punctuator fire time. The processor resets the forward counter before each delivered record via `resetForwardCount()`.

**`recordMetadata()`:** Returns the source coordinate metadata of the record currently being delivered (set by `deliver()` above) when inside a delivery call. Returns the delegate's `recordMetadata()` otherwise. This gives the user processor access to the causal position of the triggering record.

All other `ProcessorContext` methods delegate verbatim.

## State store namespace

The namespace is the `name` passed to `CausalProcessors.builder(...).addBufferStore(name, limit)`. Use a unique namespace per `CausalProcessorSupplier` instance within a topology that contains more than one. The store names are embedded in the changelog topic names, so they must be stable across deployments.
