# Streams integration

The Kafka Streams entry point is `ParsleyProcessorSupplier`, assembled by `CausalTopology#assemble`
from a `CausalStreamsBuilder`-declared stage (package-private; not constructed directly by user code —
see [Streams integration](../streams.md) for the public API). It composes three package-private
classes: `ParsleyProcessorSupplier`, `ParsleyProcessor`, and `ParsleyProcessorContext`.

## `ParsleyProcessorSupplier`

Implements `ProcessorSupplier<KIn,VIn,KOut,VOut>`.

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

0. Resolve each registered `ParsleySource` topic's stable UUID from the broker via a `ParsleyTopicAdmin` built from `context.appConfigs()` (the topology decorator has no broker config until init), populating the `topicUuids` map. Closed immediately after.
1. Retrieve the state stores from the processor context by name.
2. If topology-epoch coordination is configured: resolve this task's shared `ParsleyEpochRuntime`, join it with this member's declared input channels and sink topics, block until this member is a running member (`awaitJoinCommit`), then run the startup self-check `validateFullMeshCoverage` — fails fast if this member's own declared topics do not cover the domain the coordination log already knows about.
3. Construct the task's one `ParsleyEngine` (`buildEngine()`, cached for the processor's lifetime — sound because passthrough topics are wired as extra sources into this same node, so no second processor instance ever shares these stores). Its `ParsleyFrontier` loads the frontier clock and channel clocks from the single `"f"` value (empty if absent) and self-persists that value on every change. The restored state is pruned to the current in-scope coordinates, then a channel entry is seeded for every consumed input topic-partition — including any passthrough topic (see below).
4. Construct `ParsleyEngine` with the `ParsleyFrontier` (which owns the forwarded index and self-persists), a `StoreBackedBufferStore` wrapping the buffer store and a `ParsleySerializer`, and a `StoreBackedCandidateIndex` wrapping the candidate-index store.
5. Wrap the real context in a `ParsleyProcessorContext` (stamping proxy). Call `delegate.init(wrappedContext)`.
6. Schedule a self-cancelling, one-shot `WALL_CLOCK_TIME` punctuation that drains any record satisfiable between the last committed frontier and the last committed buffer-removal (`drainAfterRestore()`), run once against the buffer restored from a changelog. Must run as a punctuation, not inline: Kafka Streams has not finished wiring the task's `RecordCollector` until every processor in the topology returns from `init()`, so `forward()` during `init()` throws.
7. Schedule a periodic metrics-refresh punctuator that also re-pushes this task's quiesce-drained state, so a task whose buffer emptied before `requestQuiesce()` was ever called still reports drained within one tick.

There is no eviction and no buffer-size punctuator: the causal buffer is unconditionally unbounded, so nothing needs to run periodically to enforce a limit.

## Passthrough topics

When `parsley.coordination.domain-topics` is configured, `CausalTopology#assemble` wires an extra, raw `byte[]`/`byte[]` source into this same processor node for any domain topic this stage does not otherwise consume or produce (see `CausalTopology`). `ParsleyProcessor` recognises such a record by its own source topic (never a header) — it flows through the ordinary delivery gate exactly like any other channel, contributing its causal progress to the frontier, but its key/value are never handed to the delegate (they are not genuine `KIn`/`VIn` values). Any *other* record a passthrough delivery happens to release from the shared buffer as a side effect still reaches the real delegate correctly, on that message's own turn through the delivery loop.

## `process()` path

```
process(Record<KIn,VIn>)
  switch classify(record):
    WATERMARK       -> handleWatermark(record); return
    EPOCH_BOUNDARY  -> handleEpochBoundary(record); return
    EPOCH_SNAPSHOT  -> handleEpochSnapshot(record); return
    BUSINESS        -> (fall through)

  if !isPassthroughRecord(record): lastSeenKey = record.key()

  ingested = ingest(record)
    reads source metadata from context.recordMetadata()
    resolves topicId from topicUuids map (broker-resolved at init for each registered ParsleySource; throws IllegalStateException if absent)
    returns ParsleyMessage.from(record, source, offset, topicId)

  completenessBefore = engine().completeness()
  outcome = engine().receive(ingested)   # admitted records: delivered immediately or released from the buffer

  deliver(outcome.delivered())
    for each message:
      stampFrontier = engine().completeness()
      deliveryMetadata = message's source coordinate
      if message arrived on a passthrough topic:
        forwardWatermark(lastSeenKey)              # never reaches the delegate
      else:
        stampingContext.resetForwardCount()
        delegate.process(message.toConsumerRecord() as Record)
        if stampingContext.forwardCount() == 0:    # delegate emitted nothing
          forwardWatermark(message.key())
    clear deliveryMetadata

  # a consumed record that was buffered (nothing delivered) emits a heartbeat watermark only if
  # receiving it advanced completeness (a first sighting seeds the frontier), so progress still
  # propagates downstream without flooding no-op watermarks
  if outcome.delivered().isEmpty() and completeness() advanced past completenessBefore:
    forwardWatermark(passthrough ? lastSeenKey : record.key())

  pollEpochCoordination()   # act promptly on a coordination-log change, not just on the wall-clock tick
```

Each admitted record is stamped with `engine().completeness()` — the max-merge of this node's own contiguous frontier and every input channel's advertised clock, carrying transitive ancestry downstream (see [Causal consistency model](causal-consistency.md)). The delivery gate itself is the node's own contiguous frontier: a record is delivered only once this node has itself delivered every coordinate it depends on; an advertised claim feeds only the stamp. The same completeness value is stamped on forwarded records and watermarks.

When the delegate forwards no business record for a delivered input (`forwardCount() == 0`), the processor emits a protocol watermark in its place: a null-value record keyed with the triggering record's key (so it routes to that record's partition), carrying `engine().completeness()` and the `_parsley_watermark` header, so a non-emitting node still advances downstream completeness. On the receiving side `handleWatermark()` decodes the carried frontier and calls `engine().onWatermark(topicId, partition, offset, frontier)`: the watermark's own offset is genuinely delivered on its source channel (advancing that channel's contiguous frontier, which is what can release held records), and the carried clock updates the channel's advertised view — feeding the outbound stamp, never the gate. It then relays a watermark downstream — but only when that channel's carried clock genuinely advanced, never unconditionally; otherwise a cyclic topology's marker would ping-pong forever.

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

The namespace is the stage name `CausalTopology#assemble` derives (or the explicit name passed to `CausalStream#process(name, supplier)`), used internally as `ParsleyProcessorSupplier.Builder#addBufferStore(name)`. Use a unique namespace per stage within a topology that contains more than one. The store names are embedded in the changelog topic names, so they must be stable across deployments.
