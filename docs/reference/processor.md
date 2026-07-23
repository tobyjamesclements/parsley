# Streams integration

The Kafka Streams entry point is `ParsleyProcessorSupplier`, assembled by `CausalTopology#assemble`
from a `CausalStreamsBuilder`-declared stage (package-private; not constructed directly by user code —
see [Streams integration](../guide/streams.md) for the public API). It composes three package-private
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
2. Construct the task's one `ParsleyCausalBroadcast` (`buildCausalBroadcast()`, cached for the processor's lifetime — exactly one processor instance ever touches these stores within a task). Its `ParsleyChannels` loads the frontier clock and channel clocks from the single `"f"` value (empty if absent) and self-persists that value on every change. The restored state is rescoped to the current declared inputs, then a channel entry is seeded for every consumed input topic-partition.
3. Construct `ParsleyCausalBroadcast` with the `ParsleyChannels` (which owns the forwarded index and self-persists), a `StoreBackedBufferStore` wrapping the buffer store and a `ParsleySerializer`, and a `StoreBackedCandidateIndex` wrapping the candidate-index store. Build the task's `ParsleyGossip` over the same pair.
4. Wrap the real context in a `ParsleyProcessorContext` (stamping proxy). Call `delegate.init(wrappedContext)`.
5. Schedule a self-cancelling, one-shot `WALL_CLOCK_TIME` punctuation that drains any record satisfiable between the last committed frontier and the last committed buffer-removal (`drainAfterRestore()`), run once against the buffer restored from a changelog. Must run as a punctuation, not inline: Kafka Streams has not finished wiring the task's `RecordCollector` until every processor in the topology returns from `init()`, so `forward()` during `init()` throws.
6. Schedule a periodic metrics-refresh punctuator that also re-pushes this task's quiesce-drained state, so a task whose buffer emptied before `requestQuiesce()` was ever called still reports drained within one tick.

There is no eviction and no buffer-size punctuator: the causal buffer is unconditionally unbounded, so nothing needs to run periodically to enforce a limit.

## `process()` path

```
process(Record<KIn,VIn>)
  ensureTopicIdentityIntact()                      # E1 fail-fast: die before ingesting under a
                                                   #   stale name -> UUID binding
  switch classify(record):
    NULL_MESSAGE    -> handleNullMessage(record); return
    BUSINESS        -> (fall through)

  ingested = ingest(record)
    reads source metadata from context.recordMetadata()
    resolves topicId from topicUuids map (broker-resolved at init for each registered ParsleySource; throws IllegalStateException if absent)
    returns ParsleyMessage.from(record, source, offset, topicId)

  completenessBefore = causalBroadcast().completeness()
  outcome = causalBroadcast().receive(ingested)   # admitted records: delivered immediately or released from the buffer

  deliver(outcome.delivered())
    for each message:
      deliveryMetadata = message's source coordinate
      stampingContext.resetForwardCount()
      delegate.process(message as Record)          # forwards stamped live via broadcast()
      if stampingContext.forwardCount() == 0:      # delegate emitted nothing
        advertise(message.key())                   # the L3 stand-in null message
    clear deliveryMetadata

  # a consumed record that was buffered (nothing delivered) emits a heartbeat null message only if
  # receiving it advanced completeness (a first sighting seeds the frontier), so progress still
  # propagates downstream without flooding no-op null messages
  if outcome.delivered().isEmpty() and completeness() advanced past completenessBefore:
    advertise(record.key())
```

Each forwarded record is stamped live at forward time with the outbound stamp (`ParsleyChannels.stamp()`: completeness ∪ ownOutputs ∪ highestDelivered) — the max-merge of this node's own contiguous frontier, every input channel's advertised clock, and its own acked output positions, carrying transitive ancestry downstream (see [the delivery gate](../foundations/delivery-gate.md)). The delivery gate itself is the node's own contiguous frontier: a record is delivered only once this node has itself delivered every coordinate it depends on; an advertised claim feeds only the stamp. The same stamp value is attached to forwarded records and null messages.

When the delegate forwards no business record for a delivered input (`forwardCount() == 0`), the processor emits a protocol null message in its place (`ParsleyGossip.advertise`): a null-value record carrying the triggering record's key (informational only — `ParsleyMarkerPartition` routes the message to the forwarding task's own partition on every sink, regardless of key), the outbound stamp, and the `_parsley_null_message` header, so a non-emitting node still advances downstream completeness. On the receiving side `handleNullMessage()` decodes the carried clock and calls `ParsleyGossip.receive(topicId, partition, offset, carried)`: the null message's own offset is genuinely delivered on its source channel (advancing that channel's contiguous frontier, which is what can release held records), and the carried clock updates the channel's advertised view — feeding the outbound stamp, never the gate. It then relays a null message downstream — but only when the carried clock advanced this node's total knowledge on a channel it consumes (the I6 relay rule on `ParsleyGossip`; custody claims fold into the stamp without obliging a relay), never unconditionally; otherwise a cyclic topology's null messages would circulate forever.

## `ParsleyProcessorContext`

Wraps `ProcessorContext<KOut,VOut>`. Intercepts `forward()` to stamp the outbound vector timestamp
onto outgoing records.

**`forward(Record<K,V>)`:**

1. Increment the business-forward counter (read by `ParsleyProcessor` after each `delegate.process(...)`; a count of zero means the delegate emitted nothing and triggers null message emission).
2. Route the record through `ParsleyCausalBroadcast.broadcast()` — the single stamping site, shared with null messages — which folds pending producer acknowledgements, runs the crossing wait (a business forward excludes no destinations), and attaches the `parsley-causal-clock` header from `ParsleyChannels.stamp()`, replacing any existing header.
3. Call `delegate.forward(stamped)` — addressed to each declared sink node by name when the stage has incompatibly-typed sibling children, otherwise the plain forward.

The original record object is never mutated. The stamp is read live at forward time. A `forward()` during admit sees the post-admit stamp, and a `forward()` from a punctuator sees the stamp at punctuator fire time (punctuator forwards also fail fast if the topic-identity watch has detected a mid-run recreation). The processor resets the forward counter before each delivered record via `resetForwardCount()`.

**`recordMetadata()`:** Returns the source coordinate metadata of the record currently being delivered (set by `deliver()` above) when inside a delivery call. Returns the delegate's `recordMetadata()` otherwise. This gives the user processor access to the causal position of the triggering record.

All other `ProcessorContext` methods delegate verbatim.

## State store namespace

The namespace is the stage name `CausalTopology#assemble` derives from `application.id` (or the explicit name passed to `CausalStream#process(name, supplier)`), used internally as `ParsleyProcessorSupplier.Builder#addBufferStore(name)`. The store names are embedded in the changelog topic names, so they must be stable across deployments.
