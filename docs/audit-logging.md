# Audit logging

`CausalAudit` is an optional, client-supplied seam that receives every per-record event in the
causal-buffering lifecycle. The events are forwarded, held, released, evicted (a causal violation),
eviction-limit-exceeded (fail-fast under the default policy), deserialization failure, and
clock-resolution failure, together with processor startup and shutdown. Register one to route these
events wherever your audit or compliance trail needs them, such as a SIEM, a durable audit store, or
structured logs. Parsley does not decide where they go. Without one registered, the events are
discarded.

## Registering an audit

Implement `CausalAudit` and pass it to the builder.

```java
ParsleyProcessorSupplier<String, Order, String, Enriched> causal =
        ParsleyProcessors.builder(user)
                .addBufferStore("parsley", CausalBufferLimit.ofDuration(limit))
                .addBuffers(List.of("prices", "orders"), Serdes.String(), orderSerde)
                .withAudit(mySiemAudit)
                .build();
```

Without `.withAudit(...)`, Parsley uses `CausalAudit.NOOP`. No events are emitted and there is no
overhead.

## Events

| Method | Fires when | Carries |
|---|---|---|
| `recordForwarded` | A record's dependencies were already satisfied and it was delivered without buffering. | topic, partition, offset |
| `recordHeld` | A record's dependencies were unsatisfied and it was admitted to the buffer to wait. | topic, partition, offset, buffer depth after admission, the unmet gap |
| `recordReleased` | A previously held record became satisfiable and drained, in causal order. | topic, partition, offset, buffer depth after release |
| `recordViolation` | A held record was evicted and forwarded out of order because a `CausalBufferLimit` fired first. | topic, partition, offset, the gap still unmet at eviction |
| `recordDeserializationFailure` | A held record could no longer be deserialised on the forward path, for example after an incompatible Schema Registry change while it was buffered. | topic, partition, offset, an operator-facing reason string, whether the record was dropped or left buffered for recovery |
| `recordEvictionLimitExceeded` | A `CausalBufferLimit` fired and `parsley.buffer.eviction.failure.policy = fail` (the default): the task is about to fail fast; the record remains buffered rather than being delivered out of causal order. | topic, partition, offset, the gap still unmet |
| `recordClockResolutionFailure` | An inbound record's `parsley-causal-dependencies` header could not be decoded into a clock (corrupt, truncated, or unsupported wire version). | topic, partition, offset, an operator-facing reason string, whether the task is being failed (`fail` default) or the record forwarded with empty dependencies (`continue`) |
| `processorInitialized` | A task's processor started up. | task id, whether a persisted frontier was restored |
| `processorClosing` | A task's processor is shutting down. | task id |

Every method carries only topic, partition, offset, and causal metadata. None of them carry the
record's key or value, which matches Parsley's own logs.

## Relationship to Parsley's metrics

`CausalAudit` is independent of, and parallel to, the aggregate counters that Parsley always wires
into the Kafka Streams metrics registry. Those metrics are the counters `records-buffered`,
`records-released`, `records-evicted`, `violations`, `deserialization-errors`, and
`eviction-limit-exceeded`, together with the gauges `buffer-depth`, `buffer-size-limit`,
`buffer-duration-limit-ms`, and `buffer-oldest-buffered-at-ms`. Both the metrics and the audit are
notified from the same internal trigger points, but they serve different purposes. The metrics are
always on, aggregate only, and feed JMX and the Streams metrics reporters. `CausalAudit` is optional,
per-record, and routed wherever you supply it. Registering an audit has no effect on the metrics, and
the metrics have no effect on the audit.

## Failure handling

An exception thrown from any `CausalAudit` method is caught and logged by Parsley. It never fails the
record or the Streams task, so the audit is fail-open. A slow implementation will, however, add
latency to the calling thread, because these methods are called synchronously from the processing
path. Keep implementations fast and non-blocking, and hand off to a queue if the real destination is
slow.

## Thread-safety

One `CausalAudit` instance is shared across every task and partition the processor runs on, and each
task is owned by its own Kafka Streams thread. Implementations must be thread-safe.
