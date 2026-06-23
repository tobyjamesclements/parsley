# Audit logging

`CausalAudit` is an optional, client-supplied seam that receives every per-record event in the
causal-buffering lifecycle — forwarded, held, released, evicted (causal violation), and
undecodable — plus processor startup/shutdown. Register one to route these events wherever your
audit or compliance trail needs them: a SIEM, a durable audit store, structured logs. Parsley never
decides where they go; without one registered, events are simply discarded.

## Registering an audit

Implement `CausalAudit` and pass it to the builder:

```java
CausalProcessorSupplier<String, Order, String, Enriched> causal =
        CausalProcessors.builder(user)
                .addBufferStore("parsley", CausalBufferLimit.ofDuration(limit))
                .addBuffers(List.of("prices", "orders"), Serdes.String(), orderSerde)
                .withAudit(mySiemAudit)
                .build();
```

Without `.withAudit(...)`, Parsley uses `CausalAudit.NOOP` — no events are emitted, with no overhead.

## Events

| Method | Fires when | Carries |
|---|---|---|
| `recordForwarded` | a record's dependencies were already satisfied; delivered without buffering | topic, partition, offset |
| `recordHeld` | a record's dependencies were unsatisfied; admitted to the buffer to wait | topic, partition, offset, buffer depth after admission, the unmet gap |
| `recordReleased` | a previously held record became satisfiable and drained, in causal order | topic, partition, offset, buffer depth after release |
| `recordViolation` | a held record was evicted and forwarded out of order because a `CausalBufferLimit` fired first — a causal violation | topic, partition, offset, the gap still unmet at eviction |
| `recordDeserializationFailure` | a held record could no longer be deserialised on the forward path (e.g. an incompatible Schema Registry change while buffered) | topic, partition, offset, an operator-facing reason string, whether the record was dropped or left buffered for recovery |
| `processorInitialized` | a task's processor started up | task id, whether a persisted frontier was restored |
| `processorClosing` | a task's processor is shutting down | task id |

Every method carries only topic/partition/offset/causal-metadata — never the record's key or
value, matching Parsley's own logs.

## Relationship to Parsley's metrics

`CausalAudit` is independent of, and parallel to, the aggregate counters Parsley always wires into
the Kafka Streams metrics registry (`records-buffered-total`, `records-released-total`,
`buffer-depth`, the violation and deserialization-error counters). Both are notified from the same
internal trigger points, but serve different purposes: the metrics are always-on, aggregate-only,
and feed JMX/Streams' own metrics reporters; `CausalAudit` is optional, per-record, and routed
wherever you supply it. Registering an audit has no effect on the metrics, and vice versa.

## Failure handling

An exception thrown from any `CausalAudit` method is caught and logged by Parsley — it never fails
the record or the Streams task (fail-open). A slow implementation will, however, add latency to the
calling thread, since these methods are called synchronously from the processing path: keep
implementations fast and non-blocking, and hand off to a queue if the real destination is slow.

## Thread-safety

One `CausalAudit` instance is shared across every task and partition the processor runs on, each
owned by its own Kafka Streams thread. Implementations must be thread-safe.
