# Migration

Adopting Parsley in a cluster where some producers do not yet stamp the `parsley-causal-dependencies`
header requires care: those legacy records arrive with no header and are classified as
`MISSING_HEADER` violations. The all-or-nothing convenience policies force an uncomfortable choice:

- `forwardUnsafe` — tolerate legacy producers, but also tolerate buffer overflow out-of-order.
- `drop` / `deadLetter` — enforce causal ordering, but drop or DLQ every record from legacy producers.

The per-violation-type builder resolves this by letting each violation reason carry its own action.

## Recommended migration strategy

### Phase 1 — introduce Parsley consumers, tolerate legacy producers

Configure a policy that forwards `MISSING_HEADER` records out-of-order while being strict about
records that do carry a Parsley header:

```java
CausalBufferPolicy policy = CausalBufferPolicy.builder()
        .onMissing(CausalViolationAction.FORWARD_UNSAFE)      // legacy producers pass through immediately
        .onUnresolvable(CausalViolationAction.DROP)            // corrupt header is a bug, not a migration artefact
        .onLimit(CausalViolationAction.DEAD_LETTER)            // buffer overflow → DLQ for investigation
        .setLimit(CausalBufferLimit.first(
                CausalBufferLimit.ofSize(10_000),
                CausalBufferLimit.ofDuration(Duration.ofSeconds(60))))
        .build();
```

During this phase, Parsley consumers are live but the causal guarantee applies only to records
originating from Parsley producers. Legacy records are forwarded immediately without buffering.

### Phase 2 — migrate producers one service at a time

Replace legacy producers with `CausalProducer` one service at a time. As each service migrates,
its records start arriving with a valid header and are held until their dependencies are satisfied.
The `FORWARD_UNSAFE` on `MISSING_HEADER` covers any remaining legacy services; the rest of the
cluster benefits from the guarantee immediately.

Monitor `MISSING_HEADER` violations via `CausalViolationHandler` to track which services remain
unmigrated:

```java
.onViolation(v -> {
    if (v.reason() == CausalViolationReason.MISSING_HEADER) {
        metrics.increment("parsley.legacy_records",
                "topic", v.record().topic());
    }
})
```

### Phase 3 — tighten the policy once all producers are migrated

Once the `MISSING_HEADER` violation count reaches zero, tighten the policy:

```java
CausalBufferPolicy policy = CausalBufferPolicy.drop(
        CausalBufferLimit.first(
                CausalBufferLimit.ofSize(10_000),
                CausalBufferLimit.ofDuration(Duration.ofSeconds(60))));
```

Or keep the asymmetric builder if you want different handling for different reasons, and change
`onMissing` to `DROP` to make any unexpected legacy record visible as an error.

## Notes

- The frontier always advances for `MISSING_HEADER` records regardless of action, so records
  buffered downstream of a legacy producer are not permanently stalled.
- `UNRESOLVABLE_DEPENDENCIES` is distinct from `MISSING_HEADER`: it means a header is present but
  cannot be deserialised. This is not expected during a migration and warrants a `DROP` or
  `DEAD_LETTER` action rather than `FORWARD_UNSAFE`.
- The per-violation-type policy works with both `CausalConsumers` and `CausalProcessors`. To
  dead-letter on a `CausalConsumer`-based deployment, provide a sink via
  `CausalConsumers.builder(...).deadLetterSink(...)` — required for any policy using `DEAD_LETTER`
  and forbidden otherwise. The sink receives the evicted record as a typed `ConsumerRecord<K,V>`
  carrying the `parsley-dlq-*` headers.
