# Configuration

## Buffer limits

A `CausalBufferLimit` bounds how large or how long-lived the causal buffer may grow before the
policy fires.

### Size limit

```java
CausalBufferLimit.ofSize(500)  // evict when the buffer holds ≥ 500 records
```

Fires synchronously during `process()` — the eviction happens in the same call that pushes the
buffer over the limit.

### Duration limit

```java
CausalBufferLimit.ofDuration(Duration.ofSeconds(30))
```

Fires on a scheduled basis. The processor calls `evictNow()` at the configured interval; Parsley
wires this schedule automatically when using `CausalProcessors`.

### First-of (composite)

```java
CausalBufferLimit.first(
    CausalBufferLimit.ofSize(1000),
    CausalBufferLimit.ofDuration(Duration.ofSeconds(60))
)
```

Fires when the first of several limits fires. Use this to combine a size cap with a time-based
backstop.

---

## Buffer policies

A `CausalBufferPolicy` pairs a limit with a handling strategy for evicted records. Every policy
reports a `CausalViolation` for each evicted record.

### Forward unsafe

```java
CausalBufferPolicy.forwardUnsafe(limit)
```

Forwards evicted records out-of-order. Lenient: delivery is always preserved; causal ordering is
suspended for the evicted batch. Each forwarded record is reported as a violation with reason
`LIMIT_REACHED`.

### Drop

```java
CausalBufferPolicy.drop(limit)
```

Discards evicted records entirely. Strict: no out-of-order delivery, but records are lost.

### Dead letter

```java
CausalBufferPolicy.deadLetter(limit, "parsley-dlq")
```

Routes evicted records to a named dead-letter topic (via a sink you provide). Strict: no
out-of-order delivery. Each routed record receives three additional headers:

| Header | Content |
|---|---|
| `parsley-dlq-reason` | `LIMIT_REACHED` (UTF-8) |
| `parsley-dlq-required-clock` | The required `CausalDependencies` at eviction time (serialised) |
| `parsley-dlq-gap` | The per-coordinate shortfall (serialised as `CausalDependencies`) |

The gap headers allow an operator to reconstruct exactly which offsets were missing when the
record was evicted.

---

## Violation handler

```java
CausalProcessors.builder(user, policy)
        .onViolation(violation -> myMetrics.increment("causal.violations",
                "reason", violation.reason().name()))
        ...
```

`CausalViolationHandler` receives a `CausalViolation` for every record that cannot be delivered
in causal order. The violation includes:

- `reason()` — `MISSING_HEADER`, `UNRESOLVABLE_CLOCK`, or `LIMIT_REACHED`
- `frontier()` — the consumer's frontier at the time of the violation
- `required()` — the clock the record carried (empty for `MISSING_HEADER`/`UNRESOLVABLE_CLOCK`)
- `gap()` — per-coordinate shortfall list (empty if already above the frontier)
- `record()` — the underlying `ConsumerRecord`

The default handler logs at `WARN` level. Override it to integrate with your own metrics or
alerting system. Violations are expected under sustained lag or misconfiguration — their presence
is not an error in isolation, but their *frequency* and *gap size* are the key signals to watch.

---

## Clock size

The serialised `parsley-causal-dependencies` header is `5 + 28 × entries` bytes. It counts against
Kafka's record-size limit (`message.max.bytes` / `max.request.size`, ~1 MB default — there is no
separate header budget).

- **Automatic Streams stamping** stamps the per-task frontier, bounded by the number of source
  topics in the subtopology (one partition per topic per task). This stays small under normal
  topologies.
- **`consumer.frontier()`** and **`CausalFrontier.toDependencies()`** carry every partition ever
  seen by the consumer. Watch this path on wide-fan-in consumers or broad regex subscriptions.
- **`CausalDependencies.fromRecord(trigger)`** carries only the partitions the upstream producer
  depended on. Prefer it over `frontier()` when the causal context is a single upstream record.

Parsley never truncates a clock — truncation would silently break the guarantee. Keep the
relevant-partition count within your record-size budget.
