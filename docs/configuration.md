# Configuration

## Buffer limits

A `CausalBufferLimit` bounds how large or how long-lived the causal buffer may grow before the
policy fires.

### Size limit

```java
CausalBufferLimit.ofSize(500)  // evict the oldest record(s) once the buffer holds ≥ 500 records
```

Fires synchronously during `process()` — eviction happens in the same call that pushes the buffer
over the limit, and evicts only the oldest records needed to bring the buffer back under the
limit (typically one per call). Younger buffered records are left alone.

### Duration limit

```java
CausalBufferLimit.ofDuration(Duration.ofSeconds(30))
```

Fires on a scheduled basis. The processor calls `evictExpired()` at the configured interval, which
evicts only the records that have individually aged past `duration`, leaving younger records held;
Parsley wires this schedule automatically when using `CausalProcessors`.

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

A `CausalBufferPolicy` pairs a limit with a handling strategy for evicted records via a
`ViolationAction` (`FORWARD_UNSAFE`, `DROP`, or `DEAD_LETTER`). Every policy reports a
`CausalViolation` for each evicted record.

The policy applies to all three violation reasons: records evicted when a limit fires
(`LIMIT_REACHED`) and records that arrive with missing or corrupt dependency headers
(`MISSING_HEADER`, `UNRESOLVABLE_DEPENDENCIES`). The frontier always advances for violation
records regardless of policy, so buffered records waiting on that coordinate are not permanently
stalled.

### Forward unsafe

```java
CausalBufferPolicy.forwardUnsafe(limit)
```

Forwards violation records out-of-order. Lenient: delivery is always preserved; causal ordering
is suspended for the violating record.

### Drop

```java
CausalBufferPolicy.drop(limit)
```

Discards violation records entirely. Strict: no out-of-order delivery, but records are lost.

### Dead letter

```java
CausalBufferPolicy.deadLetter(limit)
```

Routes violation records to a dead-letter sink you provide. Strict: no out-of-order delivery.
Each routed record receives three additional headers:

| Header | Content |
|---|---|
| `parsley-dlq-reason` | Violation reason (`LIMIT_REACHED`, `MISSING_HEADER`, or `UNRESOLVABLE_DEPENDENCIES`) (UTF-8) |
| `parsley-dlq-required-dependencies` | The required `CausalDependencies` (serialised; empty for header violations) |
| `parsley-dlq-gap` | The per-coordinate shortfall (serialised as `CausalDependencies`; empty for header violations) |

The reason and gap headers allow an operator to distinguish eviction from header violations and
reconstruct exactly which offsets were missing at eviction time.

### Per-violation-type policy

The convenience factories above apply the same action to every violation reason. The builder lets
each reason carry its own `ViolationAction`:

```java
CausalBufferPolicy policy = CausalBufferPolicy.builder()
        .onMissing(ViolationAction.FORWARD_UNSAFE)      // tolerate legacy (non-Parsley) producers
        .onUnresolvable(ViolationAction.DROP)            // corrupt header → discard
        .onLimit(ViolationAction.DEAD_LETTER)            // buffer overflow → DLQ
        .setLimit(CausalBufferLimit.ofSize(1000))
        .build();
```

All four settings (`onMissing`, `onUnresolvable`, `onLimit`, `setLimit`) are required; `build()`
throws `IllegalStateException` if any are omitted. A dead-letter sink is required if any action
is `DEAD_LETTER`, and forbidden otherwise.

See [Migration](migration.md) for the typical use of this builder when integrating Parsley with
an existing cluster.

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

- `reason()` — `MISSING_HEADER`, `UNRESOLVABLE_DEPENDENCIES`, or `LIMIT_REACHED`
- `frontier()` — the consumer's frontier at the time of the violation
- `required()` — the dependencies the record carried (empty for `MISSING_HEADER`/`UNRESOLVABLE_DEPENDENCIES`)
- `gap()` — per-coordinate shortfall list (empty if already above the frontier)
- `record()` — the underlying `ConsumerRecord`

The default handler logs at `WARN` level. Override it to integrate with your own metrics or
alerting system. Violations are expected under sustained lag or misconfiguration — their presence
is not an error in isolation, but their *frequency* and *gap size* are the key signals to watch.

---

## Header size

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

Parsley never truncates the dependencies header — truncation would silently break the guarantee. Keep the
relevant-partition count within your record-size budget.
