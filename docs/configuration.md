# Configuration

## Buffer limits

A `CausalBufferLimit` bounds how large or how long-lived the causal buffer may grow before
eviction fires.

### Size limit

```java
CausalBufferLimit.ofSize(500)  // evict the oldest record(s) once the buffer holds ≥ 500 records
```

Fires synchronously during `process()` — eviction happens in the same call that pushes the buffer
over the limit, and evicts only the oldest records needed to bring the buffer back under the
limit (typically one per call). Younger buffered records are left alone.

The limit is also enforced once at startup, against whatever buffer was restored from the
changelog — so lowering the limit and restarting evicts the oldest excess records immediately,
rather than waiting for new traffic to retrigger the inline check above. This enforcement happens
shortly after startup, not necessarily before the very first record — but the buffer can't stay
over the limit either way: a record admitted in that window trips the inline check above instead.

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

## Always-forward delivery

Parsley never drops or diverts a record — there is no policy to configure for what happens on
eviction. Every record reaches the user's `process()`/`poll()` exactly once. In the common case it
is delivered in causal order; the exception is **eviction**, when the configured `CausalBufferLimit`
fires before a held record's dependencies are satisfied and the record is delivered anyway, out of
order. Eviction still feeds the frontier exactly like a normal delivery: once the evicted record's
coordinate closes its gap, buffered records waiting on it catch up in the same step, so they are not
permanently stalled.

Eviction is surfaced operationally, not as a per-record signal: Parsley logs every eviction at
`WARN` with the causal gap (the per-coordinate shortfall between what was required and what the
frontier had observed) and counts it via its eviction metric.

---

## Header size

The serialised `parsley-causal-dependencies` header is `5 + 28 × entries` bytes. It counts against
Kafka's record-size limit (`message.max.bytes` / `max.request.size`, ~1 MB default — there is no
separate header budget).

- **Automatic Streams stamping** stamps the per-task frontier, bounded by the number of source
  topics in the subtopology (one partition per topic per task). This stays small under normal
  topologies.
- **`CausalDependencies.fromRecord(trigger)`** carries only the partitions the upstream producer
  depended on — the recommended way to propagate causal context.
- **A manually built `CausalDependencies`** is as wide as the coordinates you `require(...)`. Watch
  this on records that legitimately depend on many topic-partitions.

Parsley never truncates the dependencies header — truncation would silently break the guarantee. Keep the
relevant-partition count within your record-size budget.

---

## `parsley.properties`

Parsley reads its own behaviour from a `parsley.properties` resource on the classpath — kept separate
from Kafka Streams configuration because these behaviours have no Streams equivalent. An absent file
(or absent keys) falls back to the defaults below.

```properties
# How a held record that can no longer be deserialised on the forward path is handled
# (e.g. an incompatible Schema Registry change while the record was buffered).
#   fail     (default) — fail fast; the record stays in the buffer changelog for recovery
#   continue           — drop the record (logged + violation metric) and keep processing
parsley.buffer.deserialization.failure.policy = fail
```

| Key | Default | Values |
|---|---|---|
| `parsley.buffer.deserialization.failure.policy` | `fail` | `fail`, `continue` |

`continue` is **best-effort and lossy** — see
[Troubleshooting → poison records](troubleshooting.md) for the full semantics (and why this is *not*
mapped from Streams' `deserialization.exception.handler`). A durable quarantine + operator-triggered
redelivery is planned to supersede it.
