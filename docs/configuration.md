# Configuration

## Buffer limits

A `CausalBufferLimit` bounds how large or how long-lived the causal buffer may grow before it
fires — what firing actually does (evict, or fail fast) is governed by
[`parsley.buffer.eviction.failure.policy`](#eviction-fail-fast-or-forward-anyway) below; this
section describes when each limit kind fires.

### Size limit

```java
CausalBufferLimit.ofSize(500)  // fires once the buffer holds ≥ 500 records
```

Fires synchronously during `process()` — in the same call that pushes the buffer over the limit,
on only the oldest records needed to bring the buffer back under the limit (typically one per
call). Younger buffered records are left alone.

The limit is also enforced once at startup, against whatever buffer was restored from the
changelog — so lowering the limit and restarting acts on the oldest excess records immediately,
rather than waiting for new traffic to retrigger the inline check above. This enforcement happens
shortly after startup, not necessarily before the very first record — but the buffer can't stay
over the limit either way: a record admitted in that window trips the inline check above instead.

### Duration limit

```java
CausalBufferLimit.ofDuration(Duration.ofSeconds(30))
```

Fires on a scheduled basis. The processor calls `evictExpired()` at the configured interval, which
acts only on the records that have individually aged past `duration`, leaving younger records
held; Parsley wires this schedule automatically when using `CausalProcessors`.

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

## Eviction: fail fast, or forward anyway

When the configured `CausalBufferLimit` fires before a held record's dependencies are satisfied,
`parsley.buffer.eviction.failure.policy` decides what happens:

- **`fail`** (default): fail the task fast, leaving the candidate record(s) buffered rather than
  deliver them out of causal order — trades availability for consistency. The record(s) are
  retried on the next attempt (after a restart, or once the limit/backlog eases).
- **`continue`**: Parsley's original always-forward behaviour — never drops or diverts a record.
  The configured limit evicts the oldest qualifying record(s) and delivers them anyway, out of
  causal order. Eviction still feeds the frontier exactly like a normal delivery: once the evicted
  record's coordinate closes its gap, buffered records waiting on it catch up in the same step, so
  they are not permanently stalled.

Either way, eviction is surfaced operationally, not as a per-record signal: Parsley logs every
eviction (or fail-fast) at `WARN`/`ERROR` with the causal gap (the per-coordinate shortfall between
what was required and what the frontier had observed) and counts it via its eviction (or
eviction-limit-exceeded) metric.

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

# How a CausalBufferLimit firing (eviction) is handled.
#   fail     (default) — fail fast; the candidate record(s) stay buffered rather than be
#                         delivered out of causal order — trades availability for consistency
#   continue           — evict and forward the record(s) anyway, out of causal order
#                         (logged + violation metric) — Parsley's original always-forward behaviour
parsley.buffer.eviction.failure.policy = fail
```

| Key | Default | Values |
|---|---|---|
| `parsley.buffer.deserialization.failure.policy` | `fail` | `fail`, `continue` |
| `parsley.buffer.eviction.failure.policy` | `fail` | `fail`, `continue` |

`parsley.buffer.deserialization.failure.policy = continue` is **best-effort and lossy** — see
[Troubleshooting → poison records](troubleshooting.md) for the full semantics (and why this is *not*
mapped from Streams' `deserialization.exception.handler`). A durable quarantine + operator-triggered
redelivery is planned to supersede it.

`parsley.buffer.eviction.failure.policy = fail` means a sustained causal gap (a dependency that
never arrives) will repeatedly fail the task once the buffer limit is reached, rather than ever
deliver out of order — set `continue` if availability matters more than strict causal order for
your use case.
