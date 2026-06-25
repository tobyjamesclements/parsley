# Configuration

## Buffer limits

A `CausalBufferLimit` bounds how large or how long-lived the causal buffer may grow before it fires.
What firing actually does, whether it evicts the record or fails the task fast, is governed by
[`parsley.buffer.eviction.failure.policy`](#eviction-fail-fast-or-forward-anyway) below. This section
describes when each kind of limit fires.

### Size limit

```java
CausalBufferLimit.ofSize(500)  // fires once the buffer holds at least 500 records
```

The size limit fires synchronously during `process()`, in the same call that pushes the buffer over
the limit. It acts on only the oldest records needed to bring the buffer back under the limit, which
is typically one record per call. Younger buffered records are left alone.

The limit is also enforced once at startup, against whatever buffer was restored from the changelog.
Lowering the limit and restarting therefore acts on the oldest excess records immediately, rather
than waiting for new traffic to retrigger the inline check above. This enforcement happens shortly
after startup, and not necessarily before the very first record. The buffer cannot stay over the
limit either way, because a record admitted in that window trips the inline check above instead.

### Duration limit

```java
CausalBufferLimit.ofDuration(Duration.ofSeconds(30))
```

The duration limit fires on a scheduled basis. The processor calls `evictExpired()` at the configured
interval, which acts only on the records that have individually aged past `duration` and leaves
younger records held. Parsley wires this schedule automatically when you use `CausalProcessors`.

### First-of (composite)

```java
CausalBufferLimit.first(
    CausalBufferLimit.ofSize(1000),
    CausalBufferLimit.ofDuration(Duration.ofSeconds(60))
)
```

The first-of limit fires when the first of several limits fires. Use it to combine a size cap with a
time-based backstop.

---

## Eviction: fail fast, or forward anyway

When the configured `CausalBufferLimit` fires before a held record's dependencies are satisfied,
`parsley.buffer.eviction.failure.policy` decides what happens.

- **`fail`** (the default) fails the task fast and leaves the candidate records buffered, rather than
  deliver them out of causal order. This trades availability for consistency. The records are retried
  on the next attempt, which is after a restart or once the limit or backlog eases.
- **`continue`** never drops or diverts a record. The configured limit evicts the oldest qualifying records and delivers them
  anyway, out of causal order. Eviction still feeds the frontier exactly like a normal delivery, so
  once the evicted record's coordinate closes its gap, buffered records waiting on it catch up in the
  same step and are not permanently stalled.

Either way, eviction is surfaced operationally rather than as a per-record signal. Parsley logs every
eviction or fail-fast at `WARN` or `ERROR` with the causal gap, which is the per-coordinate shortfall
between what was required and what the frontier had observed. It also counts the event through its
eviction metric, or its eviction-limit-exceeded metric under the `fail` policy.

---

## Unresolvable clock: fail fast, or forward empty

A record's causal dependencies travel in its `parsley-causal-dependencies` header. When that header is
present but cannot be decoded into a clock, a corrupt or truncated header, or one written in an
unsupported wire version, `parsley.clock.resolution.failure.policy` decides what happens at ingest.

- **`fail`** (the default) fails the task fast rather than forward the record on an unknown premise.
  The record was never buffered and its source offset is not committed past it, so it is reprocessed
  on the next attempt, which is after a restart or once the upstream is fixed.
- **`continue`** treats the unresolvable header as empty (vacuously satisfied) and forwards the record
  immediately. This is best-effort. It is lossy of causal premises:
  because the frontier is a high-water mark, forwarding on empty dependencies can let the record, and
  its dependents, be delivered ahead of a premise the corrupt header actually carried.

Either way the occurrence is surfaced operationally. Parsley logs it at `ERROR` (`fail`) or `WARN`
(`continue`) with the source coordinate, counts it through its clock-resolution-error metric, and
under `continue` also counts a violation.

---

## Header size

The serialised `parsley-causal-dependencies` header is `5 + 28 × entries` bytes. It counts against
Kafka's record-size limit (`message.max.bytes` and `max.request.size`, around 1 MB by default), and
there is no separate header budget.

- **Automatic Streams stamping** stamps the per-task frontier, which is bounded by the number of
  source topics in the subtopology, at one partition per topic per task. This stays small under
  normal topologies.
- **`CausalDependencies.fromRecord(trigger)`** carries only the partitions the upstream producer
  depended on. This is the recommended way to propagate causal context.
- **A manually built `CausalDependencies`** is as wide as the coordinates you `require(...)`. Watch
  this on records that legitimately depend on many topic-partitions.

Parsley never truncates the dependencies header, because truncation would silently break the
guarantee. Keep the relevant-partition count within your record-size budget.

---

## `parsley.properties`

Parsley reads its own behaviour from a `parsley.properties` resource on the classpath. This is kept
separate from Kafka Streams configuration because these behaviours have no Streams equivalent. An
absent file, or an absent key, falls back to the defaults below.

```properties
# How a held record that can no longer be deserialised on the forward path is handled
# (for example, an incompatible Schema Registry change while the record was buffered).
#   fail     (default) fails fast; the record stays in the buffer changelog for recovery
#   continue           drops the record (logged and counted) and keeps processing
parsley.buffer.deserialization.failure.policy = fail

# How a CausalBufferLimit firing (eviction) is handled.
#   fail     (default) fails fast; the candidate records stay buffered rather than be
#                      delivered out of causal order, which trades availability for consistency
#   continue           evicts and forwards the records anyway, out of causal order
#                      (logged and counted)
parsley.buffer.eviction.failure.policy = fail

# How an inbound record whose causal-dependencies header cannot be decoded is handled at ingest
# (for example, a corrupt or truncated header, or one in an unsupported wire version).
#   fail     (default) fails fast; the record is reprocessed on restart rather than forwarded
#                      on an unknown premise
#   continue           forwards the record with empty (vacuously satisfied) dependencies,
#                      out of causal order (logged and counted as a violation)
parsley.clock.resolution.failure.policy = fail
```

| Key | Default | Values |
|---|---|---|
| `parsley.buffer.deserialization.failure.policy` | `fail` | `fail`, `continue` |
| `parsley.buffer.eviction.failure.policy` | `fail` | `fail`, `continue` |
| `parsley.clock.resolution.failure.policy` | `fail` | `fail`, `continue` |

`parsley.buffer.deserialization.failure.policy = continue` is best-effort and lossy. See
[Troubleshooting](troubleshooting.md) for the full semantics, including why it is not mapped from
Streams' `deserialization.exception.handler`. A durable quarantine and operator-triggered redelivery
is planned to supersede it.

`parsley.buffer.eviction.failure.policy = fail` means that a sustained causal gap, where a dependency
never arrives, repeatedly fails the task once the buffer limit is reached, rather than ever deliver
out of order. Set it to `continue` if availability matters more than strict causal order for your use
case.

`parsley.clock.resolution.failure.policy = fail` means that a record carrying a corrupt
causal-dependencies header fails the task rather than be forwarded on unknown premises. Set it to
`continue` to forward such a record with empty dependencies, best-effort, accepting that this is
lossy of causal order.
