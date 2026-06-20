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
eviction. Every record reaches the user's `process()`/`poll()` exactly once, stamped under the
`parsley-causal-result` header:

```java
CausalResult.fromRecord(record)  // Optional<CausalResult>: SATISFIED or EVICTED
```

`SATISFIED` means the frontier had observed the record's dependencies by delivery time. `EVICTED`
means the configured `CausalBufferLimit` fired before that happened, and the record was forwarded
anyway. The frontier always advances on delivery, so buffered records waiting on that coordinate
are not permanently stalled.

There is no separate violation callback to configure — react to an `EVICTED` delivery by checking
the header in your own `process()`/`poll()` code (custom metrics, alerting, routing to your own
dead-letter sink, etc.). Parsley itself logs every eviction at `WARN` with the causal gap (the
per-coordinate shortfall between what was required and what the frontier had observed) and counts
it via its eviction metric — useful for diagnosis, not for programmatic reaction.

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
