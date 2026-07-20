# Troubleshooting

## A held record can no longer be deserialised (poison record)

Parsley persists records whose causal dependencies are not yet satisfied to a changelog-backed buffer
store, serialised with the `ParsleySource` serdes. When such a record is later forwarded, Parsley
deserialises it with that serde. If the serde now fails, most often because of a Schema Registry change
while the record was buffered, the record is a poison record.

The Avro and Schema Registry case is worth spelling out. The Confluent wire format embeds the writer
schema id in the bytes, and decode fetches the writer schema by that id. Registering an incompatible
new version on its own therefore does not break a buffered record. Decode fails only when the writer
schema id is hard-deleted, when the redeployed reader schema is incompatible, or when the
deserializer resolves against the subject's latest schema.

### What Parsley does

Delivery is unconditionally fail-closed: there is no configuration that trades causal safety for
liveness, and no third disposition besides forward or buffer. A poison record always fails the task
fast — the record is never dropped and never forwarded on an unproven premise.

- The JVM is never crashed. The error is a `RuntimeException` that is fatal only to the task, which
  Kafka Streams restarts.
- Startup is never blocked. Index restore decodes only Parsley's own framing and never your serde, so
  a poison record can be restored and only fails when it is actually forwarded.
- An `ERROR` line is logged with the held record's metadata, and never with the payload bytes.

  ```
  Buffered record could not be deserialised; failing fast (fail-closed). It remains in the buffer
  changelog for recovery. held record orders-0@42 (topicId C3o…, ts 178…) (writer schema id 1);
  dependencies: ParsleyVectorClock{Wvr…-0@0}; header keys: [...]; key bytes: 1; value bytes: 15
  ```

### Recovery

1. Read the `ERROR` line. It names the source coordinate (`orders-0@42`) and the writer schema id
   that can no longer be decoded.
2. Make the bytes decodable again. Restore Schema Registry compatibility for the subject
   (`<topic>-value`), or roll the consumer or processor's reader schema back to one that is
   compatible with that writer schema id.
3. Restart. Startup succeeds, because restore is poison-immune. The record now decodes and drains
   normally.

To inspect the exact bytes, read them from the buffer's changelog topic
`{applicationId}-{storeName}-buffer-changelog`, where `storeName` is the stage's name, with a console
consumer. Parsley never logs the payload itself.

### Do not use Streams' exception handlers for this

Setting `processing.exception.handler=CONTINUE` does not help. Streams routes the failure through that
handler, but its `CONTINUE` mode skips the innocent record currently being processed, which is the
record whose arrival triggered the drain, rather than the buffered poison record. The poison record
stays, the next trigger hits it again, and the application sheds healthy records in a livelock. The
`deserialization.exception.handler` (`LogAndContinue`) does not apply either, because it covers
source-topic consumption rather than records decoded from Parsley's state store. There is no Parsley
config to set instead — recovery is always the schema-fix-and-restart path above.

---

## An inbound record's causal-clock header cannot be decoded

A record's causal dependencies travel in its `parsley-causal-clock` header. When that header is
present but cannot be decoded into a clock — a corrupt or truncated header, or one written in an
unsupported wire version — Parsley fails the task fast at ingest rather than forward the record on an
unknown premise. The record was never buffered and its source offset is not committed past it, so it is
reprocessed on the next attempt, after a restart or once the upstream is fixed.

```
Unresolvable causal-clock header on orders-0 @42; failing fast (fail-closed). ...
```

Recovery is the same shape as the poison-record case: fix or roll back whatever upstream change
produced the undecodable header, then restart.

---

## A record depends on a coordinate this node has no input channel for

This is not a failure: the gate ignores a dependency on a coordinate this node does not consume —
an undeclared topic, or a partition a different task instance owns — and counts each ignored
coordinate on the `deps-out-of-scope-ignored` sensor. Ignoring is sound because stamps are
transitively complete and merged unconditionally: any consumed causal ancestor of a record is
claimed directly in that record's own clock, so the unconsumed entry only proxies ancestry the
clock already states.

Producers stamp a clock spanning everything they consume, so a downstream stage with a narrower
scope sees out-of-scope coordinates routinely — a steady rate on the sensor is normal there. A
sustained rate where you expected full coverage is worth investigating: it can indicate a missing
subscription, a co-partitioning mistake (also flagged by
[`parsley.topology.validation`](configuration.md)), or two deployments unintentionally sharing a
topic.

---

## A causal topic was deleted or recreated while the application ran

Topic names are resolved to their stable Kafka UUIDs once, at task initialisation, and causal
identity is bound to the UUID for the process lifetime. If a causal topic — an input or a sink — is
deleted (or deleted and recreated under the same name) while the application runs, `CausalStreams`
detects the change through a background topic-identity poll and fails the application fast the next
time a task processes or stamps a record:

```
causal topic 'prices' changed UUID from ... (resolved at init) to ... — it was deleted and
recreated while this member ran. Channel identity is bound per process lifetime (E1): records of
a recreated topic would be ingested and stamped under the old UUID, rebinding causal coordinates. ...
```

Depending on timing, the failure can instead surface as Kafka's own missing-source-topic rebalance
error or, for a recreated input whose new log is still short, the out-of-range failure the
`AutoOffsetReset.none()` sources fail closed under. All three are the same verdict: the member
stops rather than processing the new incarnation's records under the old identity. Records fetched
in the short window before detection are the residual exposure, so treat live deletion or
recreation of a causal topic as an operational error, like letting retention outrun a lagging
consumer. Restarting after a recreation is safe: identity is re-resolved at init, and the old
incarnation's history reads as lost, never reordered.

---

## Retention outran a causal consumer

Kafka retention (or an explicit `deleteRecords`) can expunge records a lagging or replaying causal
consumer has not yet delivered. No protocol can deliver a destroyed record, so Parsley fails closed
rather than guessing: every causal source is configured with `AutoOffsetReset.none()`, and a
consumer whose committed position has fallen below the log-start offset fails fast with Kafka's
out-of-range error instead of silently jumping past the destroyed causes. Parsley seeds log-start
offsets only on a genuine first start — a group whose offsets merely expired while its state
survived is refused, because replaying from log-start into surviving state could reorder history.

The result is a crash-loop until an operator intervenes. That is deliberate: a liveness stall,
never a reorder. Recovery is an explicit operator decision — accept the history loss and reset the
application (delete its state and offsets, so it genuinely first-starts against the log that still
exists), or restore the missing history from upstream if you can. Prevent it by sizing retention
on causal topics to comfortably exceed the longest consumer outage or replay you intend to
survive. A joiner that first-starts against an already-truncated log is fine: the expunged prefix
is skipped soundly (the frontier seeds at log-start), but what was lost is lost.

---

## Sustained buffer growth

If `buffer-depth` (see [Configuration](configuration.md#metrics)) grows without bound instead of
draining, a dependency is genuinely stuck — a lagging partition, a co-partitioning problem, or a
producing topic that was deleted. The buffer is unconditionally unbounded, so a stuck dependency
accumulates records rather than being evicted or dropped. Each held record's debug-level `Holding` log
line identifies the specific coordinate still missing at the time; correlate it against the lagging
topic-partition's own consumer lag.

A sharper signal for one class of stall: the `records-held-above-highest-received` gauge counts
held records whose missing dependency is *above* the highest offset ever received on its channel —
nothing received so far can satisfy the claim, which is the signature of a producer whose send
failed after being stamped elsewhere, or of a channel that has gone permanently silent (a
conservative over-claim in a stamp waits out the same way). The hold is fail-safe, never unsafe,
but the delay is unbounded until that channel produces again, so the gauge (and its `WARN` log
line) is the thing to alert on when buffer growth has no matching consumer lag.
