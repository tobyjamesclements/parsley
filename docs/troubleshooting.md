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

## An inbound record's causal-dependencies header cannot be decoded

A record's causal dependencies travel in its `parsley-causal-dependencies` header. When that header is
present but cannot be decoded into a clock — a corrupt or truncated header, or one written in an
unsupported wire version — Parsley fails the task fast at ingest rather than forward the record on an
unknown premise. The record was never buffered and its source offset is not committed past it, so it is
reprocessed on the next attempt, after a restart or once the upstream is fixed.

```
Unresolvable causal-dependencies header on orders-0 @42; failing fast (fail-closed). ...
```

Recovery is the same shape as the poison-record case: fix or roll back whatever upstream change
produced the undecodable header, then restart.

---

## A record depends on a coordinate this node has no input channel for

Parsley cannot silently treat an unreachable dependency as satisfied — it can only prove it has no way
to check it, never that the coordinate is genuinely irrelevant. A dependency naming an undeclared
topic, or a partition a different task instance owns, fails the task fast:

```
orders-0 @42 depends on a coordinate this node has no channel for; failing fast (fail-closed). The
record was not forwarded and is reprocessed on restart.
```

This usually means the topology is missing a subscription: either a genuine bug (the producer stamped
a dependency on a topic this stage never registered), or, under topology-epoch coordination, a stage
that does not cover the full coordinated domain — see
[`parsley.coordination.domain-topics`](configuration.md) and
[Evolving a running topology](streams.md#evolving-a-running-topology) for wiring a passthrough source
to cover it without a redundant business subscription.

---

## Sustained buffer growth

If `buffer-depth` (see [Configuration](configuration.md#metrics)) grows without bound instead of
draining, a dependency is genuinely stuck — a lagging partition, a co-partitioning problem, or a
producing topic that was deleted. The buffer is unconditionally unbounded, so a stuck dependency
accumulates records rather than being evicted or dropped. Each held record's debug-level `Holding` log
line identifies the specific coordinate still missing at the time; correlate it against the lagging
topic-partition's own consumer lag.
