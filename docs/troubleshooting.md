# Troubleshooting

## A held record can no longer be deserialised (poison record)

Parsley persists records whose causal dependencies are not yet satisfied to a changelog-backed buffer
store, serialised with the `ParsleyBuffer` serdes. When such a record is later forwarded, whether on
drain or on limit-driven eviction, Parsley deserialises it with that serde. If the serde now fails,
most often because of a Schema Registry change while the record was buffered, the record is a poison
record.

The Avro and Schema Registry case is worth spelling out. The Confluent wire format embeds the writer
schema id in the bytes, and decode fetches the writer schema by that id. Registering an incompatible
new version on its own therefore does not break a buffered record. Decode fails only when the writer
schema id is hard-deleted, when the redeployed reader schema is incompatible, or when the
deserializer resolves against the subject's latest schema.

### What Parsley does

The behaviour is set by `parsley.properties`. This is a Parsley-specific config surface and is not
mapped from Streams' exception handlers, because dropping a buffered record has causal-frontier
consequences that a source-record skip does not.

```properties
# parsley.properties (on the classpath)
parsley.buffer.deserialization.failure.policy = fail   # or: continue
```

| `parsley.buffer.deserialization.failure.policy` | Behaviour on a poison record |
|---|---|
| `fail` (the default) | Fail fast. The Streams client shuts down, and the record stays in the buffer changelog for recovery. |
| `continue` | Skip. The record is dropped, logged, and counted as a violation, and processing continues. |

> The `continue` policy is best-effort and lossy in this first version. The dropped record's contents
> are gone for good. Its coordinate is still accounted for, exactly like an eviction, so dropping it
> closes the gap it sat in. A dependent of the dropped record, meaning one that named its exact
> coordinate, is then later delivered as satisfied even though the contents of its premise were never
> delivered to anyone. Prefer `fail` unless you specifically need liveness over correctness. A durable
> quarantine store with operator-triggered redelivery is planned to supersede `continue`.

In both cases the following hold.

- The JVM is never crashed. The error is a `RuntimeException` that is fatal only to the task.
- Startup is never blocked. Index restore decodes only Parsley's own framing and never your serde, so
  a poison record can be restored and only fails when it is actually forwarded.
- An `ERROR` line is logged with the held record's metadata, and never with the payload bytes.

  ```
  Buffered record could not be deserialised (deserialization handler = fail); failing fast.
  It remains in the buffer changelog for recovery. held record orders-0@42 (topicId C3o…, ts 178…);
  schema id: 1; dependencies: ParsleyClock{Wvr…-0@0}; header keys: [...]; key bytes: 1; value bytes: 15
  ```

### Recovery under the default fail-fast policy

1. Read the `ERROR` line. It names the source coordinate (`orders-0@42`) and the writer schema id
   that can no longer be decoded.
2. Make the bytes decodable again. Restore Schema Registry compatibility for the subject
   (`<topic>-value`), or roll the consumer or processor's reader schema back to one that is
   compatible with that writer schema id.
3. Restart. Startup succeeds, because restore is poison-immune. The record now decodes and drains
   normally.

To inspect the exact bytes, read them from the buffer's changelog topic
`{applicationId}-{storeName}-buffer-changelog`, where the default `storeName` is `parsley`, with a
console consumer. Parsley never logs the payload itself.

If the record is genuinely unrecoverable and you need liveness, set
`parsley.buffer.deserialization.failure.policy = continue` and restart. Parsley then drops the poison
record, logs it, counts it as a violation, and continues, accepting the lossiness noted above.

### Do not use Streams' exception handlers for this

Setting `processing.exception.handler=CONTINUE` does not help. Streams routes the failure through that
handler, but its `CONTINUE` mode skips the innocent record currently being processed, which is the
record whose arrival triggered the drain, rather than the buffered poison record. The poison record
stays, the next trigger hits it again, and the application sheds healthy records in a livelock. The
`deserialization.exception.handler` (`LogAndContinue`) does not apply either, because it covers
source-topic consumption rather than records decoded from Parsley's state store. Use
`parsley.buffer.deserialization.failure.policy` instead.
