# Troubleshooting

## A held record can no longer be deserialised (poison record)

Parsley persists records whose causal dependencies are not yet satisfied to a changelog-backed
**buffer store**, serialised with the `CausalBuffer` serdes. When such a record is later forwarded
(on drain, or on limit-driven eviction) Parsley deserialises it with that serde. If the serde now
fails — most often a **Schema Registry change while the record was buffered** — the record is a
*poison record*.

How this arises (Avro / Schema Registry): the Confluent wire format embeds the **writer schema id**
in the bytes, and decode fetches the writer schema *by id*, so registering an incompatible *new*
version alone does **not** break a buffered record. Decode fails only when the writer schema id is
hard-deleted, the redeployed reader schema is incompatible, or the deserializer resolves against the
subject's *latest* schema.

### What Parsley does

The behaviour is set by **`parsley.properties`** (a Parsley-specific config surface — this is *not*
mapped from Streams' exception handlers, because dropping a buffered record has causal-frontier
consequences a source-record skip does not):

```properties
# parsley.properties (on the classpath)
parsley.buffer.deserialization.failure.policy = fail   # or: continue
```

| `parsley.buffer.deserialization.failure.policy` | Behaviour on a poison record |
|---|---|
| `fail` (**default**) | **Fail fast.** The Streams client shuts down; the record stays in the buffer changelog for recovery. |
| `continue` | **Skip.** The record is dropped, logged, and counted as a violation; processing continues. |

> **`continue` is best-effort and lossy (v1).** The dropped record's contents are gone for good — but
> its coordinate is still accounted for, exactly like an eviction: dropping it closes the gap it sat
> in, so a *dependent* of the dropped record (one that named its exact coordinate) is later
> delivered as satisfied, on a premise whose actual contents were never delivered to anyone. Prefer
> `fail` unless you specifically need liveness over correctness. A durable quarantine store plus
> operator-triggered redelivery is planned to supersede `continue`.

Either way:

- The JVM is **never** crashed — the error is a `RuntimeException` fatal only to the task.
- **Startup is never blocked** — index restore decodes only Parsley's own framing, never your serde,
  so a poison record can be restored and only fails when actually forwarded.
- An `ERROR` line is logged with the held record's **metadata, never the payload bytes**:

  ```
  Buffered record could not be deserialised (deserialization handler = fail); failing fast.
  It remains in the buffer changelog for recovery. held record orders-0@42 (topicId C3o…, ts 178…);
  schema id: 1; dependencies: ParsleyClock{Wvr…-0@0}; header keys: [...]; key bytes: 1; value bytes: 15
  ```

### Recovery (fail-fast / default)

1. Read the `ERROR` line: it names the **source coordinate** (`orders-0@42`) and the **writer schema
   id** that can no longer be decoded.
2. Make the bytes decodable again — restore Schema Registry compatibility for the subject
   (`<topic>-value`), or roll back the consumer/processor's reader schema to one compatible with that
   writer schema id.
3. Restart. Startup succeeds (restore is poison-immune); the record now decodes and drains normally.

To inspect the exact bytes, read them from the buffer's changelog topic
`{applicationId}-{storeName}-buffer-changelog` (default `storeName` is `parsley`) with a console
consumer — Parsley never logs the payload itself.

If the record is genuinely unrecoverable and you need liveness, set
`parsley.buffer.deserialization.failure.policy = continue` and restart; Parsley will drop the poison
record (logged + violation metric) and continue — accepting the lossiness noted above.

### Do not use Streams' exception handlers for this

Setting `processing.exception.handler=CONTINUE` does **not** help: Streams routes the failure through
that handler, but its `CONTINUE` mode skips the *innocent record currently being processed* — the one
whose arrival triggered the drain — not the buffered poison record. The poison record stays, the next
trigger hits it again, and the app sheds healthy records in a livelock. Likewise
`deserialization.exception.handler` (`LogAndContinue`) does not apply — it covers *source-topic*
consumption, not records decoded from Parsley's state store. Use
`parsley.buffer.deserialization.failure.policy` instead.
