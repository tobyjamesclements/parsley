# Plain Kafka clients

Topologies rarely begin and end at Streams applications. `CausalClock` gives plain Kafka producers
the same stamps a causal stage produces, and plain consumers need nothing at all.

## Producing with causal claims

A `CausalClock` is a running view of one producing thread's causal past. Fold what you observed,
fold your own acknowledgements, and stamp:

```java
CausalClock clock = new CausalClock(admin);   // an org.apache.kafka.clients.admin.Admin the caller owns

// Anything this producer's next records causally depend on:
for (ConsumerRecord<String, String> rec : consumer.poll(timeout)) {
    handle(rec);
    clock.observe(rec);              // folds the record's coordinate and its carried clock
}

ProducerRecord<String, String> out = new ProducerRecord<>("orders", key, value);
clock.stamp(out.headers());          // attach the dependency clock
RecordMetadata meta = producer.send(out).get();   // sequential producer: wait for the ack
clock.recordProduced(meta);          // the next send will claim this one
```

The sequential pattern — send, await the acknowledgement, fold it — is what orders a
producer's own sends across topics and partitions. A producer that pipelines sends without
folding acknowledgements still produces valid stamps; it simply does not claim its own
in-flight records, so their mutual order is not asserted.

A producer that stamps nothing claims nothing: its records deliver immediately everywhere,
and downstream ordering through them is not constrained. That is the safe default for topics
outside the causal path, and the reason incremental adoption needs no flag day — stamp the
producers whose ordering matters, one at a time.

## Consuming

Plain consumers need no Parsley awareness. The only trace of Parsley on the wire is a few
headers (`CausalHeaders.CLOCK` and the sender tag, readable via `CausalHeaders.readSender` /
`readSeq` for observability), which a consumer may ignore entirely.

A plain consumer that wants causal *observation* — so that records it later produces claim
what it read — uses `observe` as above. A plain consumer that wants causal *delivery order*
is, by definition, a causal stage: run it as one.
