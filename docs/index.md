# Parsley

Kafka orders records within a topic-partition and orders nothing between partitions. Parsley
supplies the missing cross-channel guarantee.

> If message A is a cause of message B, every process that delivers both delivers A first.

The guarantee holds for the whole lifetime of a process, across restarts, rebalances and
partition reassignment.

## Scope

Parsley is a library over Kafka Streams. One declared process becomes one Kafka Streams
application running under `exactly_once_v2` and `read_committed`. State changes, sends and
consumed read positions commit in a single transaction.

The delivery discipline is that of causal broadcast, indexed by channel rather than by
process. Causality is Lamport's happened-before: same-process precedence, plus the edge from
a delivery to any later send, closed transitively. A causes B when a delivery of A happened
before B was sent.

Where the guarantee cannot be upheld, a process stops. Waiting is never resolved by a
timeout, and a held message is released only by evidence.

## Shape of an application

```java
var orders    = Channel.of("orders", Serdes.String(), orderSerde);
var shipments = Channel.of("shipments", Serdes.String(), shipmentSerde);
var inventory = Store.of("inventory", Serdes.String(), Serdes.Long());

var shipper = ProcessDefinition.named("shipper")
    .receives(orders, (delivery, state) -> {
        Long stock = state.get(inventory, delivery.key());
        return Effects.builder()
            .put(inventory, delivery.key(), (stock == null ? 100 : stock) - 1)
            .send(shipments, delivery.key(), Shipment.of(delivery.value()))
            .build();
    })
    .sends(shipments)
    .stores(inventory)
    .build();

try (Parsley parsley = Parsley.start(config, shipper)) {
    parsley.awaitStopped(); // returns when a process stops, or when another thread closes
}
```

A handler receives the delivered message and a read view of state, and returns what it
changes. It is given no producer, no timer and no clock. `Parsley.start` returns once each
process has been started; `awaitStopped` is what keeps the application up, and
`status()` says what each process holds and why, or why it stopped
([Runtime](runtime.md#observing-a-process)).

## Getting it

Parsley is published to
[Maven Central](https://central.sonatype.com/artifact/io.github.tobyjamesclements/parsley).
The current release is 0.2.0.

```xml
<dependency>
  <groupId>io.github.tobyjamesclements</groupId>
  <artifactId>parsley</artifactId>
  <version>0.2.0</version>
</dependency>
```

It requires Java 21 or newer. Its own dependencies are `kafka-streams`, `kafka-clients` and
`slf4j-api`, at the versions it was built against; everything else on the classpath arrives
with Kafka, which brings RocksDB, Jackson and the compression libraries.

0.2.0 is a reimplementation from the specification, and its API shares no type with 0.1.0.
The causal metadata travels in a different header, so the two do not interoperate. Nothing
written against 0.1.0 carries over.

These pages track the `main` branch, which is 0.3.0-SNAPSHOT and can differ from the release
above. Snapshots are published continuously to
[Central's snapshot repository](https://central.sonatype.com/repository/maven-snapshots/),
which is publicly readable and needs no credentials, but carries no compatibility promise.
Pick the release unless you are tracking unreleased work.

## Reading order

| Page | Subject |
|---|---|
| [Model](model.md) | How the specification's terms map onto Kafka, and what the metadata expresses |
| [Delivery](delivery.md) | The settled frontier and the delivery decision |
| [State](state.md) | Ordering state, persistence and recovery |
| [Failing closed](failing-closed.md) | What stops a process, and why the blast radius is the process |
| [Runtime](runtime.md) | Wiring into Kafka Streams |
| [Operations](operations.md) | Names, prerequisites, scaling, resets and sizing |
| [Runbooks](runbooks.md) | What an operator does when a process stops or holds, reason by reason, and how to reset one |
| [Session consistency](session.md) | Carrying the causal frontier past the last consumer, out to clients |
| [Wire format](wire-format.md) | The frozen on-wire definition of causal metadata |
| [Verification](verification.md) | How the guarantee is tested |
| [API](api.md) | Generated Javadoc |

`SPEC.md` in the repository is the authority on correctness. `DECISIONS.md` records every
choice the specification left open, and `EVIDENCE.md` records, per criterion, what would
catch a violation.
