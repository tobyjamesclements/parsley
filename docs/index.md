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
    // runs until closed
}
```

A handler receives the delivered message and a read view of state, and returns what it
changes. It is given no producer, no timer and no clock.

## Reading order

| Page | Subject |
|---|---|
| [Model](model.md) | How the specification's terms map onto Kafka, and what the metadata expresses |
| [Delivery](delivery.md) | The settled frontier and the delivery decision |
| [State](state.md) | Ordering state, persistence and recovery |
| [Failing closed](failing-closed.md) | What stops a process, and why the blast radius is the process |
| [Runtime](runtime.md) | Wiring into Kafka Streams |
| [Session consistency](session.md) | Carrying the causal frontier past the last consumer, out to clients |
| [Wire format](wire-format.md) | The frozen on-wire definition of causal metadata |
| [Verification](verification.md) | How the guarantee is tested |
| [API](api.md) | Generated Javadoc |

`SPEC.md` in the repository is the authority on correctness. `DECISIONS.md` records every
choice the specification left open, and `EVIDENCE.md` records, per criterion, what would
catch a violation.
