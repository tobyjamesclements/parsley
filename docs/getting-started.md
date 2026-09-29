# Getting started

From an empty project to one running process, and a unit test for its handler that needs
no broker. The pages after this one say how the guarantee works; this one says how to use
it.

## Before you start

- Java 21 or newer.
- A Kafka cluster at 3.7.0 or newer. Parsley identifies a topic by the id the broker
  assigns it, which older brokers do not provide, and refuses to start against one.
- Every topic a process receives from or sends to, created before the process starts.
  Nothing is auto-created, and a missing topic refuses the start. Parsley creates only its
  own changelogs.
- The dependency:

```xml
<dependency>
  <groupId>io.github.tobyjamesclements</groupId>
  <artifactId>parsley</artifactId>
  <version>0.3.0</version>
</dependency>
```

`kafka-streams`, `kafka-clients` and `slf4j-api` arrive with it. Bring an SLF4J backend of
your own; the library logs through the API and ships none.

## Declare the topics and stores

A `Topic` binds a Kafka topic name to the serdes for its keys and values. A `Store` does
the same for a named key-value store the process owns. The serdes describe only your data:
causal metadata travels in a reserved record header and never touches keys or values.

```java
import io.github.tobyjamesclements.parsley.Effects;
import io.github.tobyjamesclements.parsley.Parsley;
import io.github.tobyjamesclements.parsley.ParsleyConfig;
import io.github.tobyjamesclements.parsley.Process;
import io.github.tobyjamesclements.parsley.Store;
import io.github.tobyjamesclements.parsley.Topic;
import org.apache.kafka.common.serialization.Serdes;

Topic<String, Order>    orders    = Topic.of("orders", Serdes.String(), orderSerde);
Topic<String, Shipment> shipments = Topic.of("shipments", Serdes.String(), shipmentSerde);
Store<String, Long>     inventory = Store.of("inventory", Serdes.String(), Serdes.Long());
```

A topic starts at its earliest retained message. To skip what is already on it the first
time the process ever runs, declare that:

```java
Topic<String, Order> orders = Topic.of("orders", Serdes.String(), orderSerde)
        .startingAt(Topic.InitialPosition.LATEST);
```

The starting position applies only to a process's first start ever. After that the process
resumes where it committed, whatever was declared.

Import `Process` by name rather than through a wildcard. `java.lang.Process` is always in
scope, and a wildcard import leaves the simple name ambiguous; a single-type import wins.

Declare each topic and store once and pass the same instance everywhere. The runtime
matches a send or a state access to the declaration by instance, and a look-alike built
with other serdes is refused at the point of use.

## Declare a process

A process names the topics it receives, with a handler for each, the topics it may send
on, and the stores it owns.

```java
Process shipper = Process.named("shipper")
        .receives(orders, (delivery, state) -> {
            Long stock = state.get(inventory, delivery.key());
            long remaining = (stock == null ? 100 : stock) - 1;
            return Effects.builder()
                    .put(inventory, delivery.key(), remaining)
                    .send(shipments, delivery.key(), Shipment.of(delivery.value()))
                    .build();
        })
        .sends(shipments)
        .stores(inventory)
        .build();
```

The handler is given the delivered message and a read view of the declared stores, and
returns everything it wants to change. It has no producer, no timer and no clock. The
state writes, the sends and the read position commit in one transaction, so a handler is
never asked to be idempotent by hand.

Two rules a handler must keep:

- **Return the same effects for the same message.** After a failure the runtime feeds the
  same message again and expects identical effects. Derive anything time-like from the
  delivered data, not from a clock.
- **Do not throw past a failure you can record.** A handler that throws stops the process,
  and a restart feeds the same message and throws again, because a message is never
  skipped. To move on from a bad message, catch the failure and return effects that record
  it, such as a send to a declared dead-letter topic. `Effects.none()` is the return for a
  step that changes nothing.

A process may receive several topics, each with its own handler. Every declared process
becomes its own Kafka Streams application, so two processes may receive the same topic.

## Configure

```java
ParsleyConfig config = ParsleyConfig.builder("kafka-1:9092,kafka-2:9092", "shop")
        .build();
```

The two arguments are the bootstrap servers and an application-id prefix. Each process's
application id, consumer group and changelog names are derived from the prefix and the
process name, `shop-shipper` here, and that id is what carries the process's committed
state across restarts. Keep it stable for the life of the process; reusing a prefix for a
different pipeline inherits the old one's state and refuses.

Three optional settings:

| Setting | Default | What it does |
|---|---|---|
| `stateDir(path)` | the Kafka Streams default under the temp directory | Where local state lives. Give it a persistent path in production, or every restart restores from the changelogs. |
| `metadataBudgetBytes(n)` | 256 KiB | The largest causal metadata a message may carry. Sized in [Operations](operations.md#sizing). |
| `streamsProperty(key, value)` | none | Any Kafka Streams property that does not carry the guarantee. The ones that do, such as `processing.guarantee`, `isolation.level` and `auto.offset.reset`, are fixed by the runtime and refused here. |

## Start, wait, and read the status

```java
try (Parsley parsley = Parsley.start(config, shipper)) {
    parsley.awaitStopped();
    parsley.status().forEach((name, status) ->
            log.error("{}: {} refusal={} detail={}", name,
                    status.lifecycle(), status.refusalReason(), status.failureDetail()));
}
```

`Parsley.start` resolves every declared topic, establishes each process's starting
positions, starts each process, and returns. It returns once each process has been
started, not once it is running; the wait is what keeps the application up. A refusal
the start can see is thrown from the call as a `FailClosedException`. A missing topic or an
unreachable cluster is an `IllegalStateException`, and a bad declaration is an
`IllegalArgumentException`.

`awaitStopped` returns when any process stops, or when another thread closes the handle.
`status()` then says which process stopped and why. A present `refusalReason` means the
process stopped to preserve the guarantee and will stop the same way on restart; what to
do about each reason is in [Runbooks](runbooks.md). An absent one means an application
failure or a cluster transient. `healthy()` is the same information as one boolean, for a
health endpoint.

A message that is waiting for a cause is not a failure and does not appear in the status.
The process is running, and the message delivers when the cause arrives. If one waits
longer than it should, the diagnosis is which cause and why, in
[a runbook](runbooks.md#a-message-is-held-and-not-moving).

## What appears on the cluster

For the prefix `shop` and the process `shipper`, Kafka Streams creates, on first start:

| What | Name |
|---|---|
| Consumer group and application id | `shop-shipper` |
| Ordering-state changelog | `shop-shipper-__parsley.ordering-changelog` |
| Changelog of the store `inventory` | `shop-shipper-inventory-changelog` |

The ordering changelog holds the process's causal state and its held messages. The
prefix `__parsley.` is reserved: no application topic, store, process or prefix may
contain it, and no application header may begin with `parsley.`. Names, ACLs, scaling and
sizing are in [Operations](operations.md).

## Test a handler without a broker

A handler is a function of a `Delivery` and a `State`, so a unit test builds both by hand.
`Delivery.of` exists for this, and `State` is an interface with one method.

```java
Topic<String, String> orders    = Topic.of("orders", Serdes.String(), Serdes.String());
Topic<String, String> shipments = Topic.of("shipments", Serdes.String(), Serdes.String());
Store<String, String> inventory = Store.of("inventory", Serdes.String(), Serdes.String());

Handler<String, String> shipper = (delivery, state) -> {
    String remaining = state.get(inventory, delivery.key());
    return Effects.builder()
            .put(inventory, delivery.key(), remaining == null ? "0" : remaining)
            .send(shipments, delivery.key(), "ship " + delivery.value())
            .build();
};

Delivery<String, String> delivery = Delivery.of(
        orders, new Channel(UUID.randomUUID(), 0), 12L, 1_000L, "sku-1", "2 units", List.of());

State state = new State() {
    @Override
    @SuppressWarnings("unchecked")
    public <K, V> V get(Store<K, V> store, K key) {
        return (V) ("sku-1".equals(key) ? "7" : null);
    }
};

Effects effects = shipper.handle(delivery, state);

assertEquals("7", effects.writes().get(0).value());
assertEquals("ship 2 units", effects.sends().get(0).value());
```

`Effects` exposes its `writes()` and `sends()` as records, so a test asserts on exactly
what the handler declared. This is how the library's own `HandlerSeamTest` drives the seam.

## Where next

- [Model](model.md) for what the guarantee means and what the metadata carries.
- [Runtime](runtime.md) for what happens at start, at each task's initialisation, and in
  the seam.
- [Failing closed](failing-closed.md) for every reason a process stops, and
  [Runbooks](runbooks.md) for what to do about each.
- The [Javadoc](https://tobyjamesclements.github.io/parsley/) of `main`.
