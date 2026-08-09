# Parsley

Causal delivery order for Kafka Streams processors, built to `SPEC.md`. Kafka guarantees a total order per
topic-partition and nothing between partitions; parsley adds the missing cross-channel guarantee: **if message A is a
cause of message B, any process that delivers both delivers A first** — across restarts, for a process's whole
lifetime.

## Terms of art

Every term of art used in this repository names the following (SPEC Structural 6):

* **Happened-before** — Lamport's irreflexive partial order over events: same-process precedence, plus send-to-receipt
  edges, closed transitively (Lamport, *Time, Clocks, and the Ordering of Events in a Distributed System*, CACM 1978).
* **Cause / causal delivery** — as `SPEC.md` defines them: A causes B when a delivery of A happened-before B's send;
  causal delivery means no process that delivers both delivers them inverted. This is the delivery discipline of
  causal broadcast (Birman & Joseph, *Reliable Communication in the Presence of Failures*, TOCS 1987), transplanted
  onto channels.
* **Causal frontier** (this implementation's summary) — a map channel → highest position known causal, playing the
  role a vector clock (Fidge 1988; Mattern 1989) plays in process-indexed causal broadcast, but indexed by channel
  because the spec forbids process identity in metadata (SPEC Structural 11).
* **Hold-back buffer** — the standard causal-broadcast queue of received-but-not-yet-deliverable messages, here
  per-channel and persistent.
* **Exactly-once / EOS** — Kafka's transactional processing (`exactly_once_v2` + `read_committed`): a step's state
  changes, sends and consumed read positions commit atomically or not at all.
* **LSO (last stable offset)** — Kafka's barrier for `read_committed` readers: the first offset of the earliest
  still-open transaction on a partition (the high watermark when none is open); everything below it is settled as
  committed data or aborted/control positions that will never yield a message.
* **Fail closed** — as `SPEC.md` defines it: stop delivering rather than weaken the guarantee.

## What it looks like

```java
var orders    = Channel.of("orders", Serdes.String(), orderSerde);
var shipments = Channel.of("shipments", Serdes.String(), shipmentSerde);
var inventory = StoreDef.of("inventory", Serdes.String(), Serdes.Long());

var shipper = ProcessDefinition.named("shipper")
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

try (Parsley parsley = Parsley.start(
        ParsleyConfig.builder("broker:9092", "my-app").build(),
        shipper)) {
    // runs until closed; a process that fails closed stays down until an operator intervenes
}
```

Each declared process runs as its own Kafka Streams application under `exactly_once_v2`; none of the safety-bearing
configuration can be overridden. The seam passes application logic exactly the delivered message and its application
state, and accepts effects only through the returned value — no timers, no producer, no clock.

## Layout

| Where | What |
|---|---|
| `io.github.tobyjamesclements.parsley.core` | Host-independent protocol: the causal frontier, hold-back buffer, and the pure deliverability decision (`Deliverability.decide`, SPEC Structural 7) |
| `io.github.tobyjamesclements.parsley.api` | The public, statically-typed declaration surface |
| `io.github.tobyjamesclements.parsley.kafka` | The Kafka Streams adapter: byte topologies, position facts from the admin client, EOS lifecycle |
| `docs/model.md` | How the pieces satisfy the spec, and why |
| `docs/wire-format.md` | The frozen wire format of the causal metadata |
| `DECISIONS.md` | Every choice the spec left open, with the alternatives rejected |
| `EVIDENCE.md` | Per criterion: what would catch a violation |

## Tests

```
./mvnw test
```

Three layers: unit tests on the pure core; a **simulation harness** driving real engines under a simulated host that
honours the spec's Host obligations — randomised topologies, interleavings, gaps from aborted transactions, crashes,
restarts, offset rewinds — checked against a happened-before oracle maintained outside the engine; and integration
tests against an embedded KRaft broker (real EOS, real aborted transactions, real truncation). The suite also runs
against deliberately sabotaged engines and asserts it catches each violation class: evidence the tests would fail if
the behaviour broke (see `EVIDENCE.md`).
