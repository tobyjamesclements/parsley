# Getting started

## Requirements

- Java 21 or later.
- Kafka brokers new enough to serve topic IDs (3.7+).
- `processing.guarantee=exactly_once_v2` — enforced by `Parsley.streams`, which also pins
  the consumer to `read_committed`. Parsley's state is defined against EOS; there is no
  at-least-once mode.

## The shape: functional core, imperative edges

Your logic is a pure function. It receives a causally delivered `Message` and returns
`Emission` values; with per-key state, it is a fold that also returns the next state. It
holds no reference to any runtime, imports nothing from Kafka, and is unit-testable with
plain equality. Everything imperative — gating, decoding, state persistence, stamping,
partitioning, the transaction — lives in the stage runtime that hosts it.

## Topics and codecs

Topics are typed values declared once — codecs live with the declaration, and a pipeline hop
is the same `Topic` appearing as one stage's sink and another's source, which makes codec
agreement across the hop hold by construction. A `Codec` is two pure functions between a
value and its bytes; Kafka serdes (Avro, JSON Schema, and friends) bridge in with
`Codec.fromSerde(serde, topicName)`. The codec contract, hand-rolled codecs, and the
schema-registry formats are covered in [codecs and Avro](codecs.md).

```java
Topic<String, Order>   orders      = Topic.of("orders", Codec.utf8(), orderCodec);
Topic<String, Payment> payments    = Topic.of("payments", Codec.utf8(), paymentCodec);
Topic<String, Settled> settlements = Topic.of("settlements", Codec.utf8(), settledCodec);
```

## A stateless stage

A `Handler` maps one delivered message to its emissions. Emissions are created by the sink
topic itself — `topic.send(key, value)` — so the payload type-checks at the construction
site, and an emission to an undeclared sink fails loudly at the stamping site.

```java
Stage settlement = Stage.named("settlement")
        .on(orders,   m -> List.of(settlements.send(m.key(), settle(m.value()))))
        .on(payments, m -> List.of(settlements.send(m.key(), apply(m.value()))))
        .into(settlements)
        .build();

Properties props = new Properties();
props.put(StreamsConfig.APPLICATION_ID_CONFIG, "settlements-app");
props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9092");

try (CausalStreams app = Parsley.of(settlement).streams(props)) {
    app.start();
    // run until shutdown
}
```

Every cause the stage consumes has already been delivered when a handler runs; that is the
guarantee. A message carries its own coordinate — source topic, partition, offset,
timestamp — and for a message released from the hold queue, the coordinate is its own, not
the release trigger's. By default an emission inherits the handled message's timestamp;
`send(key, value, timestamp)` overrides it.

## A stateful stage

Declaring `state` gives the stage per-key state, and sources fold over it: the runtime
resolves the state for the message's key (the declared initial value for an unseen key),
applies the pure `Fold`, persists the returned state, and applies the returned emissions —
all in the same transaction as the delivery. Causal order plus a pure fold makes the state a
deterministic function of the delivered history.

```java
Stage balances = Stage.named("balances")
        .state(balanceCodec, Balance::zero)
        .on(orders,   (bal, m) -> Step.of(bal.minus(m.value().total()),
                                          settlements.send(m.key(), settled(bal, m))))
        .on(payments, (bal, m) -> Step.of(bal.plus(m.value().amount())))
        .into(settlements)
        .build();
```

State is keyed by the message key's encoded bytes — the same agreement co-partitioning
already requires — and scoped to the stage. Returning `Step.of(null, ...)` deletes the key's
state. The stage name keys its stores, so keep it stable across deployments.

## Testing your logic

Handlers and folds are pure functions over values with equality, so the primary tests need
no runtime at all:

```java
assertEquals(List.of(settlements.send("k", expected)),
        handler.handle(Message.of("orders", "k", order)));

assertEquals(Step.of(expectedBalance, settlements.send("k", expected)),
        fold.apply(balance, Message.of("payments", "k", payment)));
```

For the wiring, `Parsley.of(stage).testTopology()` returns a fresh broker-less topology for
`TopologyTestDriver`: pipe records and assert on outputs. Stamp presence is observable via
the `CausalHeaders` name constants; gating behaviour itself is Parsley's contract, verified
by Parsley's own suite. The test topology is for the driver only — under a real cluster it
neither captures consumer positions nor resolves real topic identity, and the adapter fails
closed at the first delivered record.

## What the runtime wires for you

`Parsley.streams` installs a client supplier that records consumer positions after each poll
(the liveness signal — see [liveness](../foundations/liveness.md#position-advance-bridging)),
and resolves topic identity and sink end offsets through an admin client built from the same
properties. A declared sink must exist before the application starts: init fails loudly
rather than run with own-output claims silently off. The returned `CausalStreams` is a
curated allowlist over the Kafka Streams runtime — lifecycle, state, listeners, metrics,
lag — with no accessor to the underlying instance; members that could violate causality
(pausing an instance freezes the release of held records fleet-wide, handing out the
protocol stores) are absent by design.

## Where next

- [Topology shapes](topologies.md) — the common wiring shapes (linear, fan-out, fan-in,
  diamond, request and reply, event flows, cycles) and exactly what ordering each one gets.
- [Codecs and Avro](codecs.md) — the codec contract, writing your own, bridging serdes, and
  schema-registry formats.
- [The contract](expectations.md) — everything Parsley expects of you and everything it
  promises back, including the operational notes.
- [Plain clients](clients.md) — stamping from plain producers, observing from plain
  consumers.
