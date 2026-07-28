# Getting started

## Requirements

- Java 21 or later.
- Kafka brokers new enough to serve topic IDs (3.7+).
- `processing.guarantee=exactly_once_v2` — enforced by `CausalStreams.start`, which also pins
  the consumer to `read_committed`. Parsley's state is defined against EOS; there is no
  at-least-once mode.

## A causal stage

Topics are typed values declared once — serdes live with the declaration, and a pipeline hop
is the same `CausalTopic` appearing as one stage's sink and another's source, which makes
serde agreement across the hop hold by construction. A stage pairs each source with an
ordinary Kafka Streams `Processor`, supplied per source so every source is typed
independently:

```java
CausalTopic<String, Order>   orders      = CausalTopic.of("orders", Serdes.String(), orderSerde);
CausalTopic<String, Payment> payments    = CausalTopic.of("payments", Serdes.String(), paymentSerde);
CausalTopic<String, Settled> settlements = CausalTopic.of("settlements", Serdes.String(), settledSerde);

CausalStage settlement = CausalStage.builder("settlement")
        .source(orders, SettlementProcessor::new)
        .source(payments, PaymentProcessor::new)
        .sink(settlements)
        .build();

Properties props = new Properties();
props.put(StreamsConfig.APPLICATION_ID_CONFIG, "settlements-app");
props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9092");

try (CausalStreams app = CausalStreams.start(settlement, props)) {
    // run until shutdown
}
```

Processors are full Streams citizens — one instance per task, `init`/`close` lifecycle,
connected stores via `ProcessorSupplier.stores()`, punctuators, metrics, task metadata and
stream time on the context, exactly as in any Streams application:

```java
final class SettlementProcessor implements Processor<String, Order, String, Settled> {

    private ProcessorContext<String, Settled> context;

    @Override
    public void init(ProcessorContext<String, Settled> context) {
        this.context = context;   // schedule punctuators here, fetch your stores, as usual
    }

    @Override
    public void process(Record<String, Order> record) {
        // Every cause this stage consumes has already been delivered here.
        context.forward(record.withValue(settle(record.value())), "settlements");
    }
}
```

Three context members behave causally rather than raw: `forward` goes through the stamping
door — serialized with the sink topic's serdes, deterministically partitioned before
stamping, clock attached; a named forward addresses a declared sink by topic name, an
unnamed forward goes to every declared sink, and an undeclared sink fails loudly.
`getStateStore` refuses the stage's protocol store. `recordMetadata` reports the delivered
record's own coordinate — for a record released from the hold queue, that is its coordinate,
not the release trigger's. Serialization happens inside the stage — processors never see
bytes, and the dependency-clock header travels with the exact bytes it claims. The stage
name keys its state store, so keep it stable across deployments.

## What the runtime wires for you

`CausalStreams.start` installs a client supplier that records consumer positions after each
poll (the liveness signal — see
[liveness](../foundations/liveness.md#position-advance-bridging)), and resolves topic identity
and sink end offsets through an admin client built from the same properties. A declared sink
must exist before the application starts: init fails loudly rather than run with own-output
claims silently off.

## Operational notes

- **Hold queues are unbounded and disk-backed.** Held records live in the RocksDB state
  store (and its changelog), not on the heap, so a lagging cause channel grows state rather
  than exhausting memory. Watch hold depth as the operational signal of a lagging cause;
  retention economics are the real bound.
- **A blocked head blocks its channel** (head-of-line blocking, by design). If a producer
  stamps claims that are far ahead of what its consumers can fetch, convoying on that channel
  is the expected symptom.
- **Co-partition your input topics by key.** A task consumes partition *p* of every source
  topic, so causal order across topics holds within a partition slice — related keys must
  route to the same slice, exactly as Kafka Streams itself requires for multi-topic
  processing ([the causal model](../foundations/causal-model.md#preconditions)).
- **Fail closed**: a corrupt clock header or an unresolvable sink fails the task; a failed
  send aborts with its transaction, taking every claim on it along. The retry refetches.
- **Testing your stage**: build the stage normally and drive `stage.testTopology()` with
  `TopologyTestDriver` — broker-less wiring is built in, so pipe records and assert on your
  processor's outputs. Stamp presence is observable via the `CausalHeaders` name constants;
  gating behaviour itself is Parsley's contract, verified by Parsley's own suite.
