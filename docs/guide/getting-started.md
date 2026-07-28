# Getting started

## Requirements

- Java 21 or later.
- Kafka brokers new enough to serve topic IDs (3.7+).
- `processing.guarantee=exactly_once_v2` — enforced by `CausalStreams.start`, which also pins
  the consumer to `read_committed`. Parsley's state is defined against EOS; there is no
  at-least-once mode.

## A causal stage

A stage is declared with sources, a processor, and sinks, then started through
`CausalStreams`:

```java
CausalStage<String, String, String, String> stage =
        CausalStage.<String, String, String, String>builder()
                .source("orders", Serdes.String(), Serdes.String())
                .source("payments", Serdes.String(), Serdes.String())
                .processor(SettlementProcessor::new)
                .sink("settlements", Serdes.String(), Serdes.String())
                .build();

Properties props = new Properties();
props.put(StreamsConfig.APPLICATION_ID_CONFIG, "settlements-app");
props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9092");

try (CausalStreams app = CausalStreams.start(stage, props)) {
    // run until shutdown
}
```

The processor is an ordinary Streams `Processor`; records arrive in causal delivery order,
and forwards are stamped automatically:

```java
final class SettlementProcessor implements Processor<String, String, String, String> {

    private ProcessorContext<String, String> context;

    @Override
    public void init(ProcessorContext<String, String> context) {
        this.context = context;
    }

    @Override
    public void process(Record<String, String> record) {
        // Every consumed cause of this record has already been delivered here.
        context.forward(record.withValue(settle(record.value())), "settlements");
    }
}
```

`forward(record, sinkTopic)` routes to one sink by topic name; `forward(record)` fans out to
every declared sink, matching Streams' unnamed-forward semantics. Serialization happens inside
the adapter — user code never sees bytes, and the dependency-clock header travels with the
exact bytes it claims.

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
- **Fail closed**: a corrupt clock header or an unresolvable sink fails the task; a failed
  send aborts with its transaction, taking every claim on it along. The retry refetches.
- **Testing your stage**: build the stage normally and drive `stage.testTopology()` with
  `TopologyTestDriver` — broker-less wiring is built in, and `CausalStage.testChannel` gives
  the channel identities for crafting or asserting on clock headers. See `CausalStageTest`
  in the repository for the pattern.
