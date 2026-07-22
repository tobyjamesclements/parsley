package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Compiles and exercises the {@code java} samples in {@code docs/getting-started.md} and
 * {@code docs/streams.md}, so an API change that would break a published sample fails
 * {@code mvn test} instead of shipping silently.
 *
 * <p>Each sample appears twice here:
 * <ul>
 *   <li>A <em>mirror method</em> whose body is the fenced block verbatim, with the doc's free
 *       identifiers ({@code props}, {@code producer}, {@code trigger}, ...) as parameters.
 *       Compilation is the contract; mirrors whose sample needs a live broker (the
 *       {@code using(Properties)} / {@code builder(Properties)} resolvers, {@code start()}) are
 *       never invoked.</li>
 *   <li>A {@code @Test} that executes the same chain over the broker-free resolver the docs
 *       themselves bless for tests (the {@code using(Map)} / {@code builder(Map)} overloads,
 *       getting-started.md § "Stamping causal context onto produced records"; the package-private
 *       {@code topicAdmin} seam for the Streams sample) and asserts the behaviour the surrounding
 *       prose claims.</li>
 * </ul>
 *
 * <p>Drift is one-directional — a doc edit does not touch this file — so each fenced block in the
 * docs carries a comment naming its mirror method. The samples use the repo's usual T1/T2/T3
 * topic-name convention, so the mirrors do too.
 */
class DocsSamplesTest {

    /** The docs' topic names bound to fixed UUIDs — the broker-free resolver path. */
    private static final Map<String, Uuid> TOPIC_IDS = Map.of(
            "t1", Uuid.randomUuid(),
            "t2", Uuid.randomUuid(),
            "t3", Uuid.randomUuid());

    // ---------------------------------------------------------------------------------------------
    // docs/getting-started.md § "Stamping causal context onto produced records", sample 1: relay
    // ---------------------------------------------------------------------------------------------

    /**
     * Verbatim mirror of the one-to-one relay sample. Never invoked: {@code using(Properties)}
     * resolves topic UUIDs through a live broker. Executed equivalent:
     * {@link #relaySampleStampsTheTriggersDepsAndOwnPosition()}.
     */
    @SuppressWarnings("unused")
    private static void relaySample(Properties props, Producer<String, String> producer,
            ConsumerRecord<String, String> trigger, String key, String value) {
        // the trigger's own dependencies plus its own position
        CausalClock deps = CausalClock.using(props).observe(trigger);
        producer.send(deps.stamp(new ProducerRecord<>("t3", key, value)));
    }

    /**
     * The relay sample's chain, over the broker-free {@code using(Map)} overload: observing the
     * trigger folds in both the dependencies the trigger arrived with and the trigger's own
     * position, exactly as the surrounding prose claims.
     *
     * Asserts that the stamped record's clock header decodes to the trigger's carried dependency
     * plus the trigger's own coordinate.
     */
    @Test
    void relaySampleStampsTheTriggersDepsAndOwnPosition() {
        CausalClock carried = CausalClock.builder(TOPIC_IDS).require("t2", 0, 3).build();
        ConsumerRecord<String, String> trigger = new ConsumerRecord<>("t1", 0, 7L, "k", "v");
        trigger.headers().add(ParsleyHeader.CAUSAL_CLOCK, carried.toBytes());

        CausalClock deps = CausalClock.using(TOPIC_IDS).observe(trigger);
        ProducerRecord<String, String> stamped =
                deps.stamp(new ProducerRecord<>("t3", "k", "v"));

        CausalClock expected = CausalClock.builder(TOPIC_IDS)
                .require("t2", 0, 3)
                .require("t1", 0, 7)
                .build();
        assertEquals(Optional.of(expected), CausalClock.fromHeaders(stamped.headers()),
                "the relay sample must stamp the trigger's carried dependencies plus the "
                        + "trigger's own (topic, partition, offset)");
    }

    // ---------------------------------------------------------------------------------------------
    // docs/getting-started.md § "Stamping causal context onto produced records", sample 2: fan-in
    // ---------------------------------------------------------------------------------------------

    /**
     * Verbatim mirror of the fan-in sample. Never invoked: {@code using(Properties)} resolves
     * topic UUIDs through a live broker. Executed equivalent:
     * {@link #fanInSampleChainsAnObservePerInput()}.
     */
    @SuppressWarnings("unused")
    private static void fanInSample(Properties props, Producer<String, String> producer,
            ConsumerRecord<String, String> priceUpdate,
            ConsumerRecord<String, String> inventoryChange, ProducerRecord<String, String> record) {
        CausalClock deps = CausalClock.using(props)
                .observe(priceUpdate)
                .observe(inventoryChange);
        producer.send(deps.stamp(record));
    }

    /**
     * The fan-in sample's chain, over the broker-free {@code using(Map)} overload: one
     * {@code observe} per consumed input accumulates both inputs' positions into a single clock.
     *
     * Asserts that the stamped record's clock header decodes to the union of the two inputs'
     * own coordinates.
     */
    @Test
    void fanInSampleChainsAnObservePerInput() {
        ConsumerRecord<String, String> priceUpdate = new ConsumerRecord<>("t1", 0, 7L, "k", "v");
        ConsumerRecord<String, String> inventoryChange =
                new ConsumerRecord<>("t2", 0, 3L, "k", "v");

        CausalClock deps = CausalClock.using(TOPIC_IDS)
                .observe(priceUpdate)
                .observe(inventoryChange);
        ProducerRecord<String, String> stamped =
                deps.stamp(new ProducerRecord<>("t3", "k", "v"));

        CausalClock expected = CausalClock.builder(TOPIC_IDS)
                .require("t1", 0, 7)
                .require("t2", 0, 3)
                .build();
        assertEquals(Optional.of(expected), CausalClock.fromHeaders(stamped.headers()),
                "the fan-in sample must stamp the union of both consumed inputs' positions");
    }

    // ---------------------------------------------------------------------------------------------
    // docs/getting-started.md § "Stamping causal context onto produced records", sample 3: builder
    // ---------------------------------------------------------------------------------------------

    /**
     * Verbatim mirror of the explicit-requirement builder sample. Never invoked:
     * {@code builder(Properties)} resolves topic UUIDs through a live broker. Executed
     * equivalent: {@link #builderSampleRequiresTheNamedCoordinate()}.
     */
    @SuppressWarnings("unused")
    private static void builderSample(Properties props) {
        CausalClock deps = CausalClock.builder(props)
                .require("t1", /* partition */ 0, /* offset */ 42)
                .build();
    }

    /**
     * The builder sample's chain, over the broker-free {@code builder(Map)} overload: an explicit
     * {@code require} declares a dependency on a position that was never consumed.
     *
     * Asserts that the built clock equals the clock a consumer accumulates by actually observing
     * a record at that same coordinate — the two construction paths must agree.
     */
    @Test
    void builderSampleRequiresTheNamedCoordinate() {
        CausalClock deps = CausalClock.builder(TOPIC_IDS)
                .require("t1", /* partition */ 0, /* offset */ 42)
                .build();

        CausalClock observed = CausalClock.using(TOPIC_IDS)
                .observe(new ConsumerRecord<>("t1", 0, 42L, "k", "v"));
        assertEquals(observed, deps,
                "an explicit require(\"t1\", 0, 42) must build the same clock as observing "
                        + "a record at that coordinate");
    }

    // ---------------------------------------------------------------------------------------------
    // docs/getting-started.md § "Propagating causal context across services", sample 4: token
    // ---------------------------------------------------------------------------------------------

    /**
     * Verbatim mirror of the portable-token sample — broker-free, so this mirror is executed
     * directly by the tests below. The test supplies the transport hop: {@code receivedToken}
     * is {@code token} arriving unchanged.
     */
    private static CausalClock portableTokenSample(ConsumerRecord<String, String> consumedRecord) {
        // Sender. Extract the relevant dependencies and serialise them.
        CausalClock context = CausalClock.fromRecord(consumedRecord)
                .orElse(CausalClock.empty());
        byte[] token = context.toBytes();
        // Send the token over HTTP, gRPC, or another transport, applying your own encryption.
        byte[] receivedToken = token;

        // Receiver. Rebuild the dependencies.
        CausalClock required = CausalClock.fromBytes(receivedToken);
        return required;
    }

    /**
     * The portable-token sample end to end: a stamped record's dependencies survive extraction,
     * serialisation, the transport hop, and reconstruction on the receiver.
     *
     * Asserts that the receiver's rebuilt clock equals the clock the consumed record carried.
     */
    @Test
    void portableTokenSampleRebuildsTheDependenciesAcrossATransport() {
        CausalClock carried = CausalClock.builder(TOPIC_IDS)
                .require("t1", 0, 42)
                .require("t2", 0, 3)
                .build();
        ConsumerRecord<String, String> consumedRecord =
                new ConsumerRecord<>("t3", 0, 9L, "k", "v");
        consumedRecord.headers().add(ParsleyHeader.CAUSAL_CLOCK, carried.toBytes());

        assertEquals(carried, portableTokenSample(consumedRecord),
                "the receiver must rebuild exactly the dependencies the consumed record carried");
    }

    /**
     * The portable-token sample's fallback leg: a record with no clock header degrades to the
     * empty clock via {@code orElse(CausalClock.empty())} rather than failing.
     *
     * Asserts that the receiver's rebuilt clock is the empty clock.
     */
    @Test
    void portableTokenSampleFallsBackToTheEmptyClockForAnUnstampedRecord() {
        ConsumerRecord<String, String> consumedRecord =
                new ConsumerRecord<>("t3", 0, 9L, "k", "v");
        assertEquals(CausalClock.empty(), portableTokenSample(consumedRecord),
                "an unstamped record must propagate as the empty clock, not fail");
    }

    // ---------------------------------------------------------------------------------------------
    // docs/streams.md, intro sample: declare a topology and hand it to the runtime
    // ---------------------------------------------------------------------------------------------

    /**
     * Verbatim mirror of the Streams quick-start sample. Never invoked: the default topic
     * resolver and {@code start()} need a live broker. Executed equivalent (which stops short of
     * {@code start()}): {@link #streamsSampleDeclaresATopologyTheRuntimeAccepts()}.
     */
    @SuppressWarnings("unused")
    private static void streamsQuickstartSample(Properties props, Serde<String> orderSerde,
            Serde<String> enrichedSerde) {
        CausalTopology topology = new CausalStreamsBuilder()
                .stream(List.of("t1", "t2"), Serdes.String(), orderSerde)
                .process(new EnrichOrderSupplier())
                .to("t3", Serdes.String(), enrichedSerde)
                .build();

        CausalStreams causalStreams = new CausalStreams(topology, props);
        causalStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(causalStreams::close));
    }

    /**
     * The Streams sample's declaration chain, with the {@link TestTopicAdmin} double injected
     * through the package-private {@code topicAdmin} seam so no broker is needed: the fluent
     * stream/process/to/build chain produces a topology the {@code CausalStreams} runtime
     * accepts as constructed.
     *
     * Asserts that the constructed (unstarted) instance sits in the {@code CREATED} state.
     */
    @Test
    void streamsSampleDeclaresATopologyTheRuntimeAccepts() {
        Serde<String> orderSerde = Serdes.String();
        Serde<String> enrichedSerde = Serdes.String();

        CausalTopology topology = new CausalStreamsBuilder()
                .topicAdmin(TestTopicAdmin.of(TOPIC_IDS))
                .stream(List.of("t1", "t2"), Serdes.String(), orderSerde)
                .process(new EnrichOrderSupplier())
                .to("t3", Serdes.String(), enrichedSerde)
                .build();

        try (CausalStreams causalStreams = new CausalStreams(topology, streamsProps())) {
            assertEquals(KafkaStreams.State.CREATED, causalStreams.state(),
                    "the docs' declaration chain must yield a runtime instance the wrapped "
                            + "KafkaStreams accepted (CREATED, ready to start)");
        }
    }

    /** The minimal Streams configuration the docs' runtime sample presumes. */
    private static Properties streamsProps() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "docs-streams-sample");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.STATE_DIR_CONFIG, System.getProperty("java.io.tmpdir"));
        return props;
    }

    /** The doc sample's processor supplier: a minimal enrichment stand-in, forwarding unchanged. */
    private static final class EnrichOrderSupplier
            implements ProcessorSupplier<String, String, String, String> {
        @Override
        public Processor<String, String, String, String> get() {
            return new Processor<>() {
                private ProcessorContext<String, String> context;

                @Override
                public void init(ProcessorContext<String, String> context) {
                    this.context = context;
                }

                @Override
                public void process(Record<String, String> record) {
                    context.forward(record);
                }
            };
        }
    }
}
