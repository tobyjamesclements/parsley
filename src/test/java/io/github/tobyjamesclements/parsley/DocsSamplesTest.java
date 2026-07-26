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
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Compiles and exercises the {@code java} samples in {@code docs/guide/getting-started.md} and
 * {@code docs/guide/streams.md}, so an API change that would break a published sample fails
 * {@code mvn test} instead of shipping silently.
 *
 * <p>Each sample appears twice here:
 * <ul>
 *   <li>A <em>mirror method</em> whose body is the fenced block verbatim, with the doc's free
 *       identifiers ({@code props}, {@code producer}, {@code m1}, ...) as parameters.
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
 * docs carries a comment naming its mirror method. The samples use the repo's academic naming
 * convention — channels {@code c1}/{@code c2}/{@code c3}, messages {@code m1}/{@code m2}/{@code m3}
 * — so the mirrors do too.
 */
class DocsSamplesTest {

    /** The docs' topic names bound to fixed UUIDs — the broker-free resolver path. */
    private static final Map<String, Uuid> TOPIC_IDS = Map.of(
            "c1", Uuid.randomUuid(),
            "c2", Uuid.randomUuid(),
            "c3", Uuid.randomUuid());

    // ---------------------------------------------------------------------------------------------
    // docs/guide/getting-started.md § "Stamping causal context onto produced records", sample 1: relay
    // ---------------------------------------------------------------------------------------------

    /**
     * Verbatim mirror of the one-to-one relay sample. Never invoked: {@code using(Properties)}
     * resolves topic UUIDs through a live broker. Executed equivalent:
     * {@link #relaySampleStampsTheConsumedMessagesDepsAndOwnPosition()}.
     */
    @SuppressWarnings("unused")
    private static void relaySample(Properties props, Producer<String, String> producer,
            ConsumerRecord<String, String> m1, String key, String value) {
        // m1's own dependencies plus its own position
        CausalClock deps = CausalClock.using(props).observe(m1);
        producer.send(deps.stamp(new ProducerRecord<>("c3", key, value)));
    }

    /**
     * The relay sample's chain, over the broker-free {@code using(Map)} overload: observing the
     * consumed message {@code m1} folds in both the dependencies {@code m1} arrived with and
     * {@code m1}'s own position, exactly as the surrounding prose claims.
     *
     * Asserts that the stamped record's clock header decodes to {@code m1}'s carried dependency
     * plus {@code m1}'s own coordinate.
     */
    @Test
    void relaySampleStampsTheConsumedMessagesDepsAndOwnPosition() {
        CausalClock carried = CausalClock.builder(TOPIC_IDS).require("c2", 0, 3).build();
        ConsumerRecord<String, String> m1 = new ConsumerRecord<>("c1", 0, 7L, "k", "v");
        m1.headers().add(ParsleyHeader.CAUSAL_CLOCK, carried.toBytes());

        CausalClock deps = CausalClock.using(TOPIC_IDS).observe(m1);
        ProducerRecord<String, String> stamped =
                deps.stamp(new ProducerRecord<>("c3", "k", "v"));

        CausalClock expected = CausalClock.builder(TOPIC_IDS)
                .require("c2", 0, 3)
                .require("c1", 0, 7)
                .build();
        assertEquals(Optional.of(expected), CausalClock.fromHeaders(stamped.headers()),
                "the relay sample must stamp m1's carried dependencies plus m1's own "
                        + "(topic, partition, offset)");
    }

    // ---------------------------------------------------------------------------------------------
    // docs/guide/getting-started.md § "Stamping causal context onto produced records", sample 2: fan-in
    // ---------------------------------------------------------------------------------------------

    /**
     * Verbatim mirror of the fan-in sample. Never invoked: {@code using(Properties)} resolves
     * topic UUIDs through a live broker. Executed equivalent:
     * {@link #fanInSampleChainsAnObservePerInput()}.
     */
    @SuppressWarnings("unused")
    private static void fanInSample(Properties props, Producer<String, String> producer,
            ConsumerRecord<String, String> m1, ConsumerRecord<String, String> m2,
            ProducerRecord<String, String> m3) {
        CausalClock deps = CausalClock.using(props)
                .observe(m1)
                .observe(m2);
        producer.send(deps.stamp(m3));
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
        ConsumerRecord<String, String> m1 = new ConsumerRecord<>("c1", 0, 7L, "k", "v");
        ConsumerRecord<String, String> m2 = new ConsumerRecord<>("c2", 0, 3L, "k", "v");
        ProducerRecord<String, String> m3 = new ProducerRecord<>("c3", "k", "v");

        CausalClock deps = CausalClock.using(TOPIC_IDS)
                .observe(m1)
                .observe(m2);
        ProducerRecord<String, String> stamped = deps.stamp(m3);

        CausalClock expected = CausalClock.builder(TOPIC_IDS)
                .require("c1", 0, 7)
                .require("c2", 0, 3)
                .build();
        assertEquals(Optional.of(expected), CausalClock.fromHeaders(stamped.headers()),
                "the fan-in sample must stamp the union of both consumed inputs' positions");
    }

    // ---------------------------------------------------------------------------------------------
    // docs/guide/getting-started.md § "Stamping causal context onto produced records", sample 3: builder
    // ---------------------------------------------------------------------------------------------

    /**
     * Verbatim mirror of the explicit-requirement builder sample. Never invoked:
     * {@code builder(Properties)} resolves topic UUIDs through a live broker. Executed
     * equivalent: {@link #builderSampleRequiresTheNamedCoordinate()}.
     */
    @SuppressWarnings("unused")
    private static void builderSample(Properties props) {
        CausalClock deps = CausalClock.builder(props)
                .require("c1", /* partition */ 0, /* offset */ 42)
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
                .require("c1", /* partition */ 0, /* offset */ 42)
                .build();

        CausalClock observed = CausalClock.using(TOPIC_IDS)
                .observe(new ConsumerRecord<>("c1", 0, 42L, "k", "v"));
        assertEquals(observed, deps,
                "an explicit require(\"c1\", 0, 42) must build the same clock as observing "
                        + "a record at that coordinate");
    }

    // ---------------------------------------------------------------------------------------------
    // docs/guide/getting-started.md § "Propagating causal context across services", sample 4: token
    // ---------------------------------------------------------------------------------------------

    /**
     * Verbatim mirror of the portable-token sample — broker-free, so this mirror is executed
     * directly by the tests below. The test supplies the transport hop: {@code receivedToken}
     * is {@code token} arriving unchanged.
     */
    private static CausalClock portableTokenSample(ConsumerRecord<String, String> m1) {
        // Sender. Extract the relevant dependencies and serialise them.
        CausalClock context = CausalClock.fromRecord(m1)
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
                .require("c1", 0, 42)
                .require("c2", 0, 3)
                .build();
        ConsumerRecord<String, String> m1 = new ConsumerRecord<>("c3", 0, 9L, "k", "v");
        m1.headers().add(ParsleyHeader.CAUSAL_CLOCK, carried.toBytes());

        assertEquals(carried, portableTokenSample(m1),
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
        ConsumerRecord<String, String> m1 = new ConsumerRecord<>("c3", 0, 9L, "k", "v");
        assertEquals(CausalClock.empty(), portableTokenSample(m1),
                "an unstamped record must propagate as the empty clock, not fail");
    }

    // ---------------------------------------------------------------------------------------------
    // docs/guide/streams.md, intro sample: declare a topology and hand it to the runtime
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
                .stream(List.of("c1", "c2"), Serdes.String(), orderSerde)
                .process(new EnrichOrderSupplier())
                .to("c3", Serdes.String(), enrichedSerde)
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
                .stream(List.of("c1", "c2"), Serdes.String(), orderSerde)
                .process(new EnrichOrderSupplier())
                .to("c3", Serdes.String(), enrichedSerde)
                .build();

        try (CausalStreams causalStreams = new CausalStreams(topology, streamsProps())) {
            assertEquals(KafkaStreams.State.CREATED, causalStreams.state(),
                    "the docs' declaration chain must yield a runtime instance the wrapped "
                            + "KafkaStreams accepted (CREATED, ready to start)");
        }
    }

    /** A state directory per instance, so concurrent instances cannot collide on one path. */
    @RegisterExtension
    static final TestStateDirectories STATE_DIRS = new TestStateDirectories("docs-streams-sample-");

    /** The minimal Streams configuration the docs' runtime sample presumes. */
    private static Properties streamsProps() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "docs-streams-sample");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.STATE_DIR_CONFIG, STATE_DIRS.create().toAbsolutePath().toString());
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
