package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.Test;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end proof that an already-running causal topology evolves through an epoch transition driven by
 * {@link ParsleyCoordination} (the mechanism {@link CausalStreams#requestEpochTransition()} delegates to)
 * — over a real {@link TopologyTestDriver} stage (real serdes, real forward path), with the coordination
 * runtime driven synchronously over the in-memory transport.
 */
class ParsleyCoordinationTopologyTest {

    private static final Uuid T1_ID = Uuid.randomUuid();
    private static final ParsleyTopicAdmin ADMIN = TestTopicAdmin.of(Map.of("t1", T1_ID));

    /**
     * A running single-stage topology (t1 is an external source) is evolved through
     * {@code requestEpochTransition()}: genesis seals an empty-floor baseline, then each later transition —
     * with the node running and having delivered records — commits a non-empty floor merged from its own
     * completeness, which the source-layer node injects as a boundary marker to its sink. Asserts the
     * latest epoch boundary reaching the sink carries a real, non-empty cut (not the empty genesis floor).
     */
    @Test
    void runningTopologyEvolvesThroughAnEpochTransition() {
        InMemoryEpochTransport.SharedLog eventLog = new InMemoryEpochTransport.SharedLog();
        ParsleyEpochRuntime runtime = new ParsleyEpochRuntime(new InMemoryEpochTransport(eventLog));
        ParsleyCoordination coordination = ParsleyCoordination.forRuntime(runtime);
        // Bootstrap the runtime (empty log) before the driver's init() runs — a coordinated task's init
        // blocks until the runtime has folded the backlog. This is a cold start (epoch 0), so no join
        // round is opened and init proceeds once bootstrapped.
        runtime.runOnce();

        Topology topology = new CausalStreamsBuilder().topicAdmin(ADMIN)
                .stream("t1", Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("out-sink", "out", Serdes.String(), Serdes.String())
                .build()
                .assemble(config(), new ParsleyQuiesce(), coordination);

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            TestInputTopic<String, String> t1 =
                    driver.createInputTopic("t1", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());
            settle(eventLog, runtime);   // fold the task's join

            // A delivered record advances the node's completeness and sets its last-seen key.
            t1.pipeInput(new TestRecord<>("k", "hello", emptyDeps()));

            // First transition: the node is still pending, so this commits an empty-floor epoch 1 and
            // promotes it to a running member.
            coordination.requestEpochTransition();
            settle(eventLog, runtime);
            t1.pipeInput(new TestRecord<>("k", "world", emptyDeps()));   // poll adopts+injects epoch 1

            // Second transition: the node is now running and has delivered records, so it publishes its
            // completeness and the owner commits a non-empty floor.
            coordination.requestEpochTransition();
            settle(eventLog, runtime);
            t1.pipeInput(new TestRecord<>("k", "again", emptyDeps()));   // poll publishes + injects snapshot
            settle(eventLog, runtime);
            t1.pipeInput(new TestRecord<>("k", "more", emptyDeps()));    // poll adopts+injects epoch 2

            ParsleyEpochBoundary latest = out.readRecordsToList().stream()
                    .map(ParsleyCoordinationTopologyTest::boundaryOf)
                    .flatMap(Optional::stream)
                    .max(java.util.Comparator.comparingLong(ParsleyEpochBoundary::epochId))
                    .orElseThrow(() -> new AssertionError("no epoch boundary marker reached the sink"));

            assertTrue(latest.epochId() >= 2,
                    "the running node transitioned past genesis (epoch 1) through the public API");
            assertFalse(latest.lowerBounds().isEmpty(),
                    "the latest epoch's floor is a real cut merged from the running node's completeness, "
                            + "not the empty genesis floor");
        }
    }

    private static Optional<ParsleyEpochBoundary> boundaryOf(TestRecord<String, String> record) {
        for (Header h : record.headers()) {
            if (ParsleyHeader.EPOCH_BOUNDARY.equals(h.key()) && h.value() != null) {
                return Optional.of(ParsleyEpochBoundary.fromBytes(h.value()));
            }
        }
        return Optional.empty();
    }

    /** A user processor that forwards each value upper-cased. */
    private static ProcessorSupplier<String, String, String, String> upperCaser() {
        return () -> new Processor<>() {
            private ProcessorContext<String, String> ctx;

            @Override
            public void init(ProcessorContext<String, String> context) {
                this.ctx = context;
            }

            @Override
            public void process(Record<String, String> record) {
                ctx.forward(record.withValue(record.value().toUpperCase(Locale.ROOT)));
            }
        };
    }

    private static org.apache.kafka.common.header.Headers emptyDeps() {
        org.apache.kafka.common.header.Headers headers = ParsleyHeader.mutableHeaders();
        headers.add(ParsleyHeader.CAUSAL_DEPENDENCIES, ParsleyVectorClock.empty().toBytes());
        return headers;
    }

    private static Properties config() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "causal-coordination-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        return props;
    }

    /** Folds the runtime until the shared log stops growing (three quiet passes). */
    private static void settle(InMemoryEpochTransport.SharedLog log, ParsleyEpochRuntime runtime) {
        int quietPasses = 0;
        while (quietPasses < 3) {
            long before = log.events().size();
            runtime.runOnce();
            quietPasses = (log.events().size() == before) ? quietPasses + 1 : 0;
        }
    }
}
