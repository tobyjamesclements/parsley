package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Proves the headline capability max-merge exists to enable: a node directly consuming both an
 * ancestor topic and its own descendant delivers correctly, in the tightest possible shape of it —
 * a single node directly self-consuming its own sink. A gate folding channel clocks at an
 * intersection minimum, admitting a coordinate only once every channel confirms it, deadlocks
 * permanently on this shape, because the descendant channel can never confirm the ancestor
 * coordinate the descendant is derived from.
 *
 * <p>This also exercises relay convergence on a self-loop: a node observing its own null message
 * reflected back at it must not relay it onward again, or it loops forever. Convergence rests on
 * the knowledge-based relay rule — a reflected marker's carried clock is this node's own past stamp,
 * dominated by its current {@code stamp()}, so it teaches nothing and the relay settles — and the
 * self-consumed sink's claims are genuinely gated by the two-branch gate's consumed branch.
 * Under max-merge this works because a node's own registered channel for the looped-back topic
 * directly advertises the ancestor coordinate it needs ({@link ParsleyChannels#channelUpdate}) — no
 * cross-channel unanimity, and no third-party relay, is required.
 *
 * <p>A genuine <em>two-node</em> cycle (A→B→A over two separate, real Kafka Streams applications) is
 * proved instead in {@link ParsleyCyclicReflectionIT} — a single-Topology, two-stage
 * {@link TopologyTestDriver} version of that scenario is not meaningful: Kafka Streams allows only one
 * source node per topic per {@code Topology}, so two logically-separate stages sharing one root topic
 * cannot coexist in one {@code Topology} the way two genuinely separate applications' independent
 * topologies can.
 */
class CausalCyclicTopologyTest {

    private static final Uuid C1_ID = Uuid.randomUuid();
    private static final Uuid C2_ID = Uuid.randomUuid();
    private static final ParsleyTopicAdmin ADMIN = TestTopicAdmin.of(Map.of("c1", C1_ID, "c2", C2_ID));

    /**
     * P registers both {@code c1} (an external root) and {@code c2} (P's own sink) as inputs. A
     * {@code c1} record derives a {@code c2} record; that {@code c2} record loops back into P as a
     * second, distinct delivery — the delegate recognises it (by source topic) and does not forward
     * again, so the loop terminates after one round trip.
     *
     * Asserts: the delegate runs for both the original {@code c1} delivery and the looped-back {@code
     * c2} delivery (proving the self-consumed record is genuinely gated and delivered, not dropped);
     * exactly one record is ever emitted to {@code c2} (the loop does not re-emit);
     * and that emitted record's stamped dependency names {@code c1@0} — the ancestor coordinate — proving
     * the derived record's true causal history is preserved across the self-loop.
     */
    @Test
    void nodeConsumingItsOwnDescendantDeliversInsteadOfDeadlocking() {
        List<String> delegateSaw = new ArrayList<>();
        ProcessorSupplier<String, String, String, String> selfLooper = () -> new Processor<>() {
            private ProcessorContext<String, String> ctx;

            @Override
            public void init(ProcessorContext<String, String> context) {
                this.ctx = context;
            }

            @Override
            public void process(Record<String, String> record) {
                String sourceTopic = ctx.recordMetadata().map(RecordMetadata::topic).orElse("");
                delegateSaw.add(sourceTopic + ":" + record.value());
                if ("c1".equals(sourceTopic)) {
                    ctx.forward(record.withValue("derived:" + record.value()));
                }
                // A record sourced from c2 is the looped-back derivative itself — do not re-forward,
                // so the self-loop terminates after exactly one round trip.
            }
        };

        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(List.of("c1", "c2"), Consumed.with(Serdes.String(), Serdes.String()))
                .process(ParsleyProcessorSupplier.builder(selfLooper)
                        .addBufferStore("parsley")
                        .addSource(new ParsleySource<>("c1", Serdes.String(), Serdes.String()))
                        .addSource(new ParsleySource<>("c2", Serdes.String(), Serdes.String()))
                        // Declares c2 as P's own produced topic: feeds the ownOutputs clock the
                        // knowledge-based relay compares against (a reflected own claim teaches
                        // nothing, so the loop settles) and the reflected-claim diagnostic.
                        .sinkTopics(Set.of("c2"))
                        .topicAdmin(ADMIN)
                        .build())
                .to("c2", Produced.with(Serdes.String(), Serdes.String()));
        Topology topology = builder.build();

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            TestInputTopic<String, String> c1 =
                    driver.createInputTopic("c1", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> c2Out =
                    driver.createOutputTopic("c2", new StringDeserializer(), new StringDeserializer());

            c1.pipeInput(new TestRecord<>("k", "hello", emptyDeps()));

            assertEquals(List.of("c1:hello", "c2:derived:hello"), delegateSaw,
                    "the delegate must see both the original c1 delivery and the looped-back c2 "
                            + "delivery, in order — proving the self-consumed record is genuinely "
                            + "delivered, not dropped");

            // The business record (derived:hello) plus a trailing heartbeat null message (null value) that
            // the self-loop's own non-emitting c2-sourced delivery produces — see deliver()'s
            // "nothing forwarded" path. The null message itself must not trigger a further relay (that is
            // exactly the bug this fix closes): the driver returning here at all, rather than hanging,
            // is itself part of what this test proves.
            List<TestRecord<String, String>> emitted = c2Out.readRecordsToList();
            List<String> businessValues = emitted.stream().map(TestRecord::value).filter(v -> v != null).toList();
            assertEquals(List.of("derived:hello"), businessValues,
                    "the loop must terminate after one round trip — no second business c2 record");

            TestRecord<String, String> businessRecord = emitted.stream()
                    .filter(r -> r.value() != null).findFirst().orElseThrow();
            ParsleyVectorClock stampedDeps = dependenciesOf(businessRecord);
            assertTrue(stampedDeps.dominates(ParsleyVectorClock.empty().observe(C1_ID, 0, 0)),
                    "the emitted c2 record's stamp must carry the ancestor coordinate (c1@0) it was "
                            + "genuinely derived from");
        }
    }

    private static Headers emptyDeps() {
        Headers headers = ParsleyHeader.mutableHeaders();
        headers.add(ParsleyHeader.CAUSAL_CLOCK, ParsleyVectorClock.empty().toBytes());
        return headers;
    }

    private static ParsleyVectorClock dependenciesOf(TestRecord<String, String> record) {
        for (org.apache.kafka.common.header.Header header : record.headers()) {
            if (ParsleyHeader.CAUSAL_CLOCK.equals(header.key()) && header.value() != null) {
                return ParsleyVectorClock.fromBytes(header.value());
            }
        }
        return ParsleyVectorClock.empty();
    }

    /** A state directory per driver: the application id is fixed, so a shared one would collide. */
    @RegisterExtension
    static final TestStateDirectories STATE_DIRS =
            new TestStateDirectories("causal-cyclic-topology-test-");

    private static Properties config() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "causal-cyclic-topology-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.STATE_DIR_CONFIG, STATE_DIRS.create().toAbsolutePath().toString());
        return props;
    }
}
