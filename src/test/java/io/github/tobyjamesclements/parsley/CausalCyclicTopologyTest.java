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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Proves the headline capability the max-merge redesign exists to enable: a node directly consuming both
 * an ancestor topic and its own descendant delivers correctly, where the old {@code
 * ParsleyVectorClock#intersectMin} gate made this a documented, permanent deadlock (retired restriction: "no
 * ancestor with its own descendant", {@code docs/internals/causal-consistency.md}) — the tightest possible
 * shape of it: a single node directly self-consuming its own sink.
 *
 * <p>This also exercises relay convergence on a self-loop: a node observing its own null message
 * reflected back at it must not relay it onward again (an infinite loop this test caught during
 * development, historically closed by an own-coordinate strip). Convergence now rests on the I6
 * knowledge-based relay rule — a reflected marker's carried clock is this node's own past stamp,
 * dominated by its current {@code stamp()}, so it teaches nothing and the relay settles — and the
 * self-consumed sink's claims are genuinely gated by the two-branch gate's consumed branch (T3.1).
 * Under max-merge this works because a node's own registered channel for the looped-back topic
 * directly advertises the ancestor coordinate it needs ({@link ParsleyChannels#channelUpdate}) — no
 * cross-channel unanimity, and no third-party relay, is required.
 *
 * <p>A genuine <em>two-node</em> cycle (A→B→A over two separate, real Kafka Streams applications) is
 * proved instead in {@link ParsleyCyclicReflectionIT} — a single-Topology, two-stage
 * {@link TopologyTestDriver} version of that scenario is not meaningful: Kafka Streams allows only one
 * source node per topic per {@code Topology}, so two logically-separate stages sharing one root topic
 * (one directly, one via passthrough) cannot coexist in one {@code Topology} the way two genuinely
 * separate applications' independent topologies can.
 */
class CausalCyclicTopologyTest {

    private static final Uuid T1_ID = Uuid.randomUuid();
    private static final Uuid P_OUT_ID = Uuid.randomUuid();
    private static final ParsleyTopicAdmin ADMIN = TestTopicAdmin.of(Map.of("t1", T1_ID, "p-out", P_OUT_ID));

    /**
     * P registers both {@code t1} (an external root) and {@code p-out} (P's own sink) as inputs. A
     * {@code t1} record derives a {@code p-out} record; that {@code p-out} record loops back into P as a
     * second, distinct delivery — the delegate recognises it (by source topic) and does not forward
     * again, so the loop terminates after one round trip.
     *
     * Asserts: the delegate runs for both the original {@code t1} delivery and the looped-back {@code
     * p-out} delivery (proving the self-consumed record is genuinely gated and delivered, not dropped);
     * exactly one record is ever emitted to {@code p-out} (the loop does not re-emit);
     * and that emitted record's stamped dependency names {@code t1@0} — the ancestor coordinate — proving
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
                if ("t1".equals(sourceTopic)) {
                    ctx.forward(record.withValue("derived:" + record.value()));
                }
                // A record sourced from p-out is the looped-back derivative itself — do not re-forward,
                // so the self-loop terminates after exactly one round trip.
            }
        };

        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(List.of("t1", "p-out"), Consumed.with(Serdes.String(), Serdes.String()))
                .process(ParsleyProcessorSupplier.builder(selfLooper)
                        .addBufferStore("parsley")
                        .addSource(new ParsleySource<>("t1", Serdes.String(), Serdes.String()))
                        .addSource(new ParsleySource<>("p-out", Serdes.String(), Serdes.String()))
                        // Declares p-out as P's own produced topic: feeds the ownOutputs clock the
                        // I6 knowledge-based relay compares against (a reflected own claim teaches
                        // nothing, so the loop settles) and the I8 reflected-claim diagnostic.
                        .sinkTopics(Set.of("p-out"))
                        .topicAdmin(ADMIN)
                        .build())
                .to("p-out", Produced.with(Serdes.String(), Serdes.String()));
        Topology topology = builder.build();

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            TestInputTopic<String, String> t1 =
                    driver.createInputTopic("t1", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> pOut =
                    driver.createOutputTopic("p-out", new StringDeserializer(), new StringDeserializer());

            t1.pipeInput(new TestRecord<>("k", "hello", emptyDeps()));

            assertEquals(List.of("t1:hello", "p-out:derived:hello"), delegateSaw,
                    "the delegate must see both the original t1 delivery and the looped-back p-out "
                            + "delivery, in order — proving the self-consumed record is genuinely "
                            + "delivered, not dropped");

            // The business record (derived:hello) plus a trailing heartbeat null message (null value) that
            // the self-loop's own non-emitting p-out-sourced delivery produces — see deliver()'s
            // "nothing forwarded" path. The null message itself must not trigger a further relay (that is
            // exactly the bug this fix closes): the driver returning here at all, rather than hanging,
            // is itself part of what this test proves.
            List<TestRecord<String, String>> emitted = pOut.readRecordsToList();
            List<String> businessValues = emitted.stream().map(TestRecord::value).filter(v -> v != null).toList();
            assertEquals(List.of("derived:hello"), businessValues,
                    "the loop must terminate after one round trip — no second business p-out record");

            TestRecord<String, String> businessRecord = emitted.stream()
                    .filter(r -> r.value() != null).findFirst().orElseThrow();
            ParsleyVectorClock stampedDeps = dependenciesOf(businessRecord);
            assertTrue(stampedDeps.dominates(ParsleyVectorClock.empty().observe(T1_ID, 0, 0)),
                    "the emitted p-out record's stamp must carry the ancestor coordinate (t1@0) it was "
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

    private static Properties config() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "causal-cyclic-topology-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return props;
    }
}
