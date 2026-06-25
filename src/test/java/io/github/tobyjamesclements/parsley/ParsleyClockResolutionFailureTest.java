package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
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
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises {@code parsley.clock.resolution.failure.policy} at ingest: when an inbound record's
 * {@code parsley-causal-dependencies} header cannot be decoded into a clock, the {@code fail} default
 * fails the task fast and forwards nothing, while {@code continue} forwards the record with empty
 * (vacuously satisfied) dependencies, counted as a violation. Both branches meter and audit the
 * occurrence. Driven through a real Kafka Streams topology via {@link TopologyTestDriver}.
 */
class ParsleyClockResolutionFailureTest {

    private static final Uuid T1_ID = Uuid.randomUuid();

    // Fake admin resolving the test topic's UUID (no broker under TopologyTestDriver).
    private static final ParsleyTopicAdmin ADMIN = TestTopicAdmin.of(Map.of("t1", T1_ID));

    private final List<String> processed = new ArrayList<>();

    /**
     * Under the default {@code fail} policy, an undecodable causal-dependencies header fails the task
     * fast: the delegate never runs, the failure surfaces (wrapped by Streams in a
     * {@code StreamsException}) as a {@link ParsleyClockResolutionException}, and the audit records
     * the failure with {@code failed = true}.
     */
    @Test
    void failPolicyFailsFastAndAuditsTheUnresolvableClock() {
        RecordingCausalAudit audit = new RecordingCausalAudit();
        Topology topology = topology(CausalProcessors.builder(upperCaser())
                .addBufferStore("parsley", CausalBufferLimit.ofSize(100))
                .addBuffer(CausalBuffer.of("t1", Serdes.String(), Serdes.String()))
                .withAudit(audit).topicAdmin(ADMIN).build());

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            TestInputTopic<String, String> t1 =
                    driver.createInputTopic("t1", new StringSerializer(), new StringSerializer());

            StreamsException thrown = assertThrows(StreamsException.class,
                    () -> t1.pipeInput(new TestRecord<>("k", "v", garbledDepsHeader())),
                    "an undecodable dependencies header must fail the task fast under the default policy");

            assertEquals(ParsleyClockResolutionException.class, thrown.getCause().getClass(),
                    "the wrapped cause must be the typed clock-resolution exception");
            assertTrue(processed.isEmpty(), "the delegate must not run when the clock is unresolvable and policy is fail");
            assertEquals(1, audit.clockResolutionFailures.size(), "the unresolvable clock must be audited");
            RecordingCausalAudit.ClockResolutionFailure failure = audit.clockResolutionFailures.get(0);
            assertEquals("t1", failure.topic(), "audit must name the record's source topic");
            assertEquals(0L, failure.offset(), "audit must name the record's source offset");
            assertTrue(failure.failed(), "the fail policy must be reflected in the audit");
        }
    }

    /**
     * Under {@code continue}, an undecodable causal-dependencies header is treated as empty: the
     * record is forwarded immediately (its transform applied), a violation is counted, the
     * clock-resolution-error metric is incremented, and the audit records the failure with
     * {@code failed = false}.
     */
    @Test
    void continuePolicyForwardsEmptyDependenciesAndCountsAViolation() {
        RecordingCausalAudit audit = new RecordingCausalAudit();
        Topology topology = topology(CausalProcessors.builder(upperCaser())
                .addBufferStore("parsley", CausalBufferLimit.ofSize(100))
                .addBuffer(CausalBuffer.of("t1", Serdes.String(), Serdes.String()))
                .withConfig("parsley.clock.resolution.failure.policy", "continue")
                .withAudit(audit).topicAdmin(ADMIN).build());

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            TestInputTopic<String, String> t1 =
                    driver.createInputTopic("t1", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());

            t1.pipeInput(new TestRecord<>("k", "hello", garbledDepsHeader()));

            assertEquals(List.of("hello"), processed,
                    "continue must forward the record with empty dependencies rather than fail");
            assertEquals("HELLO", out.readRecord().value(), "the forwarded record's transform must still be applied");
            assertEquals(1.0, parsleyMetric(driver, "clock-resolution-errors-total"), 0.001,
                    "the unresolvable clock must increment the clock-resolution-error metric");
            assertEquals(1.0, parsleyMetric(driver, "violations-total"), 0.001,
                    "forwarding on empty dependencies counts a causal violation");

            assertEquals(1, audit.clockResolutionFailures.size(), "the unresolvable clock must be audited");
            assertFalse(audit.clockResolutionFailures.get(0).failed(),
                    "the continue policy must be reflected in the audit");
        }
    }

    // --- helpers -----------------------------------------------------------------------------------

    private static Properties config() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "clock-resolution-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return props;
    }

    /** A dependencies header whose bytes cannot be decoded into a clock. */
    private static Headers garbledDepsHeader() {
        Headers headers = ParsleyHeader.mutableHeaders();
        headers.add("parsley-causal-dependencies", new byte[]{9, 9, 9});
        return headers;
    }

    /** A user processor that records every value it sees and forwards it upper-cased. */
    private ProcessorSupplier<String, String, String, String> upperCaser() {
        return () -> new Processor<>() {
            private ProcessorContext<String, String> ctx;

            @Override
            public void init(ProcessorContext<String, String> context) {
                this.ctx = context;
            }

            @Override
            public void process(Record<String, String> record) {
                processed.add(record.value());
                ctx.forward(record.withValue(record.value().toUpperCase(Locale.ROOT)));
            }
        };
    }

    private static Topology topology(ProcessorSupplier<String, String, String, String> decorated) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(List.of("t1"), Consumed.with(Serdes.String(), Serdes.String()))
                .process(decorated)
                .to("out", Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }

    private static double parsleyMetric(TopologyTestDriver driver, String metricName) {
        Map<MetricName, ? extends Metric> all = driver.metrics();
        return all.entrySet().stream()
                .filter(e -> e.getKey().name().equals(metricName)
                          && e.getKey().group().contains("parsley"))
                .findFirst()
                .map(e -> ((Number) e.getValue().metricValue()).doubleValue())
                .orElseThrow(() -> new AssertionError("Parsley metric not found: " + metricName));
    }
}
