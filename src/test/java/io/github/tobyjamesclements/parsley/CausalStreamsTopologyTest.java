package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.header.Headers;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Exercises {@link CausalStreams} — the topology-owning high-level causal API — through a real
 * Kafka Streams topology using the {@link TopologyTestDriver} (no broker required).
 */
class CausalStreamsTopologyTest {

    // t1/t2 = single- and dual-source test topics; out = the sink.
    private static final Uuid T1_ID = Uuid.randomUuid();
    private static final Uuid T2_ID = Uuid.randomUuid();

    private static final ParsleyTopicAdmin ADMIN = TestTopicAdmin.of(Map.of("t1", T1_ID, "t2", T2_ID));

    private final List<String> processed = new ArrayList<>();

    // --- helpers -------------------------------------------------------------------------------

    private static Properties config() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "causal-streams-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return props;
    }

    private static Headers depsHeader(CausalDependencies deps) {
        Headers headers = ParsleyHeader.mutableHeaders();
        headers.add("parsley-causal-dependencies", deps.toBytes());
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

    // --- tests ---------------------------------------------------------------------------------

    /**
     * A single-source, single-sink causal stage: the built {@link Topology} wires a source for the
     * one registered {@link CausalBuffer}, a causal-decorated processor, and the one registered sink,
     * and an admitted record flows delegate-transformed through to the sink.
     *
     * Asserts the delegate runs and the sink receives the transformed, causally-stamped record.
     */
    @Test
    void singleSourceStageDeliversAdmittedRecordToItsSink() {
        Topology topology = CausalStreams.builder(upperCaser())
                .addBufferStore("parsley", CausalBufferLimit.ofSize(100))
                .addSource(CausalBuffer.of("t1", Serdes.String(), Serdes.String()))
                .addSink("out-sink", "out", Serdes.String(), Serdes.String())
                .topicAdmin(ADMIN)
                .build();

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            TestInputTopic<String, String> t1 =
                    driver.createInputTopic("t1", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());

            t1.pipeInput(new TestRecord<>("k", "hello", depsHeader(CausalDependencies.empty())));

            assertEquals(List.of("hello"), processed, "delegate.process must run for an admitted record");
            TestRecord<String, String> emitted = out.readRecord();
            assertEquals("HELLO", emitted.value(), "delegate's transform must be applied");
        }
    }

    /**
     * A two-source causal stage fans both source topics into the same processor node: a record
     * consumed from either source reaches the delegate and is forwarded to the shared sink.
     *
     * Asserts records from both t1 and t2 reach the delegate and are forwarded.
     */
    @Test
    void twoSourceStageFansBothSourcesIntoTheSameProcessor() {
        Topology topology = CausalStreams.builder(upperCaser())
                .addBufferStore("parsley", CausalBufferLimit.ofSize(100))
                .addSource(CausalBuffer.of("t1", Serdes.String(), Serdes.String()))
                .addSource(CausalBuffer.of("t2", Serdes.String(), Serdes.String()))
                .addSink("out-sink", "out", Serdes.String(), Serdes.String())
                .topicAdmin(ADMIN)
                .build();

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            TestInputTopic<String, String> t1 =
                    driver.createInputTopic("t1", new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> t2 =
                    driver.createInputTopic("t2", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());

            t1.pipeInput(new TestRecord<>("k", "fromT1", depsHeader(CausalDependencies.empty())));
            t2.pipeInput(new TestRecord<>("k", "fromT2", depsHeader(CausalDependencies.empty())));

            assertEquals(List.of("fromT1", "fromT2"), processed,
                    "both sources must feed the same processor node");
            List<TestRecord<String, String>> emitted = out.readRecordsToList();
            assertEquals(2, emitted.size(), "both source records must be forwarded to the shared sink");
        }
    }

    /**
     * {@link CausalStreams.Builder#build()} rejects a stage missing its buffer store.
     *
     * Asserts an {@link IllegalStateException} naming the missing buffer store.
     */
    @Test
    void buildFailsWithoutABufferStore() {
        CausalStreams.Builder<String, String, String, String> builder = CausalStreams.builder(upperCaser())
                .addSource(CausalBuffer.of("t1", Serdes.String(), Serdes.String()))
                .addSink("out-sink", "out", Serdes.String(), Serdes.String())
                .topicAdmin(ADMIN);

        IllegalStateException e = assertThrows(IllegalStateException.class, builder::build,
                "build() must fail without a buffer store");
        assertEquals(true, e.getMessage().contains("buffer store"), "message must name the missing buffer store");
    }

    /**
     * {@link CausalStreams.Builder#build()} rejects a stage with no registered source.
     *
     * Asserts an {@link IllegalStateException} naming the missing source.
     */
    @Test
    void buildFailsWithoutASource() {
        CausalStreams.Builder<String, String, String, String> builder = CausalStreams.builder(upperCaser())
                .addBufferStore("parsley", CausalBufferLimit.ofSize(100))
                .addSink("out-sink", "out", Serdes.String(), Serdes.String())
                .topicAdmin(ADMIN);

        IllegalStateException e = assertThrows(IllegalStateException.class, builder::build,
                "build() must fail without at least one source");
        assertEquals(true, e.getMessage().contains("source"), "message must name the missing source");
    }

    /**
     * {@link CausalStreams.Builder#build()} rejects a stage with no registered sink.
     *
     * Asserts an {@link IllegalStateException} naming the missing sink.
     */
    @Test
    void buildFailsWithoutASink() {
        CausalStreams.Builder<String, String, String, String> builder = CausalStreams.builder(upperCaser())
                .addBufferStore("parsley", CausalBufferLimit.ofSize(100))
                .addSource(CausalBuffer.of("t1", Serdes.String(), Serdes.String()))
                .topicAdmin(ADMIN);

        IllegalStateException e = assertThrows(IllegalStateException.class, builder::build,
                "build() must fail without at least one sink");
        assertEquals(true, e.getMessage().contains("sink"), "message must name the missing sink");
    }
}
