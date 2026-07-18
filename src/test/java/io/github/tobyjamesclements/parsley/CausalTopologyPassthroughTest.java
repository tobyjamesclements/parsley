package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteArraySerializer;
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
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Exercises {@code parsley.coordination.domain-topics} passthrough auto-wiring in {@link CausalTopology}:
 * a stage that does not directly consume or produce a domain topic gets a dedicated passthrough processor
 * node instead, so its declared subscriptions still cover the full coordinated domain.
 */
class CausalTopologyPassthroughTest {

    private static final Uuid T1_ID = Uuid.randomUuid();
    private static final Uuid OUT_ID = Uuid.randomUuid();
    private static final Uuid PASSTHROUGH_ID = Uuid.randomUuid();
    private static final ParsleyTopicAdmin ADMIN =
            TestTopicAdmin.of(Map.of("t1", T1_ID, "out", OUT_ID, "passthrough1", PASSTHROUGH_ID));

    /**
     * A single stage consuming only {@code t1} and producing only {@code out}, with a coordinated domain
     * of {@code t1,out,passthrough1}, gets {@code passthrough1} wired as an extra, raw byte[]/byte[] source
     * into its one processor node — so startup's full-mesh self-check passes (this member's declared
     * topics, widened to include the passthrough topic, cover the whole domain) even though the stage's
     * own delegate never consumes it.
     *
     * <p>Also proves the passthrough source's frontier contribution is genuine, not cosmetic — and proves
     * the shared-buffer/candidate-index correctness property this design depends on: a {@code t1} record
     * depending on {@code passthrough1}'s coordinate is buffered until a record actually arrives on
     * {@code passthrough1}; that record's own delivery releases the held {@code t1} record too, as a side
     * effect of the shared buffer, and the released record must still reach the real business delegate
     * correctly — while the passthrough record itself never does, structurally, not merely by chance.
     */
    @Test
    void passthroughTopicSatisfiesFullMeshWithoutReachingTheDelegate() {
        List<String> delegateSaw = new ArrayList<>();
        ProcessorSupplier<String, String, String, String> delegate = () -> new Processor<>() {
            private ProcessorContext<String, String> ctx;

            @Override
            public void init(ProcessorContext<String, String> context) {
                this.ctx = context;
            }

            @Override
            public void process(Record<String, String> record) {
                delegateSaw.add(record.value());
                ctx.forward(record);
            }
        };

        InMemoryEpochTransport.SharedLog eventLog = new InMemoryEpochTransport.SharedLog();
        ParsleyEpochRuntime runtime = new ParsleyEpochRuntime(new InMemoryEpochTransport(eventLog));
        ParsleyCoordination coordination = ParsleyCoordination.forRuntime(runtime);
        runtime.runOnce();

        Topology topology = new CausalStreamsBuilder().topicAdmin(ADMIN)
                .stream("t1", Serdes.String(), Serdes.String())
                .process(delegate)
                .to("out-sink", "out", Serdes.String(), Serdes.String())
                .build().assemble(config(), new ParsleyQuiesce(), coordination);

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            TestInputTopic<String, String> t1 =
                    driver.createInputTopic("t1", new StringSerializer(), new StringSerializer());
            TestInputTopic<byte[], byte[]> passthrough1 = driver.createInputTopic(
                    "passthrough1", new ByteArraySerializer(), new ByteArraySerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());
            settle(eventLog, runtime);   // fold the task's join — must not throw (full-mesh self-check passes)

            // A t1 record depending on passthrough1@0 — nothing has arrived on that channel yet, so it holds.
            // Asserted on the delegate (the real business-delivery signal): the raw sink queue also carries
            // the genesis epoch-boundary marker this node self-injects, which is not a business delivery.
            t1.pipeInput(new TestRecord<>("k", "gated", dependsOn(PASSTHROUGH_ID, 0)));
            assertEquals(List.of(), delegateSaw,
                    "a t1 record depending on an unseen passthrough1 offset must hold, not reach the delegate");

            // A genuine record arrives on the passthrough topic. Its own delivery releases the held t1
            // record too (same shared buffer/candidate-index, same single processor node) — that record
            // must still reach the real delegate correctly; the passthrough record itself must not.
            passthrough1.pipeInput(new TestRecord<>((byte[]) null, (byte[]) null, emptyDeps()));
            assertEquals(List.of("gated"), delegateSaw,
                    "the released t1 record must reach the real delegate; the passthrough record must not");

            // Both the released business record (real value) and a watermark (null value, standing in for
            // the passthrough node's own non-emitting "delivery") land on the shared sink.
            List<String> releasedValues = out.readRecordsToList().stream()
                    .map(TestRecord::value)
                    .filter(v -> v != null)
                    .toList();
            assertEquals(List.of("gated"), releasedValues,
                    "the held t1 record must be released once passthrough1@0 is genuinely delivered");
        }
    }

    private static Headers emptyDeps() {
        Headers headers = ParsleyHeader.mutableHeaders();
        headers.add(ParsleyHeader.CAUSAL_DEPENDENCIES, ParsleyClock.empty().toBytes());
        return headers;
    }

    private static Headers dependsOn(Uuid topicId, long offset) {
        Headers headers = ParsleyHeader.mutableHeaders();
        ParsleyClock deps = ParsleyClock.empty().observe(topicId, 0, offset);
        headers.add(ParsleyHeader.CAUSAL_DEPENDENCIES, deps.toBytes());
        return headers;
    }

    private static Properties config() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "causal-topology-passthrough-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        // Not actually consulted by CausalTopology#assemble to build the coordination object itself (this
        // test wires ParsleyCoordination directly over the in-memory transport) — only required to satisfy
        // ParsleyConfig#from's domain-topics/epoch-events-topic pairing validation.
        props.put("parsley.coordination.epoch-events-topic", "epoch-events");
        props.put("parsley.coordination.domain-topics", "t1,out,passthrough1");
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
