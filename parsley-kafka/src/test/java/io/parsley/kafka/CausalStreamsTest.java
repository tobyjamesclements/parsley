package io.parsley.kafka;

import io.parsley.BufferLimit;
import io.parsley.BufferingPolicy;
import io.parsley.CausalViolationHandler;
import io.parsley.ParsleyAttributes;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class CausalStreamsTest {

    private static final KafkaVectorClockSerialiser SERIALISER = new KafkaVectorClockSerialiser();

    private static CausalProcessorSupplier<String, String> supplier() {
        return new CausalProcessorSupplier<>(
                BufferingPolicy.ignore(BufferLimit.ofDuration(Duration.ofSeconds(30))),
                CausalViolationHandler.noop(), SERIALISER);
    }

    private static Properties driverConfig() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "causal-streams-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1");
        return props;
    }

    @Test
    void processInsertsCausalOrderingIntoTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream =
                builder.stream("in", Consumed.with(Serdes.String(), Serdes.String()));
        CausalStreams.process(stream, supplier())
                .to("out", Produced.with(Serdes.String(), Serdes.String()));

        assertRoundTrip(builder);
    }

    @Test
    void processWithNamedInsertsCausalOrderingIntoTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream =
                builder.stream("in", Consumed.with(Serdes.String(), Serdes.String()));
        CausalStreams.process(stream, supplier(), Named.as("causal"))
                .to("out", Produced.with(Serdes.String(), Serdes.String()));

        assertRoundTrip(builder);
    }

    private static void assertRoundTrip(StreamsBuilder builder) {
        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), driverConfig())) {
            TestInputTopic<String, String> in =
                    driver.createInputTopic("in", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());

            RecordHeaders headers = new RecordHeaders();
            headers.add(new RecordHeader(ParsleyAttributes.VECTOR_CLOCK,
                    SERIALISER.serialise(KafkaVectorClock.empty())));
            in.pipeInput(new TestRecord<>("k", "v", headers));

            TestRecord<String, String> received = out.readRecord();
            assertEquals("k", received.key());
            assertEquals("v", received.value());
        }
    }
}
