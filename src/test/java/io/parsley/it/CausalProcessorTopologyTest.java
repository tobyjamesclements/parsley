package io.parsley.it;

import io.parsley.BufferLimit;
import io.parsley.BufferingPolicy;
import io.parsley.CausalViolationHandler;
import io.parsley.VectorClock;
import io.parsley.internal.Attributes;
import io.parsley.stream.CausalProcessorSupplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
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
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises {@link CausalProcessorSupplier} through a real Kafka Streams topology using the
 * {@link TopologyTestDriver} (no broker required).
 */
class CausalProcessorTopologyTest {

    private static final String VECTOR_CLOCK_HEADER = Attributes.VECTOR_CLOCK;
    private static final TopicPartition PRICES_0 = new TopicPartition("prices", 0);

    private static Properties config() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "causal-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return props;
    }

    private static Headers clockHeader(VectorClock clock) {
        Headers headers = new RecordHeaders();
        headers.add(new RecordHeader(VECTOR_CLOCK_HEADER, clock.toBytes()));
        return headers;
    }

    @Test
    void outOfOrderRecordIsHeldUntilItsCausalPremiseArrives() {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(List.of("prices", "orders"), Consumed.with(Serdes.String(), Serdes.String()))
                .process(CausalProcessorSupplier.create(
                        BufferingPolicy.ignore(BufferLimit.ofSize(100)),
                        CausalViolationHandler.noop()))
                .to("out", Produced.with(Serdes.String(), Serdes.String()));

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), config())) {
            TestInputTopic<String, String> prices =
                    driver.createInputTopic("prices", new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> orders =
                    driver.createInputTopic("orders", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());

            // The order depends on prices-0 reaching offset 0, but arrives first.
            orders.pipeInput(new TestRecord<>("k", "order",
                    clockHeader(VectorClock.empty().advance(PRICES_0, 0))));
            assertTrue(out.isEmpty(), "order must be buffered until its price premise arrives");

            // The price arrives (prices-0 offset 0), releasing the order in causal order.
            prices.pipeInput(new TestRecord<>("k", "price", clockHeader(VectorClock.empty())));

            List<String> values = out.readValuesToList();
            assertEquals(List.of("price", "order"), values);
        }
    }

    @Test
    void recordWithoutClockHeaderPassesThrough() {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("prices", Consumed.with(Serdes.String(), Serdes.String()))
                .process(CausalProcessorSupplier.create(
                        BufferingPolicy.ignore(BufferLimit.ofSize(100)),
                        CausalViolationHandler.noop()))
                .to("out", Produced.with(Serdes.String(), Serdes.String()));

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), config())) {
            TestInputTopic<String, String> prices =
                    driver.createInputTopic("prices", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());

            prices.pipeInput("k", "no-clock");

            assertEquals(List.of("no-clock"), out.readValuesToList());
        }
    }

    @Test
    void deadLetterPolicyRoutesUnresolvableDependenciesToSink() {
        List<ConsumerRecord<String, String>> deadLettered = new ArrayList<>();
        BufferingPolicy.DeadLetter policy =
                (BufferingPolicy.DeadLetter) BufferingPolicy.deadLetter(BufferLimit.ofSize(1), "dlq");

        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("orders", Consumed.with(Serdes.String(), Serdes.String()))
                .process(CausalProcessorSupplier.create(policy, CausalViolationHandler.noop(), deadLettered::add))
                .to("out", Produced.with(Serdes.String(), Serdes.String()));

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), config())) {
            TestInputTopic<String, String> orders =
                    driver.createInputTopic("orders", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out =
                    driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());

            // Depends on a price that never arrives; size limit 1 evicts it straight to the sink.
            orders.pipeInput(new TestRecord<>("k", "order",
                    clockHeader(VectorClock.empty().advance(PRICES_0, 99))));

            assertTrue(out.isEmpty());
            assertEquals(1, deadLettered.size());
            assertEquals("order", deadLettered.get(0).value());
        }
    }
}
