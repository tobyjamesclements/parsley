package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static java.util.Objects.requireNonNull;

/**
 * The two-output diamond, end to end against a real broker (T2.3 IT b): one invocation of app P's
 * delegate forwards to two sink topics in one EOS transaction. The crossing wait (O1) blocks the
 * second forward's stamp until the first send's ack arrives, so the second output's wire clock
 * claims the first output's exact coordinate — and consumer app C, gating on both topics, can
 * therefore never deliver the second output before the first, however the two arrive.
 */
@Testcontainers(disabledWithoutDocker = true)
class ParsleyDiamondCrossingWaitIT {

    @Container
    private final KafkaContainer kafka =
            new KafkaContainer(ParsleyBrokerImage.get());

    private static final String C1 = "c1";
    private static final String OUT_A = "diamond-a";
    private static final String OUT_B = "diamond-b";
    private static final String OBSERVED = "observed";

    /**
     * The second forward of one invocation (to {@code diamond-b}) must be stamped after the first
     * forward's (to {@code diamond-a}) producer ack — its wire clock claims the first output's
     * coordinate — and the consumer delivers first-then-second.
     *
     * Asserts the b-record's wire clock names the a-record's exact offset on {@code diamond-a},
     * and the observing app's output shows a before b.
     */
    @Test
    void secondOutputOfOneInvocationClaimsAndFollowsTheFirst() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        createTopics(bootstrap, C1, OUT_A, OUT_B, OBSERVED);
        Uuid outAId = topicId(bootstrap, OUT_A);

        CausalTopology diamondTopology = new CausalStreamsBuilder()
                .stream(List.of(C1), Serdes.String(), Serdes.String())
                .process(fanOutProcessor())
                .to("a-sink", OUT_A, Serdes.String(), Serdes.String())
                .to("b-sink", OUT_B, Serdes.String(), Serdes.String())
                .build();
        CausalTopology observerTopology = new CausalStreamsBuilder()
                .stream(List.of(C1, OUT_A, OUT_B), Serdes.String(), Serdes.String())
                .process(taggingProcessor())
                .to(OBSERVED, Serdes.String(), Serdes.String())
                .build();

        try (CausalStreams diamond = new CausalStreams(diamondTopology, streamsConfig(bootstrap, "diamond-p"));
             CausalStreams observer = new CausalStreams(observerTopology, streamsConfig(bootstrap, "diamond-c"))) {
            diamond.start();
            observer.start();

            try (KafkaProducer<String, String> input = new KafkaProducer<>(producerConfig(bootstrap))) {
                input.send(CausalClock.empty().stamp(new ProducerRecord<>(C1, "k", "hello"))).get();
            }

            // Wire evidence of the crossing wait: b's stamp claims a's exact committed coordinate,
            // which is only knowable through a's producer ack.
            List<ConsumerRecord<String, String>> aRecords = new ArrayList<>();
            List<ConsumerRecord<String, String>> bRecords = new ArrayList<>();
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig(bootstrap))) {
                consumer.subscribe(List.of(OUT_A, OUT_B));
                await().atMost(Duration.ofSeconds(90)).until(() -> {
                    consumer.poll(Duration.ofMillis(500)).forEach(record -> {
                        if (isBusinessRecord(record)) {
                            (record.topic().equals(OUT_A) ? aRecords : bRecords).add(record);
                        }
                    });
                    return !aRecords.isEmpty() && !bRecords.isEmpty();
                });
            }
            ConsumerRecord<String, String> aRecord = aRecords.get(0);
            ConsumerRecord<String, String> bRecord = bRecords.get(0);
            assertEquals("a:hello", aRecord.value(), "the first forward must land on diamond-a");
            assertEquals("b:hello", bRecord.value(), "the second forward must land on diamond-b");
            assertEquals(aRecord.offset(), wireClock(bRecord).offsetFor(outAId, 0),
                    "the second output's stamp must claim the first output's exact coordinate — "
                            + "knowable only through the first send's ack (the crossing wait)");

            // Delivery evidence: C gates b on a's coordinate, so a delivers first.
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig(bootstrap))) {
                consumer.subscribe(List.of(OBSERVED));
                List<String> observed = new ArrayList<>();
                await().atMost(Duration.ofSeconds(90)).until(() -> {
                    consumer.poll(Duration.ofMillis(500)).forEach(record -> {
                        if (isBusinessRecord(record)) {
                            observed.add(record.value());
                        }
                    });
                    return observed.contains(OUT_A + ":a:hello") && observed.contains(OUT_B + ":b:hello");
                });
                assertTrue(observed.indexOf(OUT_A + ":a:hello") < observed.indexOf(OUT_B + ":b:hello"),
                        "the consumer must deliver the first output before the second; observed: " + observed);
            }
        }
    }

    /** App P's delegate: each input forwards {@code a:<value>} to a-sink THEN {@code b:<value>} to b-sink. */
    private static ProcessorSupplier<String, String, String, String> fanOutProcessor() {
        return () -> new Processor<>() {
            private ProcessorContext<String, String> ctx;

            @Override
            public void init(ProcessorContext<String, String> context) {
                this.ctx = context;
            }

            @Override
            public void process(Record<String, String> record) {
                ctx.forward(record.withValue("a:" + record.value()), "a-sink");
                ctx.forward(record.withValue("b:" + record.value()), "b-sink");
            }
        };
    }

    /** App C's delegate: forwards every delivery tagged {@code <sourceTopic>:<value>}. */
    private static ProcessorSupplier<String, String, String, String> taggingProcessor() {
        return () -> new Processor<>() {
            private ProcessorContext<String, String> ctx;

            @Override
            public void init(ProcessorContext<String, String> context) {
                this.ctx = context;
            }

            @Override
            public void process(Record<String, String> record) {
                String source = ctx.recordMetadata().orElseThrow().topic();
                ctx.forward(record.withValue(source + ":" + record.value()));
            }
        };
    }

    /** A record with a value and no Parsley marker header — a business record, not a null message. */
    private static boolean isBusinessRecord(ConsumerRecord<String, String> record) {
        return record.value() != null && record.headers().lastHeader(ParsleyHeader.NULL_MESSAGE) == null;
    }

    private static ParsleyVectorClock wireClock(ConsumerRecord<String, String> record) {
        Header header = record.headers().lastHeader(ParsleyHeader.CAUSAL_CLOCK);
        assertNotNull(header, "every stamped business record must carry the causal-dependencies header");
        return ParsleyVectorClock.fromBytes(header.value());
    }

    private static Uuid topicId(String bootstrap, String topic) throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap))) {
            return requireNonNull(admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic)).topicId();
        }
    }

    private static Properties streamsConfig(String bootstrap, String appPrefix) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appPrefix + "-" + UUID.randomUUID());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 200);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        return props;
    }

    private static Map<String, Object> producerConfig(String bootstrap) {
        return Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    }

    private static Map<String, Object> consumerConfig(String bootstrap) {
        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ConsumerConfig.GROUP_ID_CONFIG, "observer-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    }

    private static void createTopics(String bootstrap, String... topics) throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap))) {
            Set<NewTopic> newTopics = new HashSet<>();
            for (String topic : topics) {
                newTopics.add(new NewTopic(topic, 1, (short) 1));
            }
            admin.createTopics(newTopics).all().get();
        }
    }
}
