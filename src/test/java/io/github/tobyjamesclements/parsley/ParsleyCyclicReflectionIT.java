package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

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

/**
 * A genuine two-app topology cycle under the two-branch gate, with zero coordination (T3.1 IT c):
 * app A consumes {@code c1} and {@code tb} and produces {@code ta}; app B consumes {@code ta} and
 * produces {@code tb}. Each app's own produced coordinates reflect back at it around the cycle in
 * the other app's stamps — {@code ta} claims arrive at A (its own unconsumed sink → the ignore
 * branch, where the retired gate-side strip used to sit and the retired I7 fail-fast would
 * otherwise fire), {@code tb} claims arrive at B likewise — while the carried root ancestry
 * ({@code c1}) is genuinely gated at A's consumed branch. Before D7 this shape needed the
 * coordination subsystem's {@code domain-topics} passthrough to avoid the fail-fast; here both
 * apps run with no coordination configuration at all, and the cycle both delivers and quiesces.
 */
@Testcontainers(disabledWithoutDocker = true)
class ParsleyCyclicReflectionIT {

    @Container
    private final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"));

    private static final String C1 = "c1";
    private static final String TA = "ta";
    private static final String TB = "tb";
    private static final String OBSERVED = "observed";

    /**
     * One {@code c1} record traverses the cycle — A derives onto {@code ta}, B derives onto
     * {@code tb}, and the {@code tb} record (whose clock reflects A's own {@code ta} coordinate
     * and claims the {@code c1} root) is delivered back at A through the ignore/consumed
     * dispatch. The cycle must then quiesce: after the input stops, both cycle topics' end
     * offsets hold still and both apps are still RUNNING.
     *
     * Asserts the reflected {@code tb} record's wire clock names A's own {@code ta} coordinate
     * and the {@code c1} root, A's delegate delivers the c1 cause before the looped-back tb
     * record, the cycle topics stop growing, and both Streams instances stay RUNNING.
     */
    @Test
    void twoAppCycleDeliversReflectedClaimsAndQuiescesWithoutCoordination() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        createTopics(bootstrap, C1, TA, TB, OBSERVED);
        Uuid c1Id = topicId(bootstrap, C1);
        Uuid taId = topicId(bootstrap, TA);

        CausalTopology aTopology = new CausalStreamsBuilder()
                .stream(List.of(C1, TB), Serdes.String(), Serdes.String())
                .process(appAProcessor())
                .to("ta-sink", TA, Serdes.String(), Serdes.String())
                .to("observed-sink", OBSERVED, Serdes.String(), Serdes.String())
                .build();
        CausalTopology bTopology = new CausalStreamsBuilder()
                .stream(List.of(TA), Serdes.String(), Serdes.String())
                .process(prefixingProcessor("b:"))
                .to(TB, Serdes.String(), Serdes.String())
                .build();

        try (CausalStreams a = new CausalStreams(aTopology, streamsConfig(bootstrap, "cycle-a"));
             CausalStreams b = new CausalStreams(bTopology, streamsConfig(bootstrap, "cycle-b"))) {
            a.start();
            b.start();

            try (KafkaProducer<String, String> input = new KafkaProducer<>(producerConfig(bootstrap))) {
                input.send(CausalClock.empty().stamp(new ProducerRecord<>(C1, "k", "hello"))).get();
            }

            // The record that traversed the cycle: B's tb output reflects A's own ta coordinate
            // back at A (the ignore branch's input) and claims the c1 root (the consumed branch's
            // input) — both in one clock.
            List<ConsumerRecord<String, String>> tbRecords = new ArrayList<>();
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig(bootstrap))) {
                consumer.subscribe(List.of(TB));
                await().atMost(Duration.ofSeconds(90)).until(() -> {
                    consumer.poll(Duration.ofMillis(500)).forEach(record -> {
                        if (isBusinessRecord(record)) {
                            tbRecords.add(record);
                        }
                    });
                    return !tbRecords.isEmpty();
                });
            }
            ParsleyVectorClock tbClock = wireClock(tbRecords.get(0));
            assertEquals("b:a:hello", tbRecords.get(0).value(), "the cycle must carry the derivation chain");
            assertTrue(tbClock.offsetFor(taId, 0) >= 0,
                    "the looped-back record must reflect A's own ta coordinate — the claim the "
                            + "ignore branch (not a strip, not a fail-fast) handles at A");
            assertTrue(tbClock.offsetFor(c1Id, 0) >= 0,
                    "the looped-back record must claim the c1 root (I2/I9 custody around the "
                            + "cycle) — the claim A's consumed branch genuinely gates");

            // The delivery evidence: A's delegate genuinely delivers the looped-back tb record —
            // its reflected ta claim lands in the ignore branch, its c1 claim in the consumed
            // branch — after the c1 cause. Under the retired I7 fail-fast this delivery crashed
            // the task instead.
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig(bootstrap))) {
                consumer.subscribe(List.of(OBSERVED));
                List<String> observed = new ArrayList<>();
                await().atMost(Duration.ofSeconds(90)).until(() -> {
                    consumer.poll(Duration.ofMillis(500)).forEach(record -> {
                        if (isBusinessRecord(record)) {
                            observed.add(record.value());
                        }
                    });
                    return observed.contains(C1 + ":hello") && observed.contains(TB + ":b:a:hello");
                });
                assertTrue(observed.indexOf(C1 + ":hello") < observed.indexOf(TB + ":b:a:hello"),
                        "A must deliver the c1 cause before the looped-back derivative; observed: " + observed);
            }

            // Quiescence: after the input stops, the cycle's topics stop growing for a sustained
            // window — the I6 knowledge-based relay settles (a reflected own claim teaches
            // nothing) instead of ping-ponging null messages around the cycle forever.
            await().atMost(Duration.ofSeconds(120)).until(() -> {
                long taBefore = endOffset(bootstrap, TA, 0);
                long tbBefore = endOffset(bootstrap, TB, 0);
                Thread.sleep(5_000);
                return endOffset(bootstrap, TA, 0) == taBefore && endOffset(bootstrap, TB, 0) == tbBefore;
            });
            assertEquals(KafkaStreams.State.RUNNING, a.state(),
                    "app A must still be running — reflected claims are ignored or gated, never a crash");
            assertEquals(KafkaStreams.State.RUNNING, b.state(),
                    "app B must still be running — reflected claims are ignored or gated, never a crash");
        }
    }

    /**
     * App A's delegate: every delivery is tagged {@code <sourceTopic>:<value>} to the observed
     * sink; a {@code c1} record additionally derives {@code a:<value>} onto the {@code ta} sink,
     * while a {@code tb}-sourced delivery (the looped-back derivative) is not re-derived, so the
     * business loop terminates after one round trip.
     */
    private static ProcessorSupplier<String, String, String, String> appAProcessor() {
        return () -> new Processor<>() {
            private ProcessorContext<String, String> ctx;

            @Override
            public void init(ProcessorContext<String, String> context) {
                this.ctx = context;
            }

            @Override
            public void process(Record<String, String> record) {
                String source = ctx.recordMetadata().orElseThrow().topic();
                ctx.forward(record.withValue(source + ":" + record.value()), "observed-sink");
                if (C1.equals(source)) {
                    ctx.forward(record.withValue("a:" + record.value()), "ta-sink");
                }
            }
        };
    }

    /** A delegate that forwards every delivery with {@code prefix} prepended to its value. */
    private static ProcessorSupplier<String, String, String, String> prefixingProcessor(String prefix) {
        return () -> new Processor<>() {
            private ProcessorContext<String, String> ctx;

            @Override
            public void init(ProcessorContext<String, String> context) {
                this.ctx = context;
            }

            @Override
            public void process(Record<String, String> record) {
                ctx.forward(record.withValue(prefix + record.value()));
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

    private static long endOffset(String bootstrap, String topic, int partition) throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap))) {
            return admin.listOffsets(Map.of(new TopicPartition(topic, partition), OffsetSpec.latest()))
                    .all().get().get(new TopicPartition(topic, partition)).offset();
        }
    }

    private static Uuid topicId(String bootstrap, String topic) throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap))) {
            return admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic).topicId();
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
