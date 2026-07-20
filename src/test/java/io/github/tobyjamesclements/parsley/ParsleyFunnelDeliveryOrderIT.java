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
import org.apache.kafka.common.utils.Utils;
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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The delivery-order half of the T3.0 A7 funnel, deferred from T2.3 to T3.1: one invocation of
 * app A's delegate sends to TWO partitions of one sink in one EOS transaction, and the crossing
 * wait puts the first send's cross-partition coordinate into the second send's clock (the wire
 * half, already proven by {@link ParsleyFunnelCrossingWaitIT}). This test runs the half the
 * interim I7 fail-fast made impossible: app B's task that owns only partition 1 receives the
 * second output's cross-partition claim — under the retired fail-fast that was an unreachable
 * coordinate and a crash; under the two-branch gate it is ignored (D1) and carried (I9) — and
 * app C, consuming both the funnel topic and B's re-keyed derivative on the SAME task, gates the
 * second output's descendant on the first output through its consumed branch.
 *
 * <p>Topology (funnel and derived topics all two-partition): {@code t1} → app A →
 * {@code funnel[p0,p1]}; app B re-keys every funnel record onto partition 0 of {@code rekeyed};
 * app C consumes {@code funnel} and {@code rekeyed} and tags every delivery to {@code observed}.
 */
@Testcontainers(disabledWithoutDocker = true)
class ParsleyFunnelDeliveryOrderIT {

    @Container
    private final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"));

    private static final String T1 = "t1";
    private static final String FUNNEL = "funnel";
    private static final String REKEYED = "rekeyed";
    private static final String OBSERVED = "observed";

    /**
     * The descendant B derives from the funnel's second (partition-1) output must carry the first
     * (partition-0) output's coordinate — custody through B's partition-1 task, which itself can
     * only IGNORE that cross-partition claim (it owns partition 1 alone; the retired I7 fail-fast
     * crashed exactly here) — and C's partition-0 task, which CONSUMES the funnel's partition 0,
     * must deliver the first output before that descendant.
     *
     * Asserts the descendant's wire clock names the first output's exact coordinate, and C's
     * observed partition-0 output shows the first funnel output strictly before the second
     * output's descendant.
     */
    @Test
    void secondOutputsDescendantGatesOnTheFirstOutputAcrossTheFunnel() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        createTopics(bootstrap, Map.of(T1, 1, FUNNEL, 2, REKEYED, 2, OBSERVED, 2));
        Uuid funnelId = topicId(bootstrap, FUNNEL);
        String keyForP0 = keyForPartition(0);
        String keyForP1 = keyForPartition(1);

        CausalTopology funnelTopology = new CausalStreamsBuilder()
                .stream(List.of(T1), Serdes.String(), Serdes.String())
                .process(funnelProcessor(keyForP0, keyForP1))
                .to(FUNNEL, Serdes.String(), Serdes.String())
                .build();
        CausalTopology rekeyTopology = new CausalStreamsBuilder()
                .stream(List.of(FUNNEL), Serdes.String(), Serdes.String())
                .process(rekeyingProcessor(keyForP0))
                .to(REKEYED, Serdes.String(), Serdes.String())
                .build();
        CausalTopology observerTopology = new CausalStreamsBuilder()
                .stream(List.of(FUNNEL, REKEYED), Serdes.String(), Serdes.String())
                .process(taggingProcessor())
                .to(OBSERVED, Serdes.String(), Serdes.String())
                .build();

        try (CausalStreams funnel = new CausalStreams(funnelTopology, streamsConfig(bootstrap, "funnel-a"));
             CausalStreams rekeyer = new CausalStreams(rekeyTopology, streamsConfig(bootstrap, "funnel-b"));
             CausalStreams observer = new CausalStreams(observerTopology, streamsConfig(bootstrap, "funnel-c"))) {
            funnel.start();
            rekeyer.start();
            observer.start();

            try (KafkaProducer<String, String> input = new KafkaProducer<>(producerConfig(bootstrap))) {
                input.send(CausalDependencies.empty().stamp(new ProducerRecord<>(T1, "k", "hello"))).get();
            }

            // The custody evidence: the descendant of the SECOND output (derived by B's
            // partition-1 task, which could only ignore the cross-partition claim) still carries
            // the FIRST output's exact partition-0 coordinate (I9 — the merge may not strip).
            List<ConsumerRecord<String, String>> firstOutputs = new ArrayList<>();
            List<ConsumerRecord<String, String>> secondDescendants = new ArrayList<>();
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig(bootstrap))) {
                consumer.subscribe(List.of(FUNNEL, REKEYED));
                await().atMost(Duration.ofSeconds(90)).until(() -> {
                    consumer.poll(Duration.ofMillis(500)).forEach(record -> {
                        if (!isBusinessRecord(record)) {
                            return;
                        }
                        if (record.topic().equals(FUNNEL) && record.value().startsWith("first:")) {
                            firstOutputs.add(record);
                        }
                        if (record.topic().equals(REKEYED) && record.value().equals("d:second:hello")) {
                            secondDescendants.add(record);
                        }
                    });
                    return !firstOutputs.isEmpty() && !secondDescendants.isEmpty();
                });
            }
            assertTrue(wireClock(secondDescendants.get(0)).offsetFor(funnelId, 0) >= firstOutputs.get(0).offset(),
                    "the second output's descendant must claim the first output's partition-0 "
                            + "coordinate — carried through B's partition-1 task, which ignores "
                            + "but never strips it (D1 + I9)");

            // The delivery evidence: C's partition-0 task consumes both the funnel's partition 0
            // and the re-keyed partition 0, so the descendant's claim is genuinely gated there —
            // first output before second output's descendant, however the two arrive.
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig(bootstrap))) {
                consumer.subscribe(List.of(OBSERVED));
                List<String> observedP0 = new ArrayList<>();
                await().atMost(Duration.ofSeconds(90)).until(() -> {
                    consumer.poll(Duration.ofMillis(500)).forEach(record -> {
                        if (isBusinessRecord(record) && record.partition() == 0) {
                            observedP0.add(record.value());
                        }
                    });
                    return observedP0.contains(FUNNEL + ":first:hello")
                            && observedP0.contains(REKEYED + ":d:second:hello");
                });
                assertTrue(observedP0.indexOf(FUNNEL + ":first:hello")
                                < observedP0.indexOf(REKEYED + ":d:second:hello"),
                        "C must deliver the funnel's first output before the second output's "
                                + "descendant — the A7 delivery-order guarantee; observed p0: " + observedP0);
            }
        }
    }

    /**
     * App A's funnel delegate: each T1 record forwards {@code first:<value>} keyed onto partition
     * 0, then {@code second:<value>} keyed onto partition 1 of the single funnel sink — one
     * invocation, one transaction, two partitions.
     */
    private static ProcessorSupplier<String, String, String, String> funnelProcessor(
            String keyForP0, String keyForP1) {
        return () -> new Processor<>() {
            private ProcessorContext<String, String> ctx;

            @Override
            public void init(ProcessorContext<String, String> context) {
                this.ctx = context;
            }

            @Override
            public void process(Record<String, String> record) {
                ctx.forward(new Record<>(keyForP0, "first:" + record.value(), record.timestamp()));
                ctx.forward(new Record<>(keyForP1, "second:" + record.value(), record.timestamp()));
            }
        };
    }

    /**
     * App B's delegate: re-keys every funnel record onto partition 0 of the rekeyed sink as
     * {@code d:<value>} — so the second output's descendant lands in the same downstream task
     * partition as the first output.
     */
    private static ProcessorSupplier<String, String, String, String> rekeyingProcessor(String keyForP0) {
        return () -> new Processor<>() {
            private ProcessorContext<String, String> ctx;

            @Override
            public void init(ProcessorContext<String, String> context) {
                this.ctx = context;
            }

            @Override
            public void process(Record<String, String> record) {
                ctx.forward(new Record<>(keyForP0, "d:" + record.value(), record.timestamp()));
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

    /**
     * A key Kafka's default (murmur2) partitioner routes to {@code partition} of a two-partition
     * topic — searched deterministically so the test never depends on a lucky constant.
     */
    private static String keyForPartition(int partition) {
        for (int i = 0; i < 100; i++) {
            String candidate = "key-" + i;
            int routed = Utils.toPositive(Utils.murmur2(candidate.getBytes(StandardCharsets.UTF_8))) % 2;
            if (routed == partition) {
                return candidate;
            }
        }
        throw new AssertionError("no key found for partition " + partition + " in 100 candidates");
    }

    /** A record with a value and no Parsley marker header — a business record, not a null message. */
    private static boolean isBusinessRecord(ConsumerRecord<String, String> record) {
        return record.value() != null && record.headers().lastHeader(ParsleyHeader.WATERMARK) == null;
    }

    private static ParsleyVectorClock wireClock(ConsumerRecord<String, String> record) {
        Header header = record.headers().lastHeader(ParsleyHeader.CAUSAL_DEPENDENCIES);
        assertNotNull(header, "every stamped business record must carry the causal-dependencies header");
        return ParsleyVectorClock.fromBytes(header.value());
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

    private static void createTopics(String bootstrap, Map<String, Integer> topicPartitions) throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap))) {
            Set<NewTopic> newTopics = new HashSet<>();
            topicPartitions.forEach((topic, partitions) ->
                    newTopics.add(new NewTopic(topic, partitions, (short) 1)));
            admin.createTopics(newTopics).all().get();
        }
    }
}
