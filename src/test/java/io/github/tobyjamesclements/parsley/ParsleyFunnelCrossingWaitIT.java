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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static java.util.Objects.requireNonNull;

/**
 * The funnel against a real broker: one invocation of the delegate sends to
 * TWO partitions of ONE sink topic in one EOS transaction. Process-order edges cross partitions of
 * a single sink too, and Kafka's FIFO covers only same-partition sends — so the crossing wait must
 * hold the second forward's stamp until the first send's ack arrives, at (topic, partition)
 * granularity (a per-topic wait would deterministically under-claim exactly this case). The wire
 * evidence: the second output's clock claims the first output's exact coordinate on the other
 * partition.
 *
 * <p>The downstream half of the funnel scenario — a Parsley consumer of the funnel sink gating the
 * second output's descendants on the first output — is proven by
 * {@link ParsleyFunnelDeliveryOrderIT}: the cross-partition claim this test puts on the wire is
 * ignored and carried at a task that owns only the other partition, and genuinely gated
 * where both partitions' derivatives converge.
 */
@Testcontainers(disabledWithoutDocker = true)
class ParsleyFunnelCrossingWaitIT {

    @Container
    private final KafkaContainer kafka =
            new KafkaContainer(ParsleyBrokerImage.get());

    private static final String C1 = "c1";
    private static final String FUNNEL = "funnel";

    /**
     * The second forward of one invocation, landing on partition 1 of the funnel sink, must be
     * stamped after the first forward's ack on partition 0 — its wire clock claims the first
     * output's exact (topic, partition, offset) — while the first output's stamp claims nothing on
     * partition 1 (a stamp can never claim a send that had not been acked when it was taken).
     *
     * Asserts the partition-1 record's clock names the partition-0 record's exact offset, and the
     * partition-0 record's clock carries no partition-1 entry.
     */
    @Test
    void secondSendToASecondPartitionOfOneSinkClaimsTheFirstSendsCoordinate() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        createTopics(bootstrap, Map.of(C1, 1, FUNNEL, 2));
        Uuid funnelId = topicId(bootstrap, FUNNEL);
        String keyForP0 = keyForPartition(0);
        String keyForP1 = keyForPartition(1);

        CausalTopology topology = new CausalStreamsBuilder()
                .stream(List.of(C1), Serdes.String(), Serdes.String())
                .process(funnelProcessor(keyForP0, keyForP1))
                .to(FUNNEL, Serdes.String(), Serdes.String())
                .build();

        try (CausalStreams streams = new CausalStreams(topology, streamsConfig(bootstrap))) {
            streams.start();

            try (KafkaProducer<String, String> input = new KafkaProducer<>(producerConfig(bootstrap))) {
                input.send(CausalClock.empty().stamp(new ProducerRecord<>(C1, "k", "hello"))).get();
            }

            List<ConsumerRecord<String, String>> p0Records = new ArrayList<>();
            List<ConsumerRecord<String, String>> p1Records = new ArrayList<>();
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig(bootstrap))) {
                consumer.subscribe(List.of(FUNNEL));
                await().atMost(Duration.ofSeconds(90)).until(() -> {
                    consumer.poll(Duration.ofMillis(500)).forEach(record -> {
                        if (isBusinessRecord(record)) {
                            (record.partition() == 0 ? p0Records : p1Records).add(record);
                        }
                    });
                    return !p0Records.isEmpty() && !p1Records.isEmpty();
                });
            }

            ConsumerRecord<String, String> first = p0Records.get(0);
            ConsumerRecord<String, String> second = p1Records.get(0);
            assertEquals("first:hello", first.value(), "the first forward must land on partition 0");
            assertEquals("second:hello", second.value(), "the second forward must land on partition 1");
            assertEquals(first.offset(), wireClock(second).offsetFor(funnelId, 0),
                    "the second send's stamp must claim the first send's exact coordinate on the "
                            + "OTHER partition of the same sink — knowable only through the first "
                            + "send's ack (the per-partition crossing wait)");
            assertEquals(-1L, wireClock(first).offsetFor(funnelId, 1),
                    "the first send's stamp must claim nothing on partition 1 — its sibling had "
                            + "not been sent, let alone acked, when this stamp was taken");
        }
    }

    /**
     * The funnel delegate: each C1 record forwards {@code first:<value>} keyed onto partition 0,
     * then {@code second:<value>} keyed onto partition 1 of the single funnel sink — one
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

    private static Properties streamsConfig(String bootstrap) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "funnel-" + UUID.randomUUID());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 200);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        // The funnel fans a 1-partition source into a 2-partition sink; a wider sink passes the
        // sink-width startup check with no opt-down needed.
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
                ConsumerConfig.GROUP_ID_CONFIG, "funnel-observer-" + UUID.randomUUID(),
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
