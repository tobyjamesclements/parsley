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
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Repro probe for the EOS offset-density concern: does a causal stage's contiguous frontier track a
 * <strong>transactionally-produced</strong> input across the commit markers that a {@code read_committed}
 * consumer skips? The input {@code src} is written as two committed transactions of three records, so the
 * broker log is {@code rec@0,1,2, MARKER@3, rec@4,5,6}; a {@code read_committed} consumer (which EOS sets)
 * sees offsets {@code 0,1,2,4,5,6} with a hole at 3 that no record ever fills.
 *
 * <p>The stage passes each record through, and {@link ParsleyProcessorContext} stamps every forward with
 * the node's completeness, whose {@code src} entry is the contiguous frontier. Reading the stamps back off
 * {@code out} therefore reveals how far the frontier advanced. If the contiguous walk
 * ({@code ParsleyFrontier.deliver}) stalls at the first marker hole, the highest {@code src} offset in any
 * stamp is capped at 2 even though all six records were delivered — confirming the frontier cannot track a
 * transactional topic past its first commit.
 */
@Testcontainers(disabledWithoutDocker = true)
class ParsleyEosFrontierDensityIT {

    @Container
    private final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"));

    private static final String SRC = "src";
    private static final String OUT = "out";

    /**
     * Produces {@code src} as two committed transactions (a commit marker between the batches) and asserts
     * the stage's frontier for {@code src}, read back from the output stamps, advances past the marker to
     * cover all six records. A stall at offset 2 confirms the EOS frontier-density bug.
     */
    @Test
    void frontierTracksATransactionallyProducedInputAcrossCommitMarkers() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        createTopics(bootstrap, SRC, OUT);
        Uuid srcId;
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap))) {
            srcId = admin.describeTopics(List.of(SRC)).allTopicNames().get().get(SRC).topicId();
        }

        // Two committed transactions of three records each: log = rec@0,1,2, MARKER@3, rec@4,5,6.
        Map<String, Object> txnConfig = new HashMap<>(producerConfig(bootstrap));
        txnConfig.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "src-tx-" + UUID.randomUUID());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(txnConfig)) {
            producer.initTransactions();
            for (int batch = 0; batch < 2; batch++) {
                producer.beginTransaction();
                for (int i = 0; i < 3; i++) {
                    ProducerRecord<String, String> record = new ProducerRecord<>(SRC, "k", "v" + batch + i);
                    record.headers().add(ParsleyHeader.CAUSAL_DEPENDENCIES, ParsleyClock.empty().toBytes());
                    producer.send(record).get();
                }
                producer.commitTransaction();
            }
        }

        try (KafkaStreams streams =
                     new KafkaStreams(topology(), streamsConfig(bootstrap, "eos-density-" + UUID.randomUUID()))) {
            streams.start();

            List<Long> srcOffsetsInStamps = new ArrayList<>();
            try (KafkaConsumer<String, String> out = new KafkaConsumer<>(consumerConfig(bootstrap))) {
                out.subscribe(List.of(OUT));
                await().atMost(Duration.ofSeconds(60)).until(() -> {
                    for (ConsumerRecord<String, String> record : out.poll(Duration.ofMillis(300))) {
                        Header stamp = record.headers().lastHeader(ParsleyHeader.CAUSAL_DEPENDENCIES);
                        if (stamp != null) {
                            srcOffsetsInStamps.add(ParsleyClock.fromBytes(stamp.value()).offsetFor(srcId, 0));
                        }
                    }
                    return srcOffsetsInStamps.size() >= 6;
                });
            }

            long highest = srcOffsetsInStamps.stream().mapToLong(Long::longValue).max().orElse(-1L);
            assertTrue(highest >= 5,
                    "the frontier for a transactionally-produced input must advance past the EOS commit "
                            + "marker (all six src records were delivered, last at broker offset 6); the "
                            + "highest src offset in any output stamp was " + highest
                            + " — a value of 2 confirms the contiguous walk stalls at the first marker hole");
        }
    }

    private static Topology topology() {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(SRC, Consumed.with(Serdes.String(), Serdes.String()))
                .process(ParsleyProcessorSupplier.builder(passthrough())
                        .addBufferStore("parsley")
                        .addSource(new ParsleySource<>(SRC, Serdes.String(), Serdes.String()))
                        .build())
                .to(OUT, Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }

    private static ProcessorSupplier<String, String, String, String> passthrough() {
        return () -> new Processor<>() {
            private ProcessorContext<String, String> ctx;

            @Override
            public void init(ProcessorContext<String, String> context) {
                this.ctx = context;
            }

            @Override
            public void process(Record<String, String> record) {
                ctx.forward(record);
            }
        };
    }

    private static Properties streamsConfig(String bootstrap, String appId) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 200);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        return props;
    }

    private static Map<String, Object> producerConfig(String bootstrap) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return config;
    }

    private static Map<String, Object> consumerConfig(String bootstrap) {
        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ConsumerConfig.GROUP_ID_CONFIG, "out-" + UUID.randomUUID(),
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
