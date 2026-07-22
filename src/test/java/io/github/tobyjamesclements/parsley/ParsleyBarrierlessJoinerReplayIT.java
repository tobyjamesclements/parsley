package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A joiner needs zero coordination (D7, T3.2 IT a): a fresh app joins an established, still-running
 * topology by simply starting to consume — no barrier, no admission, no join wait — and its
 * multi-partition replay lands in causal delivery order. App A ({@code c1} → {@code c2},
 * key-preserving, both topics two partitions) builds derived history while running; joiner Z then
 * starts consuming {@code c1} and {@code c2} from log start. Each of Z's two tasks replays its
 * partition pair in arbitrary cross-topic arrival order; the hold-back queue must convert that into
 * causal order — every derived {@code c2} record delivered after its {@code c1} cause — because the
 * {@code c2} records' truthful (unfloored) stamps claim their {@code c1} coordinates directly.
 */
@Testcontainers(disabledWithoutDocker = true)
class ParsleyBarrierlessJoinerReplayIT {

    @Container
    private final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"));

    private static final String C1 = "c1";
    private static final String C2 = "c2";
    private static final String OBSERVED = "observed";
    private static final int PARTITIONS = 2;
    private static final int RECORDS = 6;

    /**
     * Z joins while A is still running, with history already on both partitions of both topics, and
     * with no coordination configured anywhere. Z must start delivering without any barrier and,
     * for every key on every partition, deliver the {@code c1} cause strictly before the derived
     * {@code c2} effect, despite replaying both topics concurrently from log start.
     *
     * Asserts Z observes both records for every key, cause before effect per key.
     */
    @Test
    void joinerReplayAcrossPartitionsLandsInCausalOrderWithNoBarrier() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        createTopics(bootstrap, PARTITIONS, C1, C2, OBSERVED);

        CausalTopology aTopology = new CausalStreamsBuilder()
                .stream(List.of(C1), Serdes.String(), Serdes.String())
                .process(prefixingProcessor("a:"))
                .to(C2, Serdes.String(), Serdes.String())
                .build();
        CausalTopology joinerTopology = new CausalStreamsBuilder()
                .stream(List.of(C1, C2), Serdes.String(), Serdes.String())
                .process(taggingProcessor())
                .to(OBSERVED, Serdes.String(), Serdes.String())
                .build();

        try (CausalStreams a = new CausalStreams(aTopology, streamsConfig(bootstrap, "replay-a"))) {
            a.start();

            // Build derived history on both partitions: same key on c1 and c2 → same partition
            // (key-preserving stage, equal partition counts, default partitioner).
            try (KafkaProducer<String, String> input = new KafkaProducer<>(producerConfig(bootstrap))) {
                for (int i = 0; i < RECORDS; i++) {
                    input.send(CausalClock.empty()
                            .stamp(new ProducerRecord<>(C1, "k" + i, "v" + i))).get();
                }
            }
            awaitDerivedHistory(bootstrap);

            // The join: no coordination config, no barrier — Z just starts consuming.
            try (CausalStreams joiner = new CausalStreams(joinerTopology, streamsConfig(bootstrap, "replay-z"))) {
                joiner.start();

                try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig(bootstrap))) {
                    consumer.subscribe(List.of(OBSERVED));
                    List<String> observed = new ArrayList<>();
                    await().atMost(Duration.ofSeconds(120)).until(() -> {
                        consumer.poll(Duration.ofMillis(500)).forEach(record -> {
                            if (isBusinessRecord(record)) {
                                observed.add(record.value());
                            }
                        });
                        return observed.size() >= 2 * RECORDS;
                    });
                    for (int i = 0; i < RECORDS; i++) {
                        String cause = C1 + ":v" + i;
                        String effect = C2 + ":a:v" + i;
                        assertTrue(observed.contains(cause),
                                "Z must deliver the replayed c1 cause for k" + i + "; observed: " + observed);
                        assertTrue(observed.contains(effect),
                                "Z must deliver the replayed derived c2 effect for k" + i + "; observed: " + observed);
                        assertTrue(observed.indexOf(cause) < observed.indexOf(effect),
                                "the joiner's replay must deliver the c1 cause before its derived c2 "
                                        + "effect for k" + i + " — the hold-back queue converts arbitrary "
                                        + "cross-topic arrival into causal order; observed: " + observed);
                    }
                }
            }
        }
    }

    /** Waits until every derived record has been committed to {@code c2}, so the joiner faces real history. */
    private void awaitDerivedHistory(String bootstrap) {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig(bootstrap))) {
            consumer.subscribe(List.of(C2));
            Set<String> derived = new HashSet<>();
            await().atMost(Duration.ofSeconds(90)).until(() -> {
                consumer.poll(Duration.ofMillis(500)).forEach(record -> {
                    if (isBusinessRecord(record)) {
                        derived.add(record.value());
                    }
                });
                return derived.size() >= RECORDS;
            });
        }
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

    /** Z's delegate: forwards every delivery tagged {@code <sourceTopic>:<value>}. */
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
    private static boolean isBusinessRecord(org.apache.kafka.clients.consumer.ConsumerRecord<String, String> record) {
        return record.value() != null && record.headers().lastHeader(ParsleyHeader.NULL_MESSAGE) == null;
    }

    private static Properties streamsConfig(String bootstrap, String appPrefix) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appPrefix + "-" + UUID.randomUUID());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 200);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, PARTITIONS);
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

    private static void createTopics(String bootstrap, int partitions, String... topics) throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap))) {
            Set<NewTopic> newTopics = new HashSet<>();
            for (String topic : topics) {
                newTopics.add(new NewTopic(topic, partitions, (short) 1));
            }
            admin.createTopics(newTopics).all().get();
        }
    }
}
