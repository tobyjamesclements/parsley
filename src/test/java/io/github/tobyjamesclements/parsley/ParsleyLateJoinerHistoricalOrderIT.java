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
 * The historical-pairs ordering hole floors used to open, now closed by truthful stamps (D4, T3.2
 * IT b): a late joiner consuming both an input topic and its derived topic replays fully-written
 * history and must deliver every (cause, effect) pair in causal order. Under the retired epoch
 * floors, a coordinated joiner's replay-era view repositioned historical stamps at the epoch origin
 * — erasing the derived records' {@code c1} ancestry, so nothing ordered a historical {@code c2}
 * record after its {@code c1} cause. With truthful stamps the derived record's clock claims its
 * exact {@code c1} coordinate, and the joiner's own gate orders the pair — no floors, no
 * coordination, no barrier.
 *
 * <p>App A ({@code c1} → {@code c2}) processes a batch and shuts down; only then does joiner Z
 * (consuming {@code c1} and {@code c2}) start, against complete, closed history.
 */
@Testcontainers(disabledWithoutDocker = true)
class ParsleyLateJoinerHistoricalOrderIT {

    @Container
    private final KafkaContainer kafka =
            new KafkaContainer(ParsleyBrokerImage.get());

    private static final String C1 = "c1";
    private static final String C2 = "c2";
    private static final String OBSERVED = "observed";
    private static final int RECORDS = 8;

    /**
     * Z starts after the whole {@code c1} → {@code c2} history is written and A is gone, replays
     * both topics from log start, and must deliver every historical pair cause-first: each derived
     * {@code c2} record's stamp names its exact {@code c1} cause, so Z's gate holds it until that
     * cause has been locally delivered — the ordering floored stamps could not express.
     *
     * Asserts Z observes both halves of every historical pair, cause strictly before effect.
     */
    @Test
    void lateJoinerDeliversHistoricalPairsInCausalOrder() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        createTopics(bootstrap, C1, C2, OBSERVED);

        CausalTopology aTopology = new CausalStreamsBuilder()
                .stream(List.of(C1), Serdes.String(), Serdes.String())
                .process(prefixingProcessor("a:"))
                .to(C2, Serdes.String(), Serdes.String())
                .build();

        // Phase 1: write the history, then shut A down — the joiner faces closed, purely
        // historical topics.
        try (CausalStreams a = new CausalStreams(aTopology, streamsConfig(bootstrap, "history-a"))) {
            a.start();
            try (KafkaProducer<String, String> input = new KafkaProducer<>(producerConfig(bootstrap))) {
                for (int i = 0; i < RECORDS; i++) {
                    input.send(CausalClock.empty()
                            .stamp(new ProducerRecord<>(C1, "k", "v" + i))).get();
                }
            }
            awaitDerivedHistory(bootstrap);
        }

        // Phase 2: the late joiner replays input and derived topic together.
        CausalTopology joinerTopology = new CausalStreamsBuilder()
                .stream(List.of(C1, C2), Serdes.String(), Serdes.String())
                .process(taggingProcessor())
                .to(OBSERVED, Serdes.String(), Serdes.String())
                .build();
        try (CausalStreams joiner = new CausalStreams(joinerTopology, streamsConfig(bootstrap, "history-z"))) {
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
                            "the joiner must deliver the historical c1 cause v" + i + "; observed: " + observed);
                    assertTrue(observed.contains(effect),
                            "the joiner must deliver the historical derived c2 effect for v" + i
                                    + "; observed: " + observed);
                    assertTrue(observed.indexOf(cause) < observed.indexOf(effect),
                            "the historical pair for v" + i + " must deliver cause-first — the derived "
                                    + "record's truthful stamp names its c1 cause, closing the ordering "
                                    + "hole floored stamps opened; observed: " + observed);
                }
            }
        }
    }

    /** Waits until every derived record has been committed to {@code c2}, so history is complete. */
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
