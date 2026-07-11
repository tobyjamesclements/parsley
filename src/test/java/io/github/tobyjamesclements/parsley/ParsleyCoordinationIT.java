package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
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

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end proof, against a real broker, that a running coordinated topology evolves through an epoch
 * transition driven by the public {@link ParsleyCoordination} API — exercising the real
 * {@link KafkaEpochTransport} (idempotent append, full-log fold, {@code caughtUp} against actual
 * end offsets) and the runtime thread that unit tests over the in-memory double cannot.
 */
@Testcontainers(disabledWithoutDocker = true)
class ParsleyCoordinationIT {

    @Container
    private final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"));

    private static final String IN = "t1";
    private static final String OUT = "out";
    private static final String EPOCH_EVENTS = "epoch-events";

    /**
     * A single running node, evolved twice via {@code requestEpochTransition()}: the first cut promotes
     * it to a running member (empty floor), the second — once it has delivered records — commits a
     * non-empty floor merged from its own completeness. Asserts the real epoch-events log carries the full
     * handshake ending in an {@code EpochCommitted} with a non-empty floor, and that the source-layer node
     * injects the resulting boundary marker to its sink.
     */
    @Test
    void runningTopologyEvolvesThroughAnEpochTransitionOverARealBroker() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        createTopics(bootstrap, IN, OUT, EPOCH_EVENTS);
        String appId = "coordination-it-" + UUID.randomUUID();
        Path stateDir = Files.createTempDirectory("parsley-coordination");

        try (CausalStreams causalStreams = new CausalStreams(topology(), streamsConfig(bootstrap, appId, stateDir))) {
            causalStreams.start();
            await().atMost(Duration.ofSeconds(30)).until(() -> causalStreams.state() == KafkaStreams.State.RUNNING);

            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig(bootstrap))) {
                // Deliver some records so the node's completeness (hence the eventual floor) is non-empty.
                for (int i = 0; i < 3; i++) {
                    producer.send(stampEmptyDeps(new ProducerRecord<>(IN, "k", "v" + i))).get();
                }
            }

            // The node joins on init; wait until it is on the log, then promote it to running (epoch 1),
            // then evolve to epoch 2 — now running and with delivered records, so the floor is non-empty.
            awaitEvent(bootstrap, e -> e instanceof ParsleyEpochEvent.JoinRequested);
            requestUntilCommitted(causalStreams, bootstrap, 1L);
            requestUntilCommitted(causalStreams, bootstrap, 2L);

            ParsleyEpochEvent.EpochCommitted epochTwo = awaitEvent(bootstrap,
                    e -> e instanceof ParsleyEpochEvent.EpochCommitted c && c.epochId() == 2L);
            assertFalse(epochTwo.lowerBounds().isEmpty(),
                    "epoch 2's committed floor is a real cut from the running node's completeness, not empty");

            assertTrue(awaitBoundaryMarkerAtSink(bootstrap),
                    "the source-layer node injects the epoch boundary marker to its sink");
        }
    }

    /**
     * Requests transitions until the log shows {@code EpochCommitted(targetEpoch)}. A single request may
     * coalesce into an already-open round; retrying is harmless (requests coalesce) and covers the race
     * between issuing the request and the running node publishing.
     */
    private static void requestUntilCommitted(CausalStreams causalStreams, String bootstrap, long targetEpoch) {
        await().atMost(Duration.ofSeconds(60)).until(() -> {
            if (readEpochEvents(bootstrap).stream()
                    .anyMatch(e -> e instanceof ParsleyEpochEvent.EpochCommitted c && c.epochId() >= targetEpoch)) {
                return true;
            }
            try {
                causalStreams.requestEpochTransition();
            } catch (IllegalStateException notReadyYet) {
                // No local member has joined/initialised yet; retry on the next tick.
            }
            return false;
        });
    }

    @SuppressWarnings("unchecked")
    private static <T extends ParsleyEpochEvent> T awaitEvent(String bootstrap, java.util.function.Predicate<ParsleyEpochEvent> match) {
        List<ParsleyEpochEvent> found = new ArrayList<>();
        await().atMost(Duration.ofSeconds(60)).until(() -> {
            for (ParsleyEpochEvent event : readEpochEvents(bootstrap)) {
                if (match.test(event)) {
                    found.add(event);
                    return true;
                }
            }
            return false;
        });
        return (T) found.get(found.size() - 1);
    }

    /** Reads the whole epoch-events log from the beginning and decodes every record. */
    private static List<ParsleyEpochEvent> readEpochEvents(String bootstrap) {
        List<ParsleyEpochEvent> events = new ArrayList<>();
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ConsumerConfig.GROUP_ID_CONFIG, "epoch-reader-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(List.of(EPOCH_EVENTS));
            long deadline = System.currentTimeMillis() + 2000;
            while (System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<byte[], byte[]> record : consumer.poll(Duration.ofMillis(200))) {
                    events.add(ParsleyEpochEvent.fromBytes(record.value()));
                }
            }
        }
        return events;
    }

    /** Polls the sink for a record carrying the {@link ParsleyHeader#EPOCH_BOUNDARY} marker header. */
    private static boolean awaitBoundaryMarkerAtSink(String bootstrap) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ConsumerConfig.GROUP_ID_CONFIG, "sink-reader-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(List.of(OUT));
            Optional<Boolean> seen = Optional.empty();
            long deadline = System.currentTimeMillis() + 30_000;
            while (System.currentTimeMillis() < deadline && seen.isEmpty()) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(300))) {
                    for (Header h : record.headers()) {
                        if (ParsleyHeader.EPOCH_BOUNDARY.equals(h.key()) && h.value() != null) {
                            seen = Optional.of(true);
                        }
                    }
                }
            }
            return seen.orElse(false);
        }
    }

    private static CausalTopology topology() {
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream(IN, Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("out-sink", OUT, Serdes.String(), Serdes.String());
        return builder.build();
    }

    private static ProcessorSupplier<String, String, String, String> upperCaser() {
        return () -> new Processor<>() {
            private ProcessorContext<String, String> ctx;

            @Override
            public void init(ProcessorContext<String, String> context) {
                this.ctx = context;
            }

            @Override
            public void process(Record<String, String> record) {
                ctx.forward(record.withValue(record.value().toUpperCase(Locale.ROOT)));
            }
        };
    }

    private static ProducerRecord<String, String> stampEmptyDeps(ProducerRecord<String, String> record) {
        record.headers().add(ParsleyHeader.CAUSAL_DEPENDENCIES, ParsleyClock.empty().toBytes());
        return record;
    }

    private static Properties streamsConfig(String bootstrap, String appId, Path stateDir) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 200);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toAbsolutePath().toString());
        props.put(ParsleyConfig.COORDINATION_EPOCH_EVENTS_TOPIC, EPOCH_EVENTS);
        return props;
    }

    private static Map<String, Object> producerConfig(String bootstrap) {
        return Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    }

    private static void createTopics(String bootstrap, String... topics) throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap))) {
            Set<NewTopic> newTopics = new HashSet<>();
            for (String topic : topics) {
                newTopics.add(new NewTopic(topic, 1, (short) 1)
                        // retention.ms=-1: the epoch-events log must never lose history (KafkaEpochTransport
                        // fails fast otherwise); harmless for the business topics created alongside.
                        .configs(Map.of(TopicConfig.RETENTION_MS_CONFIG, "-1")));
            }
            CreateTopicsResult result = admin.createTopics(newTopics);
            result.all().get();
        }
    }
}
