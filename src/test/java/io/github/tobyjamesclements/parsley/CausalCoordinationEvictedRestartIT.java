package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
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

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression proof for the crashed-then-evicted restart path: an app crashes while holding a buffered
 * record, is evicted by a surviving app as the domain moves on, then restarts. It must <strong>not</strong>
 * crash-loop (the bug); it must block until re-admitted, re-adopt the current floor, and drain its buffer.
 */
@Testcontainers(disabledWithoutDocker = true)
class CausalCoordinationEvictedRestartIT {

    @Container
    private final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"));

    private static final String PREREQ = "prereq";   // X's held record depends on prereq@0
    private static final String IN = "in";           // X's business input
    private static final String OUT = "out";         // X's sink
    private static final String YIN = "yin";         // survivor Y's input
    private static final String YOUT = "yout";       // survivor Y's sink
    private static final String EPOCH_EVENTS = "epoch-events";

    /**
     * App X (prereq + in -> out) holds a record on {@code in} that depends on {@code prereq@0}; survivor Y
     * keeps the domain moving. X is hard-killed with the record still buffered; Y evicts X and advances the
     * epoch. X restarts: it blocks until re-admitted, then — once {@code prereq@0} is produced — drains the
     * held record to {@code out}. Asserts X recovers (does not crash-loop) and delivers its buffered record.
     */
    @Test
    void aCrashedThenEvictedAppRestartsWithoutLoopingAndDrainsItsBuffer() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        createTopics(bootstrap, PREREQ, IN, OUT, YIN, YOUT, EPOCH_EVENTS);
        String runId = UUID.randomUUID().toString().substring(0, 8);
        String appIdX = "evicted-x-" + runId;   // stable across the restart
        String appIdY = "evicted-y-" + runId;

        // Y evicts a silent member after 3s (short, for the test).
        CausalCoordination coordinationY =
                CausalCoordination.create(EPOCH_EVENTS, CausalCoordination.DEFAULT_JOIN_TIMEOUT, Duration.ofSeconds(3));
        CausalCoordination coordinationX1 = CausalCoordination.create(EPOCH_EVENTS);
        Path stateY = Files.createTempDirectory("parsley-evict-y");
        Path stateX1 = Files.createTempDirectory("parsley-evict-x1");

        KafkaStreams appY = new KafkaStreams(stageY(coordinationY), streamsConfig(bootstrap, appIdY, stateY));
        KafkaStreams appX = new KafkaStreams(stageX(coordinationX1), streamsConfig(bootstrap, appIdX, stateX1));
        CausalCoordination coordinationX2 = CausalCoordination.create(EPOCH_EVENTS);
        KafkaStreams appXRestarted = null;
        try {
            appY.start();
            appX.start();
            await().atMost(Duration.ofSeconds(60)).until(() ->
                    appY.state() == KafkaStreams.State.RUNNING && appX.state() == KafkaStreams.State.RUNNING);

            // Kick Y so it is active, and establish an epoch so both X and Y are running members.
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig(bootstrap))) {
                producer.send(CausalDependencies.empty().stamp(new ProducerRecord<>(YIN, "yk", "y"))).get();
            }
            requestUntilCommitted(coordinationY, coordinationX1, bootstrap, 1L);

            // Buffer a record in X: an `in` record that depends on prereq@0 (never yet produced) is held.
            CausalDependencies orderDeps;
            try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap))) {
                CausalTopics topics = CausalTopics.of(admin);
                orderDeps = CausalDependencies.using(topics)
                        .observe(new ConsumerRecord<>(PREREQ, 0, 0L, "pk", "prereq"));
            }
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig(bootstrap))) {
                producer.send(orderDeps.stamp(new ProducerRecord<>(IN, "k", "order"))).get();
            }

            // Hard-kill X with the record still buffered (no graceful leave).
            appX.close(Duration.ofSeconds(30));
            coordinationX1.close();

            // Y evicts the gone X and advances the epoch.
            long epochBeforeEvict = highestCommittedEpoch(bootstrap);
            await().atMost(Duration.ofSeconds(90)).pollInterval(Duration.ofSeconds(1)).until(() -> {
                requestQuietly(coordinationY);
                return highestCommittedEpoch(bootstrap) > epochBeforeEvict;
            });
            assertTrue(leaveExistsFor(bootstrap, appIdX), "the survivor must evict the gone X");

            // Restart X (same application id, so it restores its buffer changelog; fresh local state dir).
            Path stateX2 = Files.createTempDirectory("parsley-evict-x2");
            appXRestarted = new KafkaStreams(stageX(coordinationX2), streamsConfig(bootstrap, appIdX, stateX2));
            appXRestarted.start();

            // It must NOT crash-loop: it blocks until re-admitted, then reaches RUNNING. Y publishes for its
            // re-admission round, so keep nudging transitions.
            KafkaStreams restarted = appXRestarted;
            await().atMost(Duration.ofSeconds(90)).pollInterval(Duration.ofSeconds(1)).until(() -> {
                requestQuietly(coordinationY);
                return restarted.state() == KafkaStreams.State.RUNNING;
            });

            // Satisfy the held record's dependency; the recovered X drains its buffer to out.
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig(bootstrap))) {
                producer.send(CausalDependencies.empty().stamp(new ProducerRecord<>(PREREQ, "pk", "prereq"))).get();
            }
            assertTrue(awaitOutContains(bootstrap, "ORDER"),
                    "the crashed-then-evicted app recovers (no crash loop) and drains its buffered record");
        } finally {
            appX.close(Duration.ofSeconds(5));
            if (appXRestarted != null) {
                appXRestarted.close(Duration.ofSeconds(30));
            }
            appY.close(Duration.ofSeconds(30));
            coordinationX2.close();
            coordinationY.close();
        }
    }

    private static Topology stageX(CausalCoordination coordination) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(List.of(PREREQ, IN), Consumed.with(Serdes.String(), Serdes.String()))
                .process(CausalProcessors.builder(upperCaser())
                        .addBufferStore("parsley-x", CausalBufferLimit.ofDuration(Duration.ofMinutes(10)))
                        .addBuffers(List.of(PREREQ, IN), Serdes.String(), Serdes.String())
                        .withCoordination(coordination)
                        .build())
                .to(OUT, Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }

    private static Topology stageY(CausalCoordination coordination) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(YIN, Consumed.with(Serdes.String(), Serdes.String()))
                .process(CausalProcessors.builder(upperCaser())
                        .addBufferStore("parsley-y", CausalBufferLimit.ofDuration(Duration.ofMinutes(10)))
                        .addBuffer(CausalBuffer.of(YIN, Serdes.String(), Serdes.String()))
                        .withCoordination(coordination)
                        .build())
                .to(YOUT, Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }

    private static ProcessorSupplier<String, String, String, String> upperCaser() {
        return () -> new Processor<>() {
            private ProcessorContext<String, String> ctx;
            @Override public void init(ProcessorContext<String, String> context) { this.ctx = context; }
            @Override public void process(Record<String, String> record) {
                ctx.forward(record.withValue(record.value().toUpperCase(Locale.ROOT)));
            }
        };
    }

    private static void requestUntilCommitted(CausalCoordination a, CausalCoordination b, String bootstrap, long target) {
        await().atMost(Duration.ofSeconds(90)).pollInterval(Duration.ofSeconds(1)).until(() -> {
            if (highestCommittedEpoch(bootstrap) >= target) {
                return true;
            }
            requestQuietly(a);
            requestQuietly(b);
            return false;
        });
    }

    private static void requestQuietly(CausalCoordination coordination) {
        try {
            coordination.requestEpochTransition();
        } catch (IllegalStateException notReadyYet) {
            // No local member has initialised yet; retry next tick.
        }
    }

    private static boolean leaveExistsFor(String bootstrap, String appIdPrefix) {
        return readEpochEvents(bootstrap).stream()
                .anyMatch(e -> e instanceof EpochEvent.Leave leave && leave.memberId().startsWith(appIdPrefix));
    }

    private static long highestCommittedEpoch(String bootstrap) {
        long highest = 0;
        for (EpochEvent event : readEpochEvents(bootstrap)) {
            if (event instanceof EpochEvent.EpochCommitted commit) {
                highest = Math.max(highest, commit.epochId());
            }
        }
        return highest;
    }

    private static List<EpochEvent> readEpochEvents(String bootstrap) {
        List<EpochEvent> events = new ArrayList<>();
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
                    events.add(EpochEvent.fromBytes(record.value()));
                }
            }
        }
        return events;
    }

    private static boolean awaitOutContains(String bootstrap, String value) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ConsumerConfig.GROUP_ID_CONFIG, "out-reader-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(List.of(OUT));
            long deadline = System.currentTimeMillis() + 60_000;
            while (System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(300))) {
                    if (value.equals(record.value())) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    private static Properties streamsConfig(String bootstrap, String appId, Path stateDir) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 200);
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toAbsolutePath().toString());
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
                newTopics.add(new NewTopic(topic, 1, (short) 1));
            }
            CreateTopicsResult result = admin.createTopics(newTopics);
            result.all().get();
        }
    }
}
