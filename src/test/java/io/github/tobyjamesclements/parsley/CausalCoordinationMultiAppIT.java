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
 * Proves topology-epoch coordination spans a <strong>DAG of many independently-deployed applications</strong>
 * — the production shape. App A ({@code t1 -> mid}) and app B ({@code mid -> out}) are separate
 * {@link KafkaStreams} applications with distinct {@code application.id}s, sharing one epoch-events log for
 * the causal domain. Asserts both apps appear as <em>distinct</em> members on the shared log (the member id
 * being {@code application.id/taskId}, so A's {@code 0_0} and B's {@code 0_0} do not collide) and that a
 * transition commits an epoch — coordination the single-app ITs cannot exercise.
 */
@Testcontainers(disabledWithoutDocker = true)
class CausalCoordinationMultiAppIT {

    @Container
    private final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"));

    private static final String IN = "t1";
    private static final String MID = "mid";
    private static final String OUT = "out";
    private static final String EPOCH_EVENTS = "epoch-events";

    /**
     * Two applications form one causal DAG over a shared epoch-events log. Records flow t1 -> A -> mid ->
     * B -> out (across the app boundary), and a transition is driven; asserts the DAG serves end to end,
     * both apps are distinct members on the log, and an epoch commits.
     */
    @Test
    void aDagOfTwoApplicationsCoordinatesOverOneSharedLog() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        createTopics(bootstrap, IN, MID, OUT, EPOCH_EVENTS);
        String runId = UUID.randomUUID().toString().substring(0, 8);
        String appIdA = "dag-a-" + runId;
        String appIdB = "dag-b-" + runId;

        // App A is source-layer (t1 is external); app B consumes mid (internal to the DAG), so it declares
        // no external sources and is driven by A's in-band markers relayed through mid.
        CausalCoordination coordinationA = CausalCoordination.create(EPOCH_EVENTS, Set.of(IN));
        CausalCoordination coordinationB = CausalCoordination.create(EPOCH_EVENTS, Set.of());
        Path stateA = Files.createTempDirectory("parsley-dag-a");
        Path stateB = Files.createTempDirectory("parsley-dag-b");

        try (KafkaStreams appA = new KafkaStreams(stageA(coordinationA), streamsConfig(bootstrap, appIdA, stateA));
             KafkaStreams appB = new KafkaStreams(stageB(coordinationB), streamsConfig(bootstrap, appIdB, stateB))) {
            appA.start();
            appB.start();
            await().atMost(Duration.ofSeconds(60)).until(() ->
                    appA.state() == KafkaStreams.State.RUNNING && appB.state() == KafkaStreams.State.RUNNING);

            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig(bootstrap))) {
                for (int i = 0; i < 3; i++) {
                    producer.send(stampEmptyDeps(new ProducerRecord<>(IN, "k", "v" + i))).get();
                }
            }

            // The DAG serves end to end across the app boundary: t1 -> A (upper) -> mid -> B ("B:") -> out.
            assertTrue(awaitServed(bootstrap), "records must flow through both applications to out");

            // Drive a transition; both apps participate as distinct members on the shared log.
            requestUntilCommitted(coordinationA, coordinationB, bootstrap, 1L);
            requestUntilCommitted(coordinationA, coordinationB, bootstrap, 2L);

            Set<String> members = memberIds(bootstrap);
            assertTrue(members.stream().anyMatch(m -> m.startsWith(appIdA)),
                    "app A must be a member on the shared log (members: " + members + ")");
            assertTrue(members.stream().anyMatch(m -> m.startsWith(appIdB)),
                    "app B must be a distinct member on the shared log (members: " + members + ")");
            assertTrue(highestCommittedEpoch(bootstrap) >= 2,
                    "the two-app DAG must commit an epoch transition over the shared log");
        } finally {
            coordinationA.close();
            coordinationB.close();
        }
    }

    private static Topology stageA(CausalCoordination coordination) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(IN, Consumed.with(Serdes.String(), Serdes.String()))
                .process(CausalProcessors.builder(mapper(v -> v.toUpperCase(Locale.ROOT)))
                        .addBufferStore("parsley-a", CausalBufferLimit.ofDuration(Duration.ofSeconds(120)))
                        .addBuffer(CausalBuffer.of(IN, Serdes.String(), Serdes.String()))
                        .withCoordination(coordination)
                        .build())
                .to(MID, Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }

    private static Topology stageB(CausalCoordination coordination) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(MID, Consumed.with(Serdes.String(), Serdes.String()))
                .process(CausalProcessors.builder(mapper(v -> "B:" + v))
                        .addBufferStore("parsley-b", CausalBufferLimit.ofDuration(Duration.ofSeconds(120)))
                        .addBuffer(CausalBuffer.of(MID, Serdes.String(), Serdes.String()))
                        .withCoordination(coordination)
                        .build())
                .to(OUT, Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }

    private static ProcessorSupplier<String, String, String, String> mapper(java.util.function.UnaryOperator<String> fn) {
        return () -> new Processor<>() {
            private ProcessorContext<String, String> ctx;
            @Override public void init(ProcessorContext<String, String> context) { this.ctx = context; }
            @Override public void process(Record<String, String> record) {
                ctx.forward(record.withValue(fn.apply(record.value())));
            }
        };
    }

    private static void requestUntilCommitted(CausalCoordination a, CausalCoordination b, String bootstrap, long target) {
        await().atMost(Duration.ofSeconds(90)).until(() -> {
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
            // No local member has initialised on this app yet; retry next tick.
        }
    }

    private static Set<String> memberIds(String bootstrap) {
        Set<String> members = new HashSet<>();
        for (EpochEvent event : readEpochEvents(bootstrap)) {
            if (event instanceof EpochEvent.JoinRequested j) {
                members.add(j.memberId());
            } else if (event instanceof EpochEvent.FrontierPublished f) {
                members.add(f.memberId());
            }
        }
        return members;
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

    private static boolean awaitServed(String bootstrap) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ConsumerConfig.GROUP_ID_CONFIG, "out-reader-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class.getName());
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(List.of(OUT));
            long deadline = System.currentTimeMillis() + 60_000;
            while (System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(300))) {
                    if (record.value() != null && record.value().startsWith("B:")) {
                        return true;
                    }
                }
            }
            return false;
        }
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
