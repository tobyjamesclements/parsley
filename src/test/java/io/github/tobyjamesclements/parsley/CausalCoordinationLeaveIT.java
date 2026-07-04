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
 * The headline mandatory guarantee: a member that is <strong>gone</strong> (hard-killed, no graceful
 * leave) must not freeze the DAG's epochs. Two apps form a DAG over a shared epoch-events log; after both
 * are running members, app B is hard-killed. The surviving app A, whose next round waits for B's frontier,
 * <strong>evicts</strong> B after the eviction timeout and a subsequent epoch still commits.
 */
@Testcontainers(disabledWithoutDocker = true)
class CausalCoordinationLeaveIT {

    @Container
    private final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"));

    private static final String IN = "t1";
    private static final String MID = "mid";
    private static final String OUT = "out";
    private static final String EPOCH_EVENTS = "epoch-events";

    /**
     * App A (t1 -> mid) and app B (mid -> out) both become running members; app B is then hard-killed
     * without leaving. A transition is driven on A; A's round waits for B's frontier, evicts B after the
     * (short) eviction timeout, and commits. Asserts a {@code Leave} for B's member reaches the log and the
     * committed epoch advances past where B was last present.
     */
    @Test
    void aHardKilledAppIsEvictedSoEpochsKeepProgressing() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        createTopics(bootstrap, IN, MID, OUT, EPOCH_EVENTS);
        String runId = UUID.randomUUID().toString().substring(0, 8);
        String appIdA = "leave-a-" + runId;
        String appIdB = "leave-b-" + runId;

        // App A evicts a silent member after 3s (short, for the test); the default is 30s.
        CausalCoordination coordinationA =
                CausalCoordination.create(EPOCH_EVENTS, Set.of(IN), CausalCoordination.DEFAULT_JOIN_TIMEOUT, Duration.ofSeconds(3));
        CausalCoordination coordinationB = CausalCoordination.create(EPOCH_EVENTS, Set.of());
        Path stateA = Files.createTempDirectory("parsley-leave-a");
        Path stateB = Files.createTempDirectory("parsley-leave-b");

        KafkaStreams appA = new KafkaStreams(stageA(coordinationA), streamsConfig(bootstrap, appIdA, stateA));
        KafkaStreams appB = new KafkaStreams(stageB(coordinationB), streamsConfig(bootstrap, appIdB, stateB));
        try {
            appA.start();
            appB.start();
            await().atMost(Duration.ofSeconds(60)).until(() ->
                    appA.state() == KafkaStreams.State.RUNNING && appB.state() == KafkaStreams.State.RUNNING);
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig(bootstrap))) {
                producer.send(stampEmptyDeps(new ProducerRecord<>(IN, "k", "v"))).get();
            }

            // Establish an epoch so both apps are running members.
            requestUntilCommitted(coordinationA, coordinationB, bootstrap, 1L);
            long epochBeforeKill = highestCommittedEpoch(bootstrap);
            assertTrue(epochBeforeKill >= 1, "both apps must be running members before the kill");

            // HARD-KILL app B — close it and its runtime WITHOUT a graceful leave(). Its member stays a
            // running member on the log, so A's next round would wait for it forever without eviction.
            appB.close(Duration.ofSeconds(30));
            coordinationB.close();

            // Drive transitions on A; A's round waits for B's frontier, then evicts B and commits.
            await().atMost(Duration.ofSeconds(90)).until(() -> {
                requestQuietly(coordinationA);
                return highestCommittedEpoch(bootstrap) > epochBeforeKill;
            });

            assertTrue(leaveExistsFor(bootstrap, appIdB),
                    "the surviving app must evict the gone member (a Leave for app B on the log)");
            assertTrue(highestCommittedEpoch(bootstrap) > epochBeforeKill,
                    "epochs keep progressing after the app was hard-killed — the domain did not freeze");
        } finally {
            appA.close(Duration.ofSeconds(30));
            appB.close(Duration.ofSeconds(5));
            coordinationA.close();
        }
    }

    private static boolean leaveExistsFor(String bootstrap, String appIdPrefix) {
        return readEpochEvents(bootstrap).stream()
                .anyMatch(e -> e instanceof EpochEvent.Leave leave && leave.memberId().startsWith(appIdPrefix));
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
            // No local member has initialised yet; retry next tick.
        }
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
