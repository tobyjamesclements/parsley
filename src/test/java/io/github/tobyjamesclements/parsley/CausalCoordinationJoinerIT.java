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
 * End-to-end proof, against a real broker, that a node <strong>deployed into a running, already-
 * coordinated topology blocks until an epoch computed without it commits, then serves</strong>. Modelled
 * the way Kafka Streams actually admits a new node: a redeploy that adds a new stage (a fresh subtopology,
 * with its own task id, so no member-id collision with the running stage).
 */
@Testcontainers(disabledWithoutDocker = true)
class CausalCoordinationJoinerIT {

    @Container
    private final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"));

    private static final String IN = "t1";
    private static final String MID = "mid";
    private static final String OUT = "out";
    private static final String EPOCH_EVENTS = "epoch-events";

    /**
     * Phase 1 runs a single stage (t1 -> A -> mid) and establishes an epoch with a non-empty floor. Phase
     * 2 redeploys with a second stage added (mid -> B -> out): B's tasks are fresh, so on init they block
     * until an epoch that post-dates their join commits (A, still a running member, publishes for it),
     * then replay {@code mid} from the start and serve {@code out}. Asserts the epoch advances past phase
     * 1 (B's join drove a new commit) and B serves its sink.
     */
    @Test
    void aNodeAddedToARunningTopologyBlocksUntilItsEpochCommitsThenServes() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        createTopics(bootstrap, IN, MID, OUT, EPOCH_EVENTS);
        String appId = "joiner-it-" + UUID.randomUUID();

        // Phase 1: run stage A alone and establish an epoch (non-empty floor from delivered t1 records).
        long epochAfterPhase1;
        CausalCoordination coordination1 = CausalCoordination.create(EPOCH_EVENTS, Set.of(IN));
        Path stateDir1 = Files.createTempDirectory("parsley-joiner-1");
        try (KafkaStreams streams = new KafkaStreams(stageAOnly(coordination1), streamsConfig(bootstrap, appId, stateDir1))) {
            streams.start();
            await().atMost(Duration.ofSeconds(30)).until(() -> streams.state() == KafkaStreams.State.RUNNING);
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig(bootstrap))) {
                for (int i = 0; i < 3; i++) {
                    producer.send(stampEmptyDeps(new ProducerRecord<>(IN, "k", "v" + i))).get();
                }
            }
            requestUntilCommitted(coordination1, bootstrap, 1L);
            requestUntilCommitted(coordination1, bootstrap, 2L);
            epochAfterPhase1 = highestCommittedEpoch(bootstrap);
        } finally {
            coordination1.close();
        }
        assertTrue(epochAfterPhase1 >= 2, "phase 1 must establish an epoch B can join into");

        // Phase 2: redeploy with stage B added. A restores (running member); B is a fresh joiner.
        CausalCoordination coordination2 = CausalCoordination.create(EPOCH_EVENTS, Set.of(IN));
        Path stateDir2 = Files.createTempDirectory("parsley-joiner-2");
        try (KafkaStreams streams = new KafkaStreams(stageAPlusB(coordination2), streamsConfig(bootstrap, appId, stateDir2))) {
            streams.start();
            // Do not await RUNNING: while B's task blocks in init (the joiner wait), the instance stays in
            // REBALANCING. A runs on its own StreamThread (num.stream.threads=2) and publishes for B's
            // round, so B's join drives the epoch past where phase 1 left it — that is the unblock signal.
            await().atMost(Duration.ofSeconds(120))
                    .until(() -> highestCommittedEpoch(bootstrap) > epochAfterPhase1);

            // Having unblocked and adopted its floor, B replays mid from the start and serves out.
            assertTrue(awaitSinkServed(bootstrap), "the joined stage B serves its sink after its epoch commits");
        } finally {
            coordination2.close();
        }
    }

    /** Stage A only: t1 -> upper-case -> mid. */
    private static Topology stageAOnly(CausalCoordination coordination) {
        StreamsBuilder builder = new StreamsBuilder();
        addStageA(builder, coordination);
        return builder.build();
    }

    /** Stage A plus the newly-added stage B: mid -> prefix -> out. */
    private static Topology stageAPlusB(CausalCoordination coordination) {
        StreamsBuilder builder = new StreamsBuilder();
        addStageA(builder, coordination);
        builder.stream(MID, Consumed.with(Serdes.String(), Serdes.String()))
                .process(CausalProcessors.builder(prefixer())
                        .addBufferStore("parsley-b", CausalBufferLimit.ofDuration(Duration.ofSeconds(120)))
                        .addBuffer(CausalBuffer.of(MID, Serdes.String(), Serdes.String()))
                        .withCoordination(coordination)
                        .build())
                .to(OUT, Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }

    private static void addStageA(StreamsBuilder builder, CausalCoordination coordination) {
        builder.stream(IN, Consumed.with(Serdes.String(), Serdes.String()))
                .process(CausalProcessors.builder(upperCaser())
                        .addBufferStore("parsley-a", CausalBufferLimit.ofDuration(Duration.ofSeconds(120)))
                        .addBuffer(CausalBuffer.of(IN, Serdes.String(), Serdes.String()))
                        .withCoordination(coordination)
                        .build())
                .to(MID, Produced.with(Serdes.String(), Serdes.String()));
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

    private static ProcessorSupplier<String, String, String, String> prefixer() {
        return () -> new Processor<>() {
            private ProcessorContext<String, String> ctx;
            @Override public void init(ProcessorContext<String, String> context) { this.ctx = context; }
            @Override public void process(Record<String, String> record) {
                ctx.forward(record.withValue("B:" + record.value()));
            }
        };
    }

    private static void requestUntilCommitted(CausalCoordination coordination, String bootstrap, long targetEpoch) {
        await().atMost(Duration.ofSeconds(60)).until(() -> {
            if (highestCommittedEpoch(bootstrap) >= targetEpoch) {
                return true;
            }
            try {
                coordination.requestEpochTransition();
            } catch (IllegalStateException notReadyYet) {
                // No local member has joined/initialised yet; retry on the next tick.
            }
            return false;
        });
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

    private static boolean awaitSinkServed(String bootstrap) {
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
        // Two threads so a blocking joiner task (stage B's init) does not starve the running stage A on
        // the same thread — A must keep publishing for B's round to commit and unblock B.
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
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
