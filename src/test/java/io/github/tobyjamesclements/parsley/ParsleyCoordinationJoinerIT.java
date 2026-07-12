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
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.function.UnaryOperator;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end proof, against a real broker, that a node <strong>deployed into a running, already-
 * coordinated topology blocks until an epoch computed without it commits, then serves</strong>. Modelled
 * the way Kafka Streams actually admits a new node: a redeploy that adds a new stage (a fresh subtopology,
 * with its own task id, so no member-id collision with the running stage).
 *
 * <p>The pipeline {@code t1 -> A -> mid -> B -> out} is a linear DAG, so it is <em>not</em> a full mesh:
 * running member A consumes {@code t1} and produces {@code mid}, and a fresh downstream stage B consumes
 * {@code mid} while carrying dependencies on {@code t1} it cannot observe. The coordinated domain
 * ({@code parsley.coordination.domain-topics = t1,mid,out}) makes {@link CausalTopology#assemble}
 * auto-wire a passthrough source for the one domain topic each stage does not otherwise touch, so every
 * stage's own subscriptions cover the whole domain and the full-mesh precondition an epoch commits under
 * ({@link ParsleyEpochLog#isFullMeshSatisfied()}) is satisfied.
 *
 * <p>Stage A and stage B run as <strong>separate applications</strong> (distinct {@code application.id}s)
 * sharing the one epoch-events log — the way Kafka Streams actually admits a new node, and the only way
 * the domain topic passthrough can wire without colliding with a stage's real source (a single app that
 * both produced {@code mid} and passthrough-consumed it, or sourced {@code t1} in one subtopology and
 * passthrough-sourced it in another, would register the same topic twice). Unlike
 * {@link ParsleyCoordinationMultiAppIT}, which starts both apps together, stage B here joins a domain A
 * has <em>already</em> established an epoch in, so it exercises the block-until-admitted join path.
 */
@Testcontainers(disabledWithoutDocker = true)
class ParsleyCoordinationJoinerIT {

    @Container
    private final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"));

    private static final String IN = "t1";
    private static final String MID = "mid";
    private static final String OUT = "out";
    private static final String EPOCH_EVENTS = "epoch-events";

    /**
     * Phase 1 runs stage A alone (t1 -> A -> mid) and establishes an epoch. Phase 2 starts stage B
     * (mid -> B -> out) as a separate app into the still-running domain: B's task is fresh, so on init it
     * blocks until an epoch that post-dates its join commits (A, still a running member, publishes for it),
     * then replays {@code mid} from the start and serves {@code out}. Asserts the epoch advances past
     * phase 1 (B's join drove a new commit) and B serves its sink.
     */
    @Test
    void aNodeAddedToARunningTopologyBlocksUntilItsEpochCommitsThenServes() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        createTopics(bootstrap, IN, MID, OUT, EPOCH_EVENTS);
        String appIdA = "joiner-a-" + UUID.randomUUID();
        String appIdB = "joiner-b-" + UUID.randomUUID();
        Path stateDirA = Files.createTempDirectory("parsley-joiner-a");
        Path stateDirB = Files.createTempDirectory("parsley-joiner-b");

        // Stage A runs for the whole test; stage B joins the domain A has already established an epoch in.
        try (CausalStreams appA = new CausalStreams(stageATopology(), streamsConfig(bootstrap, appIdA, stateDirA))) {
            appA.start();
            await().atMost(Duration.ofSeconds(30)).until(() -> appA.state() == KafkaStreams.State.RUNNING);
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig(bootstrap))) {
                for (int i = 0; i < 3; i++) {
                    producer.send(stampEmptyDeps(new ProducerRecord<>(IN, "k", "v" + i))).get();
                }
            }
            requestUntilCommitted(appA, bootstrap, 1L);
            requestUntilCommitted(appA, bootstrap, 2L);
            long epochAfterPhase1 = highestCommittedEpoch(bootstrap);
            assertTrue(epochAfterPhase1 >= 2, "phase 1 must establish an epoch B can join into");

            // Phase 2: stage B joins as a fresh member of the running domain.
            try (CausalStreams appB = new CausalStreams(stageBTopology(), streamsConfig(bootstrap, appIdB, stateDirB))) {
                appB.start();
                // Do not await RUNNING: while B's task blocks in init (the joiner wait), the instance stays
                // in REBALANCING. A, still running in its own app, publishes for B's round, so B's join
                // drives the epoch past where phase 1 left it — that is the unblock signal.
                await().atMost(Duration.ofSeconds(120))
                        .until(() -> highestCommittedEpoch(bootstrap) > epochAfterPhase1);

                // Having unblocked and adopted its floor, B replays mid from the start and serves out.
                assertTrue(awaitSinkServed(bootstrap), "the joined stage B serves its sink after its epoch commits");
            }
        }
    }

    /** Stage A: t1 -> upper-case -> mid. */
    private static CausalTopology stageATopology() {
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream(IN, Serdes.String(), Serdes.String())
                .process(mapper(v -> v.toUpperCase(Locale.ROOT)))
                .to("mid-sink", MID, Serdes.String(), Serdes.String());
        return builder.build();
    }

    /** Stage B: mid -> prefix -> out. */
    private static CausalTopology stageBTopology() {
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream(MID, Serdes.String(), Serdes.String())
                .process(mapper(v -> "B:" + v))
                .to("out-sink", OUT, Serdes.String(), Serdes.String());
        return builder.build();
    }

    private static ProcessorSupplier<String, String, String, String> mapper(UnaryOperator<String> fn) {
        return () -> new Processor<>() {
            private ProcessorContext<String, String> ctx;

            @Override
            public void init(ProcessorContext<String, String> context) {
                this.ctx = context;
            }

            @Override
            public void process(Record<String, String> record) {
                ctx.forward(record.withValue(fn.apply(record.value())));
            }
        };
    }

    /** Requests transitions until the log shows {@code EpochCommitted(targetEpoch)}; requests coalesce. */
    private static void requestUntilCommitted(CausalStreams streams, String bootstrap, long targetEpoch) {
        await().atMost(Duration.ofSeconds(60)).until(() -> {
            if (highestCommittedEpoch(bootstrap) >= targetEpoch) {
                return true;
            }
            try {
                streams.requestEpochTransition();
            } catch (IllegalStateException notReadyYet) {
                // No local member has joined/initialised yet; retry on the next tick.
            }
            return false;
        });
    }

    private static long highestCommittedEpoch(String bootstrap) {
        long highest = 0;
        for (ParsleyEpochEvent event : readEpochEvents(bootstrap)) {
            if (event instanceof ParsleyEpochEvent.EpochCommitted commit) {
                highest = Math.max(highest, commit.epochId());
            }
        }
        return highest;
    }

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
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toAbsolutePath().toString());
        props.put(ParsleyConfig.COORDINATION_EPOCH_EVENTS_TOPIC, EPOCH_EVENTS);
        // The linear DAG is not a full mesh; the coordinated domain auto-wires a passthrough source for
        // the one domain topic each stage does not otherwise touch, so every stage covers the whole domain.
        props.put(ParsleyConfig.COORDINATION_DOMAIN_TOPICS, IN + "," + MID + "," + OUT);
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
