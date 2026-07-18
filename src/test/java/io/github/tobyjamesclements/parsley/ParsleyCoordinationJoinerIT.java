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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end proof, against a real broker, of the <strong>genesis cohort barrier</strong>: a coordinated
 * topology whose founding roster is {@code {A,B}} does not seal genesis until <em>both</em> founders have
 * declared, even though A comes up first and consumes at the empty genesis floor. Modelled with each stage
 * as its own application (a fresh subtopology, its own task id, so no member-id collision).
 *
 * <p>The pipeline {@code t1 -> A -> mid -> B -> out} is a linear DAG, so it is <em>not</em> a full mesh:
 * stage A consumes {@code t1} and produces {@code mid}, and stage B consumes {@code mid} while carrying
 * dependencies on {@code t1} it cannot observe. The coordinated domain
 * ({@code parsley.coordination.domain-topics = t1,mid,out}) makes {@link CausalTopology#assemble}
 * auto-wire a passthrough source for the one domain topic each stage does not otherwise touch, so every
 * stage's own subscriptions cover the whole domain — the full-mesh precondition a commit requires (the
 * mesh conjunct of {@link ParsleyEpochLog#evaluateCommit()}).
 *
 * <p>Stage A and stage B run as <strong>separate applications</strong> (distinct {@code application.id}s)
 * sharing the one epoch-events log — the way Kafka Streams actually admits a new node, and the only way
 * the domain topic passthrough can wire without colliding with a stage's real source (a single app that
 * both produced {@code mid} and passthrough-consumed it, or sourced {@code t1} in one subtopology and
 * passthrough-sourced it in another, would register the same topic twice). Both apps declare the same
 * founding member-app roster {@code {A,B}}, so this exercises the genesis cohort barrier: A comes up first
 * and consumes at the empty genesis floor, but genesis does not seal until B — the other founder — has
 * also declared its full task set.
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
     * The genesis cohort barrier, end-to-end. Both stages declare the founding roster {@code {A,B}}. Stage A
     * comes up first and consumes {@code t1} at the empty genesis floor, producing to {@code mid} — but
     * genesis must NOT seal while founder B is still absent (sealing it early would leave B to be admitted
     * later at a non-empty floor and skip the genesis-era {@code mid} records). Once stage B starts and
     * declares its full task set, the cohort is complete, genesis commits, and B replays {@code mid} from
     * the start and serves {@code out}. Asserts no epoch commits while A is alone, then genesis commits once
     * B has joined the cohort and B serves its sink.
     */
    @Test
    void genesisWaitsForTheWholeFoundingCohortBeforeSealing() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        createTopics(bootstrap, IN, MID, OUT, EPOCH_EVENTS);
        String appIdA = "joiner-a-" + UUID.randomUUID();
        String appIdB = "joiner-b-" + UUID.randomUUID();
        String roster = appIdA + "," + appIdB;
        Path stateDirA = Files.createTempDirectory("parsley-joiner-a");
        Path stateDirB = Files.createTempDirectory("parsley-joiner-b");

        try (CausalStreams appA = new CausalStreams(stageATopology(), streamsConfig(bootstrap, appIdA, stateDirA, roster))) {
            appA.start();
            await().atMost(Duration.ofSeconds(30)).until(() -> appA.state() == KafkaStreams.State.RUNNING);
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig(bootstrap))) {
                for (int i = 0; i < 3; i++) {
                    producer.send(stampEmptyDeps(new ProducerRecord<>(IN, "k", "v" + i))).get();
                }
            }

            // The barrier: genesis must not seal while founder B is still absent from the cohort. A alone
            // is a running founder consuming at the empty floor, but its cohort {A,B} is not yet complete.
            Thread.sleep(5000);
            assertEquals(0L, highestCommittedEpoch(bootstrap),
                    "genesis must wait for the whole founding cohort — B has not declared yet");

            // B joins the founding cohort: the cohort is now complete, so genesis seals.
            try (CausalStreams appB = new CausalStreams(stageBTopology(), streamsConfig(bootstrap, appIdB, stateDirB, roster))) {
                appB.start();
                await().atMost(Duration.ofSeconds(120))
                        .until(() -> highestCommittedEpoch(bootstrap) >= 1L);   // genesis seals with the full cohort
                assertTrue(awaitSinkServed(bootstrap),
                        "once the cohort completes and genesis seals, B replays mid and serves out");
            }
        }
    }

    /** Stage A: t1 -> upper-case -> mid. */
    private static CausalTopology stageATopology() {
        return new CausalStreamsBuilder()
                .stream(IN, Serdes.String(), Serdes.String())
                .process(mapper(v -> v.toUpperCase(Locale.ROOT)))
                .to("mid-sink", MID, Serdes.String(), Serdes.String())
                .build();
    }

    /** Stage B: mid -> prefix -> out. */
    private static CausalTopology stageBTopology() {
        return new CausalStreamsBuilder()
                .stream(MID, Serdes.String(), Serdes.String())
                .process(mapper(v -> "B:" + v))
                .to("out-sink", OUT, Serdes.String(), Serdes.String())
                .build();
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

    private static Properties streamsConfig(String bootstrap, String appId, Path stateDir, String memberApps) {
        Properties props = streamsConfig(bootstrap, appId, stateDir);
        props.put(ParsleyConfig.COORDINATION_MEMBER_APPS, memberApps);
        return props;
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
