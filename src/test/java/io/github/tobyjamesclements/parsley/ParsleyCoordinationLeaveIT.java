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
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The block-until-drained guarantee: a member that is <strong>gone</strong> (hard-killed, no graceful
 * leave) <strong>blocks</strong> the DAG's next epoch transition rather than being evicted — because
 * evicting a member that might hold un-drained buffered records would strand those records below a floor
 * committed without it, a causal-safety violation. Two apps form a DAG over a shared epoch-events log;
 * after both are running members, app B is hard-killed. The surviving app A opens a round that cannot
 * commit while B is absent; once B <strong>restarts</strong> and publishes, the epoch commits — and no
 * {@code Leave} for B is ever written (it was never evicted).
 *
 * <p>Both apps consume the <em>same</em> input topic and produce to the <em>same</em> output topic
 * (rather than a chained {@code t1 -> mid -> out} pipeline) so each app's own declared subscriptions
 * trivially cover the whole coordinated domain — a genuine full mesh (see {@link
 * ParsleyEpochLog#isFullMeshSatisfied()}) — without needing the passthrough-source auto-wiring a chained
 * pipeline would require, which does not exist yet. This is purely a topology-shape choice for the test;
 * it does not change what member-lifecycle behaviour is under test.
 */
@Testcontainers(disabledWithoutDocker = true)
class ParsleyCoordinationLeaveIT {

    @Container
    private final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"));

    private static final String IN = "t1";
    private static final String OUT = "out";
    private static final String EPOCH_EVENTS = "epoch-events";

    /**
     * App A and app B (each {@code t1 -> out}) both become running members; app B is then hard-killed
     * without leaving. A transition is driven on A; A's round opens but cannot commit while B is absent
     * (asserted: the epoch does not advance for a window). App B is then restarted over its own state; it
     * rejoins as the same running member and publishes, and the epoch finally commits. Asserts the epoch
     * advances only after the restart, and that <em>no</em> {@code Leave} for B was written — a gone member
     * is waited for, never evicted.
     */
    @Test
    void aHardKilledAppBlocksTheEpochUntilItRestartsAndPublishes() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        createTopics(bootstrap, IN, OUT, EPOCH_EVENTS);
        String runId = UUID.randomUUID().toString().substring(0, 8);
        String appIdA = "leave-a-" + runId;
        String appIdB = "leave-b-" + runId;

        ParsleyCoordination coordinationA = ParsleyCoordination.create(EPOCH_EVENTS);
        ParsleyCoordination coordinationB = ParsleyCoordination.create(EPOCH_EVENTS);
        Path stateA = Files.createTempDirectory("parsley-leave-a");
        Path stateB = Files.createTempDirectory("parsley-leave-b");

        KafkaStreams appA = new KafkaStreams(
                stage(coordinationA, "parsley-a", v -> "A:" + v), streamsConfig(bootstrap, appIdA, stateA));
        KafkaStreams appB = new KafkaStreams(
                stage(coordinationB, "parsley-b", v -> "B:" + v), streamsConfig(bootstrap, appIdB, stateB));
        KafkaStreams appBRestarted = null;
        ParsleyCoordination coordinationBRestarted = null;
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
            // running member on the log, so A's next round must wait for it (block-until-drained).
            appB.close(Duration.ofSeconds(30));
            coordinationB.close();

            // Drive a transition on A: the round opens but cannot commit while B is absent. Assert the epoch
            // does NOT advance over a window — the domain deliberately blocks rather than evicting B.
            for (int i = 0; i < 10; i++) {
                requestQuietly(coordinationA);
                Thread.sleep(500);
            }
            assertEquals(epochBeforeKill, highestCommittedEpoch(bootstrap),
                    "while the gone member is absent the epoch must not advance — it blocks, it does not evict");
            assertFalse(leaveExistsFor(bootstrap, appIdB),
                    "the gone member is never evicted, so no Leave for it is written");

            // RESTART app B over its own state: it rejoins as the same running member and publishes, so the
            // blocked round finally commits.
            coordinationBRestarted = ParsleyCoordination.create(EPOCH_EVENTS);
            appBRestarted = new KafkaStreams(stage(coordinationBRestarted, "parsley-b", v -> "B:" + v),
                    streamsConfig(bootstrap, appIdB, stateB));
            appBRestarted.start();
            ParsleyCoordination coordB = coordinationBRestarted;
            await().atMost(Duration.ofSeconds(90)).until(() -> {
                requestQuietly(coordinationA);
                requestQuietly(coordB);
                return highestCommittedEpoch(bootstrap) > epochBeforeKill;
            });

            assertTrue(highestCommittedEpoch(bootstrap) > epochBeforeKill,
                    "the epoch commits once the restarted member returns and publishes");
            assertFalse(leaveExistsFor(bootstrap, appIdB),
                    "the member was waited for and rejoined — never evicted, so still no Leave for it");
        } finally {
            appA.close(Duration.ofSeconds(30));
            appB.close(Duration.ofSeconds(5));
            if (appBRestarted != null) {
                appBRestarted.close(Duration.ofSeconds(30));
            }
            coordinationA.close();
            if (coordinationBRestarted != null) {
                coordinationBRestarted.close();
            }
        }
    }

    private static boolean leaveExistsFor(String bootstrap, String appIdPrefix) {
        return readEpochEvents(bootstrap).stream()
                .anyMatch(e -> e instanceof ParsleyEpochEvent.Leave leave && leave.memberId().startsWith(appIdPrefix));
    }

    /**
     * Builds one member's topology: {@code t1 -> out}, namespaced under its own buffer store. Both A and
     * B use this — same input, same output — so each member's own declared subscriptions trivially cover
     * the whole domain (see the class Javadoc).
     */
    private static Topology stage(ParsleyCoordination coordination, String namespace,
                                  java.util.function.UnaryOperator<String> fn) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(IN, Consumed.with(Serdes.String(), Serdes.String()))
                .process(ParsleyProcessors.builder(mapper(fn))
                        .addBufferStore(namespace)
                        .addBuffer(new ParsleyBuffer<>(IN, Serdes.String(), Serdes.String()))
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

    private static void requestUntilCommitted(ParsleyCoordination a, ParsleyCoordination b, String bootstrap, long target) {
        await().atMost(Duration.ofSeconds(90)).until(() -> {
            if (highestCommittedEpoch(bootstrap) >= target) {
                return true;
            }
            requestQuietly(a);
            requestQuietly(b);
            return false;
        });
    }

    private static void requestQuietly(ParsleyCoordination coordination) {
        try {
            coordination.requestEpochTransition();
        } catch (IllegalStateException notReadyYet) {
            // No local member has initialised yet; retry next tick.
        }
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
