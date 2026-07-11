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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The causal-safety guarantee across a crash-and-restart, and the proof that block-until-drained never
 * severs an un-drained buffer. App X holds a record on {@code in} that depends on {@code prereq@0} (never
 * yet produced); X is hard-killed with the record still buffered. Because a crashed member is
 * <strong>not</strong> evicted, the surviving app Y's transition <strong>blocks</strong> — the epoch does
 * not advance and no {@code Leave} for X is written. X then restarts over its buffer changelog and, still a
 * running member, resumes at once. The held record must remain buffered — <strong>not</strong> delivered
 * before its cause (the old severing bug) — and drain to {@code out} only once {@code prereq@0} finally
 * arrives.
 *
 * <p>App Y — the survivor keeping the domain alive while X is down — consumes the <em>same</em> topics
 * as X (also {@code prereq + in -> out}, distinguished by a lower-cased value instead of X's
 * upper-cased one) rather than an unrelated {@code yin -> yout} pair, so each app's own declared
 * subscriptions trivially cover the whole coordinated domain — a genuine full mesh (see {@link
 * ParsleyEpochLog#isFullMeshSatisfied()}) — without needing the passthrough-source auto-wiring an
 * unrelated topic pair would require, which does not exist yet. This is purely a topology-shape choice
 * for the test; it does not change what member-lifecycle behaviour is under test.
 */
@Testcontainers(disabledWithoutDocker = true)
class ParsleyCoordinationCrashRestartIT {

    @Container
    private final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"));

    private static final String PREREQ = "prereq";   // X's held record depends on prereq@0
    private static final String IN = "in";           // X's business input
    private static final String OUT = "out";         // X's sink
    private static final String EPOCH_EVENTS = "epoch-events";

    /**
     * App X (prereq + in -> out) holds a record on {@code in} depending on {@code prereq@0}; survivor Y keeps
     * the domain alive. X is hard-killed with the record buffered. A transition driven on Y opens a round
     * that blocks on the absent X (asserted: the epoch does not advance and X is not evicted). X restarts
     * over its buffer changelog and resumes as the same running member. The held record must NOT yet be at
     * {@code out} (never severed/delivered concurrently); once {@code prereq@0} is produced, it drains in
     * causal order.
     */
    @Test
    void aCrashedMemberBlocksThenRestartsAndDrainsInCausalOrder() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        createTopics(bootstrap, PREREQ, IN, OUT, EPOCH_EVENTS);
        String runId = UUID.randomUUID().toString().substring(0, 8);
        String appIdX = "crash-x-" + runId;   // stable across the restart
        String appIdY = "crash-y-" + runId;

        ParsleyCoordination coordinationY = ParsleyCoordination.create(EPOCH_EVENTS);
        ParsleyCoordination coordinationX1 = ParsleyCoordination.create(EPOCH_EVENTS);
        Path stateY = Files.createTempDirectory("parsley-crash-y");
        Path stateX1 = Files.createTempDirectory("parsley-crash-x1");

        KafkaStreams appY = new KafkaStreams(stageY(coordinationY), streamsConfig(bootstrap, appIdY, stateY));
        KafkaStreams appX = new KafkaStreams(stageX(coordinationX1), streamsConfig(bootstrap, appIdX, stateX1));
        ParsleyCoordination coordinationX2 = ParsleyCoordination.create(EPOCH_EVENTS);
        KafkaStreams appXRestarted = null;
        try {
            appY.start();
            appX.start();
            await().atMost(Duration.ofSeconds(60)).until(() ->
                    appY.state() == KafkaStreams.State.RUNNING && appX.state() == KafkaStreams.State.RUNNING);

            // Establish an epoch so both X and Y are running members (Y needs no separate "kick" — it
            // shares X's input topics, so it is already actively consuming).
            requestUntilCommitted(coordinationY, coordinationX1, bootstrap, 1L);

            // Buffer a record in X: an `in` record that depends on prereq@0 (never yet produced) is held.
            Properties resolverProps = new Properties();
            resolverProps.put("bootstrap.servers", bootstrap);
            CausalDependencies orderDeps = CausalDependencies.using(resolverProps)
                    .observe(new ConsumerRecord<>(PREREQ, 0, 0L, "pk", "prereq"));
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig(bootstrap))) {
                producer.send(orderDeps.stamp(new ProducerRecord<>(IN, "k", "order"))).get();
            }

            // Hard-kill X with the record still buffered (no graceful leave).
            appX.close(Duration.ofSeconds(30));
            coordinationX1.close();

            // Drive a transition on Y: the round opens but cannot commit while X is absent. The epoch must
            // NOT advance (block-until-drained) and X must NOT be evicted.
            long epochBeforeKill = highestCommittedEpoch(bootstrap);
            for (int i = 0; i < 10; i++) {
                requestQuietly(coordinationY);
                Thread.sleep(500);
            }
            assertEquals(epochBeforeKill, highestCommittedEpoch(bootstrap),
                    "the round blocks on the absent member — the epoch does not advance");
            assertFalse(leaveExistsFor(bootstrap, appIdX), "the crashed member is never evicted, so no Leave for it");

            // Restart X (same application id, so it restores its buffer changelog; fresh local state dir). It is
            // still a running member, so it resumes immediately.
            Path stateX2 = Files.createTempDirectory("parsley-crash-x2");
            appXRestarted = new KafkaStreams(stageX(coordinationX2), streamsConfig(bootstrap, appIdX, stateX2));
            appXRestarted.start();
            KafkaStreams restarted = appXRestarted;
            await().atMost(Duration.ofSeconds(90)).pollInterval(Duration.ofSeconds(1)).until(() -> {
                requestQuietly(coordinationY);
                return restarted.state() == KafkaStreams.State.RUNNING;
            });

            // SAFETY: prereq@0 is still unproduced, so the held record's dependency is unsatisfied. It must
            // NOT have been delivered — the reversed severing bug would have released it out of causal order.
            assertFalse(outAppearsWithin(bootstrap, "ORDER", Duration.ofSeconds(8)),
                    "the held record is not delivered before its cause — block-until-drained never severs");

            // Satisfy the held record's dependency; the recovered X drains it to out, in causal order.
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig(bootstrap))) {
                producer.send(CausalDependencies.empty().stamp(new ProducerRecord<>(PREREQ, "pk", "prereq"))).get();
            }
            assertTrue(outAppearsWithin(bootstrap, "ORDER", Duration.ofSeconds(60)),
                    "once its dependency arrives, the held record drains — delivered in causal order, not lost");
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

    private static Topology stageX(ParsleyCoordination coordination) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(List.of(PREREQ, IN), Consumed.with(Serdes.String(), Serdes.String()))
                .process(ParsleyProcessors.builder(upperCaser())
                        .addBufferStore("parsley-x")
                        .addBuffers(List.of(PREREQ, IN), Serdes.String(), Serdes.String())
                        .withCoordination(coordination)
                        .build())
                .to(OUT, Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }

    /**
     * Y's own topology: the same {@code prereq + in -> out} shape as X, distinguished by a lower-cased
     * value instead of X's upper-cased one, so both apps' outputs coexist on {@code out} without
     * colliding with the test's exact-match assertions on X's "ORDER" value (see the class Javadoc for
     * why Y shares X's topics rather than an unrelated pair).
     */
    private static Topology stageY(ParsleyCoordination coordination) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(List.of(PREREQ, IN), Consumed.with(Serdes.String(), Serdes.String()))
                .process(ParsleyProcessors.builder(lowerCaser())
                        .addBufferStore("parsley-y")
                        .addBuffers(List.of(PREREQ, IN), Serdes.String(), Serdes.String())
                        .withCoordination(coordination)
                        .build())
                .to(OUT, Produced.with(Serdes.String(), Serdes.String()));
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

    private static ProcessorSupplier<String, String, String, String> lowerCaser() {
        return () -> new Processor<>() {
            private ProcessorContext<String, String> ctx;
            @Override public void init(ProcessorContext<String, String> context) { this.ctx = context; }
            @Override public void process(Record<String, String> record) {
                ctx.forward(record.withValue(record.value().toLowerCase(Locale.ROOT)));
            }
        };
    }

    private static void requestUntilCommitted(ParsleyCoordination a, ParsleyCoordination b, String bootstrap, long target) {
        await().atMost(Duration.ofSeconds(90)).pollInterval(Duration.ofSeconds(1)).until(() -> {
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

    private static boolean leaveExistsFor(String bootstrap, String appIdPrefix) {
        return readEpochEvents(bootstrap).stream()
                .anyMatch(e -> e instanceof ParsleyEpochEvent.Leave leave && leave.memberId().startsWith(appIdPrefix));
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

    /** Whether {@code value} appears on {@code out} within {@code window}. False if the window elapses first. */
    private static boolean outAppearsWithin(String bootstrap, String value, Duration window) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ConsumerConfig.GROUP_ID_CONFIG, "out-reader-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(List.of(OUT));
            long deadline = System.currentTimeMillis() + window.toMillis();
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
