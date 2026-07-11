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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A graceful {@link ParsleyCoordination#leave() leave()} drains before it departs. App X (prereq + in ->
 * out) holds a record on {@code in} that depends on {@code prereq@0}; it then calls {@code leave()}. The
 * call must <strong>block</strong> — appending no {@code Leave} — while the record is still buffered, so a
 * decommission never strands un-drained work. Once {@code prereq@0} is produced the held record drains to
 * {@code out}, and only then does {@code leave()} append the {@code Leave} and return. The buffered record
 * is delivered, not lost.
 *
 * <p>App Y — the survivor keeping the domain alive while X departs — consumes the <em>same</em> topics
 * as X (also {@code prereq + in -> out}, distinguished by a lower-cased value instead of X's
 * upper-cased one) rather than an unrelated {@code yin -> yout} pair, so each app's own declared
 * subscriptions trivially cover the whole coordinated domain — a genuine full mesh (see {@link
 * ParsleyEpochLog#isFullMeshSatisfied()}) — without needing the passthrough-source auto-wiring an
 * unrelated topic pair would require, which does not exist yet. This is purely a topology-shape choice
 * for the test; it does not change what member-lifecycle behaviour is under test.
 */
@Testcontainers(disabledWithoutDocker = true)
class ParsleyCoordinationLeaveDrainIT {

    @Container
    private final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"));

    private static final String PREREQ = "prereq";
    private static final String IN = "in";
    private static final String OUT = "out";
    private static final String EPOCH_EVENTS = "epoch-events";

    /**
     * X holds an {@code in} record depending on {@code prereq@0}; survivor Y keeps the domain alive. X calls
     * {@code leave()} on a background thread: it blocks (no {@code Leave} on the log) while the record is
     * buffered. Producing {@code prereq@0} drains the record to {@code out}; {@code leave()} then appends the
     * {@code Leave} and returns. Asserts the held record was delivered before departure and the member left.
     */
    @Test
    void aGracefulLeaveDrainsTheBufferBeforeDeparting() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        createTopics(bootstrap, PREREQ, IN, OUT, EPOCH_EVENTS);
        String runId = UUID.randomUUID().toString().substring(0, 8);
        String appIdX = "leave-drain-x-" + runId;
        String appIdY = "leave-drain-y-" + runId;

        ParsleyCoordination coordinationX = ParsleyCoordination.create(EPOCH_EVENTS);
        ParsleyCoordination coordinationY = ParsleyCoordination.create(EPOCH_EVENTS);
        Path stateX = Files.createTempDirectory("parsley-leave-drain-x");
        Path stateY = Files.createTempDirectory("parsley-leave-drain-y");

        KafkaStreams appX = new KafkaStreams(stageX(coordinationX), streamsConfig(bootstrap, appIdX, stateX));
        KafkaStreams appY = new KafkaStreams(stageY(coordinationY), streamsConfig(bootstrap, appIdY, stateY));
        Thread leaver = null;
        try {
            appX.start();
            appY.start();
            await().atMost(Duration.ofSeconds(60)).until(() ->
                    appX.state() == KafkaStreams.State.RUNNING && appY.state() == KafkaStreams.State.RUNNING);

            // Establish an epoch so both X and Y are running members (Y needs no separate "kick" — it
            // shares X's input topics, so it is already actively consuming).
            requestUntilCommitted(coordinationX, coordinationY, bootstrap, 1L);

            // Buffer a record in X: an `in` record depending on prereq@0 (never yet produced) is held.
            Properties resolverProps = new Properties();
            resolverProps.put("bootstrap.servers", bootstrap);
            CausalDependencies orderDeps = CausalDependencies.using(resolverProps)
                    .observe(new ConsumerRecord<>(PREREQ, 0, 0L, "pk", "prereq"));
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig(bootstrap))) {
                producer.send(orderDeps.stamp(new ProducerRecord<>(IN, "k", "order"))).get();
                // A follow-up `in` record with no dependencies, which X forwards immediately once consumed.
                // Its arrival at `out` as MARKER confirms X has consumed past `order` — i.e. `order` is now
                // buffered — so the leave() below genuinely races an occupied buffer, not an empty one (the
                // "stop feeding it first" contract; otherwise leave could see a momentarily-empty buffer).
                producer.send(CausalDependencies.empty().stamp(new ProducerRecord<>(IN, "mk", "marker"))).get();
            }
            assertTrue(outAppearsWithin(bootstrap, "MARKER", Duration.ofSeconds(60)),
                    "the marker record confirms X has consumed past — and so is buffering — the held `order` record");

            // Call leave() on a background thread: phase 1 blocks until X's buffer drains.
            Thread leaverThread = new Thread(coordinationX::leave, "leave-drain");
            leaver = leaverThread;
            leaverThread.start();

            // While prereq@0 is unproduced the record stays buffered, so leave() must NOT have departed: no
            // Leave for X on the log, and the call is still blocked.
            Thread.sleep(6000);
            assertTrue(leaverThread.isAlive(), "leave() blocks while X's buffered record is not yet drained");
            assertFalse(leaveExistsFor(bootstrap, appIdX),
                    "leave() must not remove X while it still holds an un-drained record");

            // Produce prereq@0: the held record drains to out (delivered before departure, not lost).
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig(bootstrap))) {
                producer.send(CausalDependencies.empty().stamp(new ProducerRecord<>(PREREQ, "pk", "prereq"))).get();
            }
            assertTrue(outAppearsWithin(bootstrap, "ORDER", Duration.ofSeconds(60)),
                    "the held record drains to out — delivered before departure, not lost");

            // leave() now proceeds: it appends the Leave and returns.
            leaverThread.join(Duration.ofSeconds(60).toMillis());
            assertFalse(leaverThread.isAlive(), "leave() returns once X has drained and been removed");
            await().atMost(Duration.ofSeconds(30)).until(() -> leaveExistsFor(bootstrap, appIdX));
            assertTrue(leaveExistsFor(bootstrap, appIdX), "leave() appended a Leave for the drained member");
        } finally {
            if (leaver != null) {
                leaver.join(Duration.ofSeconds(5).toMillis());
            }
            appX.close(Duration.ofSeconds(30));
            appY.close(Duration.ofSeconds(30));
            coordinationX.close();
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
     * colliding with the test's exact-match assertions on X's "ORDER"/"MARKER" values (see the class
     * Javadoc for why Y shares X's topics rather than an unrelated pair).
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
