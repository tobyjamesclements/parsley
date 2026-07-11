package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
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
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Proves topology-epoch coordination requires a genuine <strong>full mesh</strong> — every running
 * member's own subscriptions must cover the whole coordinated domain — rather than silently admitting
 * an incomplete one. App A ({@code t1 -> mid}) and app B ({@code mid -> out}) are separate {@link
 * KafkaStreams} applications with distinct {@code application.id}s, sharing one epoch-events log. This
 * ordinary two-stage pipeline is <em>not</em> a full mesh under the coordinated-domain definition: A
 * never consumes or produces {@code out}, and B never consumes or produces {@code t1} — each is missing
 * a domain topic only the other side touches. Neither app auto-wires a passthrough source for the
 * coordinate it is missing (that auto-wiring is a separate, not-yet-built piece of the full-mesh design)
 * so B's own startup self-check ({@code ParsleyProcessor#validateFullMeshCoverage}) must fail closed the
 * moment it joins an epoch where A has already declared {@code t1} — never silently proceed and let the
 * gap surface later as a data-path crash loop or a round that can never complete.
 */
@Testcontainers(disabledWithoutDocker = true)
class ParsleyCoordinationMultiAppIT {

    @Container
    private final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"));

    private static final String IN = "t1";
    private static final String MID = "mid";
    private static final String OUT = "out";
    private static final String EPOCH_EVENTS = "epoch-events";

    /**
     * App A joins alone first — its self-check passes, since at that moment the only declared member is
     * itself and its own {@code {t1, mid}} trivially covers the whole (so-far) domain. App B then joins
     * an epoch where A has already declared {@code t1}, growing the domain to {@code {t1, mid, out}}; B's
     * own {@code {mid, out}} does not cover {@code t1}, so B's startup self-check must fail closed rather
     * than silently proceed.
     *
     * Asserts app A reaches {@code RUNNING}, app B's {@code KafkaStreams} client reaches {@code ERROR}
     * (its task never got past {@code init()}), the captured startup exception names the missing
     * coordinate, and no record ever reaches {@code out} (B never actually ran).
     */
    @Test
    void bJoiningAnIncompleteMeshFailsClosedAtStartup() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        createTopics(bootstrap, IN, MID, OUT, EPOCH_EVENTS);
        String runId = UUID.randomUUID().toString().substring(0, 8);
        String appIdA = "dag-a-" + runId;
        String appIdB = "dag-b-" + runId;

        ParsleyCoordination coordinationA = ParsleyCoordination.create(EPOCH_EVENTS);
        ParsleyCoordination coordinationB = ParsleyCoordination.create(EPOCH_EVENTS);
        Path stateA = Files.createTempDirectory("parsley-dag-a");
        Path stateB = Files.createTempDirectory("parsley-dag-b");

        try (KafkaStreams appA = new KafkaStreams(stageA(coordinationA), streamsConfig(bootstrap, appIdA, stateA));
             KafkaStreams appB = new KafkaStreams(stageB(coordinationB), streamsConfig(bootstrap, appIdB, stateB))) {
            // App A joins alone first, so its own declaration is the whole domain when its self-check runs.
            appA.start();
            await().atMost(Duration.ofSeconds(60)).until(() -> appA.state() == KafkaStreams.State.RUNNING);

            // App B joins second, into a domain that already includes A's t1 — which B never declares.
            AtomicReference<Throwable> startupFailure = new AtomicReference<>();
            appB.setUncaughtExceptionHandler(exception -> {
                startupFailure.compareAndSet(null, exception);
                return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            });
            appB.start();
            await().atMost(Duration.ofSeconds(60)).until(() -> appB.state() == KafkaStreams.State.ERROR);

            Throwable failure = startupFailure.get();
            assertTrue(failure != null && messageChainContains(failure, "cover the coordinated domain"),
                    "B's startup failure must be the full-mesh self-check, not something else: " + failure);
            assertTrue(messageChainContains(failure, "missing: [" + IN + "]"),
                    "the failure must name the missing coordinate (" + IN + "): " + failure);

            assertFalse(awaitServedBriefly(bootstrap),
                    "no record can ever reach out — B's task never got past init()");
        } finally {
            coordinationA.close();
            coordinationB.close();
        }
    }

    private static Topology stageA(ParsleyCoordination coordination) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(IN, Consumed.with(Serdes.String(), Serdes.String()))
                .process(ParsleyProcessors.builder(mapper(v -> v.toUpperCase(Locale.ROOT)))
                        .addBufferStore("parsley-a")
                        .addBuffer(ParsleyBuffer.of(IN, Serdes.String(), Serdes.String()))
                        .withCoordination(coordination)
                        .build())
                .to(MID, Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }

    private static Topology stageB(ParsleyCoordination coordination) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(MID, Consumed.with(Serdes.String(), Serdes.String()))
                .process(ParsleyProcessors.builder(mapper(v -> "B:" + v))
                        .addBufferStore("parsley-b")
                        .addBuffer(ParsleyBuffer.of(MID, Serdes.String(), Serdes.String()))
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

    /** Whether {@code exception} or any cause in its chain has a message containing {@code substring}. */
    private static boolean messageChainContains(Throwable exception, String substring) {
        for (Throwable t = exception; t != null; t = t.getCause()) {
            if (t.getMessage() != null && t.getMessage().contains(substring)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether a {@code "B:"}-prefixed record ever reaches {@code out} within a short window — used to
     * assert a negative (B's task never got past {@code init()}, so it can never produce one), not to
     * await a positive outcome, so the window is short rather than the usual generous IT timeout.
     */
    private static boolean awaitServedBriefly(String bootstrap) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ConsumerConfig.GROUP_ID_CONFIG, "out-reader-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(List.of(OUT));
            long deadline = System.currentTimeMillis() + 5_000;
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
