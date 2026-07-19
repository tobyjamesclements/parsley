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
import java.util.ArrayList;
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
 * a domain topic only the other side touches.
 *
 * <p>{@link #bJoiningAnIncompleteMeshFailsClosedAtStartup} proves the fail-closed side of this, over the
 * low-level {@link ParsleyProcessorSupplier} API with no {@code parsley.coordination.domain-topics} configured:
 * B's own startup self-check ({@code ParsleyProcessor#validateFullMeshCoverage}) must fail closed the
 * moment it joins an epoch where A has already declared {@code t1}. {@link
 * #domainTopicsAutoWiresPassthroughSoBothAppsRunAndDeliverEndToEnd} proves the other side: with {@code
 * parsley.coordination.domain-topics} configured (over the higher-level {@link CausalStreamsBuilder}/
 * {@link CausalStreams} API, which is what actually wires it), the exact same otherwise-incomplete
 * two-stage pipeline auto-wires a passthrough source for each app's missing coordinate, both apps reach
 * {@code RUNNING}, and a real record still flows end to end through the genuine business path.
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

    /**
     * The exact same two-stage pipeline shape as {@link #bJoiningAnIncompleteMeshFailsClosedAtStartup} —
     * app A ({@code t1 -> mid}), app B ({@code mid -> out}), neither directly consuming or producing the
     * other's missing coordinate — but built through {@link CausalStreamsBuilder}/{@link CausalStreams}
     * with {@code parsley.coordination.domain-topics = t1,mid,out} configured on both. Each app's {@link
     * CausalTopology#assemble} auto-wires a passthrough source for the one domain topic it does not
     * otherwise touch ({@code t1} for B, {@code out} for A), so both members' declared subscriptions cover
     * the whole domain and the startup self-check that failed the other test now passes for both.
     *
     * Asserts both apps reach {@code RUNNING}, and a record produced to {@code t1} still flows through the
     * genuine business transforms in both stages (A upper-cases, B prefixes {@code "B:"}) all the way to
     * {@code out} — proving the auto-wired passthrough sources coexist correctly with the real delivery
     * path rather than merely avoiding a startup crash.
     */
    @Test
    void domainTopicsAutoWiresPassthroughSoBothAppsRunAndDeliverEndToEnd() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        createTopics(bootstrap, IN, MID, OUT, EPOCH_EVENTS);
        String runId = UUID.randomUUID().toString().substring(0, 8);
        String appIdA = "dag-a-domain-" + runId;
        String appIdB = "dag-b-domain-" + runId;
        Path stateA = Files.createTempDirectory("parsley-dag-a-domain");
        Path stateB = Files.createTempDirectory("parsley-dag-b-domain");
        Properties roster = memberApps(appIdA, appIdB);

        try (CausalStreams appA = new CausalStreams(stageATopology(), causalStreamsConfig(bootstrap, appIdA, stateA, roster));
             CausalStreams appB = new CausalStreams(stageBTopology(), causalStreamsConfig(bootstrap, appIdB, stateB, roster))) {
            appA.start();
            appB.start();
            await().atMost(Duration.ofSeconds(60)).until(() -> appA.state() == KafkaStreams.State.RUNNING);
            await().atMost(Duration.ofSeconds(60)).until(() -> appB.state() == KafkaStreams.State.RUNNING);

            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig(bootstrap))) {
                producer.send(stampEmptyDeps(new ProducerRecord<>(IN, "k", "hello"))).get();
            }

            assertTrue(awaitServed(bootstrap, "B:HELLO"),
                    "a record produced to t1 must flow t1 -> mid -> out through both real business stages");
        }
    }

    /**
     * A genuine two-node <strong>cycle</strong> across two separate real applications — app A ({@code
     * t1, from-b -> from-a}) and app B ({@code from-a -> from-b}), each its own {@link KafkaStreams}
     * instance with its own {@code application.id} and its own {@link Topology}, so (unlike a single
     * shared {@code Topology}, where Kafka Streams allows only one source node per topic) there is no
     * topology-wiring conflict between A's direct consumption of {@code t1} and — via {@code
     * parsley.coordination.domain-topics = t1,from-a,from-b} — B's passthrough-wired visibility into the
     * same topic. A absorbs (does not re-forward) a record sourced from {@code from-b}, so the cycle
     * terminates after one round trip.
     *
     * <p>This is the headline capability the whole max-merge redesign (PR1) plus clock-invisible markers
     * (PR2) plus passthrough auto-wiring (PR5) plus {@link ParsleyEngine}'s own-coordinate stripping
     * (added during this same PR6 verification pass, after a {@link TopologyTestDriver} single-node
     * self-loop test — see {@code CausalCyclicTopologyTest} — caught a genuine infinite watermark loop)
     * together exist to enable: a topology where a node consumes both an ancestor and its own descendant,
     * which the old {@code ParsleyVectorClock#intersectMin} gate made a documented, permanent deadlock.
     *
     * Asserts both apps reach {@code RUNNING} and a record produced to {@code t1} flows all the way
     * around the cycle to {@code from-b} exactly once.
     */
    @Test
    void twoNodeCycleAcrossSeparateAppsDeliversEndToEnd() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        String fromA = "from-a";
        String fromB = "from-b";
        createTopics(bootstrap, IN, fromA, fromB, EPOCH_EVENTS);
        String runId = UUID.randomUUID().toString().substring(0, 8);
        String appIdA = "cycle-a-" + runId;
        String appIdB = "cycle-b-" + runId;
        Path stateA = Files.createTempDirectory("parsley-cycle-a");
        Path stateB = Files.createTempDirectory("parsley-cycle-b");

        CausalTopology aTopology = new CausalStreamsBuilder()
                .stream(List.of(IN, fromB), Serdes.String(), Serdes.String())
                .process(cycleStage("t1", "A:"))
                .to("from-a-sink", fromA, Serdes.String(), Serdes.String())
                .build();
        CausalTopology bTopology = new CausalStreamsBuilder()
                .stream(fromA, Serdes.String(), Serdes.String())
                .process(cycleStage(null, "B:"))
                .to("from-b-sink", fromB, Serdes.String(), Serdes.String())
                .build();

        Properties domainConfig = cycleConfig(IN + "," + fromA + "," + fromB);
        domainConfig.put(ParsleyConfig.COORDINATION_MEMBER_APPS, appIdA + "," + appIdB);
        try (CausalStreams appA = new CausalStreams(aTopology,
                     causalStreamsConfig(bootstrap, appIdA, stateA, domainConfig));
             CausalStreams appB = new CausalStreams(bTopology,
                     causalStreamsConfig(bootstrap, appIdB, stateB, domainConfig))) {
            appA.start();
            appB.start();
            await().atMost(Duration.ofSeconds(60)).until(() -> appA.state() == KafkaStreams.State.RUNNING);
            await().atMost(Duration.ofSeconds(60)).until(() -> appB.state() == KafkaStreams.State.RUNNING);

            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig(bootstrap))) {
                producer.send(stampEmptyDeps(new ProducerRecord<>(IN, "k", "hello"))).get();
            }

            assertTrue(awaitServedOn(bootstrap, fromB, "B:A:hello"),
                    "a record produced to t1 must flow all the way around the cycle to from-b");
        }
    }

    /**
     * A cyclic founding cohort {@code {A,B}} seals genesis together, then the running cycle carries on
     * serving across a further, explicit epoch boundary — the cyclic counterpart of the linear cohort in
     * {@code ParsleyCoordinationJoinerIT}.
     *
     * <p>Both the absorbing side A ({@code t1, from-b -> from-a}) and the closing side B ({@code from-a ->
     * from-b}) declare the founding roster {@code {A,B}} and come up together, each consuming at the empty
     * genesis floor; genesis does not commit until both founders have declared their full task set, then it
     * seals the cut over the whole cohort and closes the cycle. A record produced to {@code t1} flows all
     * the way around ({@code t1 -> A -> from-a -> B -> from-b -> A}, where A absorbs the return leg). The
     * running cycle is then evolved through an explicit {@link CausalStreams#requestEpochTransition()} and a
     * second record still serves across the new boundary.
     *
     * Asserts genesis commits over the whole cohort, a record produced to {@code t1} reaches {@code from-b}
     * around the cycle, and a second record still serves after a later explicit epoch transition.
     */
    @Test
    void aCyclicCohortSealsGenesisTogetherAndCarriesOnAcrossAnEpochTransition() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        String fromA = "from-a";
        String fromB = "from-b";
        createTopics(bootstrap, IN, fromA, fromB, EPOCH_EVENTS);
        String runId = UUID.randomUUID().toString().substring(0, 8);
        Path stateA = Files.createTempDirectory("parsley-join-cycle-a");
        Path stateB = Files.createTempDirectory("parsley-join-cycle-b");

        CausalTopology aTopology = new CausalStreamsBuilder()
                .stream(List.of(IN, fromB), Serdes.String(), Serdes.String())
                .process(cycleStage("t1", "A:"))
                .to("from-a-sink", fromA, Serdes.String(), Serdes.String())
                .build();
        CausalTopology bTopology = new CausalStreamsBuilder()
                .stream(fromA, Serdes.String(), Serdes.String())
                .process(cycleStage(null, "B:"))
                .to("from-b-sink", fromB, Serdes.String(), Serdes.String())
                .build();

        String appIdA = "join-cycle-a-" + runId;
        String appIdB = "join-cycle-b-" + runId;
        Properties domainConfig = cycleConfig(IN + "," + fromA + "," + fromB);
        domainConfig.put(ParsleyConfig.COORDINATION_MEMBER_APPS, appIdA + "," + appIdB);
        try (CausalStreams appA = new CausalStreams(aTopology,
                     causalStreamsConfig(bootstrap, appIdA, stateA, domainConfig));
             CausalStreams appB = new CausalStreams(bTopology,
                     causalStreamsConfig(bootstrap, appIdB, stateB, domainConfig))) {
            // The cyclic cohort {A,B} seals genesis together: both founders come up and consume at the empty
            // genesis floor, and genesis does not commit until both have declared their full task set — then
            // it seals the cut over the whole cohort and the closed cycle serves.
            appA.start();
            appB.start();
            await().atMost(Duration.ofSeconds(60)).until(() -> appA.state() == KafkaStreams.State.RUNNING);
            await().atMost(Duration.ofSeconds(60)).until(() -> appB.state() == KafkaStreams.State.RUNNING);
            await().atMost(Duration.ofSeconds(90)).until(() -> latestCommittedEpoch(bootstrap) >= 1L);

            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig(bootstrap))) {
                producer.send(stampEmptyDeps(new ProducerRecord<>(IN, "k", "hello"))).get();
            }
            assertTrue(awaitServedOn(bootstrap, fromB, "B:A:hello"),
                    "once genesis seals the cyclic cohort, a record must flow around the closed cycle to from-b");

            // Evolve the running two-node cycle through a further, explicit epoch transition and confirm it
            // keeps serving across the new boundary.
            long epochBeforeTransition = latestCommittedEpoch(bootstrap);
            requestUntilCommitted(appA, bootstrap, epochBeforeTransition + 1);

            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig(bootstrap))) {
                producer.send(stampEmptyDeps(new ProducerRecord<>(IN, "k", "world"))).get();
            }
            assertTrue(awaitServedOn(bootstrap, fromB, "B:A:world"),
                    "the cycle must keep serving after an explicit epoch transition over the running members");
        }
    }

    /**
     * A stage that forwards a record sourced from {@code forwardOnlyFromTopic} (or every record, if
     * {@code null}) with {@code prefix} prepended, and absorbs (never re-forwards) anything else — the
     * termination strategy for a cyclic topology: without an absorbing side, the business cycle would
     * circulate forever, which is a workload concern this gate deliberately does not suppress (see
     * {@code ParsleyProcessor}'s class Javadoc).
     */
    private static ProcessorSupplier<String, String, String, String> cycleStage(
            String forwardOnlyFromTopic, String prefix) {
        return () -> new Processor<>() {
            private ProcessorContext<String, String> ctx;
            @Override public void init(ProcessorContext<String, String> context) { this.ctx = context; }
            @Override public void process(Record<String, String> record) {
                String sourceTopic = ctx.recordMetadata().map(org.apache.kafka.streams.processor.api.RecordMetadata::topic).orElse("");
                if (forwardOnlyFromTopic == null || forwardOnlyFromTopic.equals(sourceTopic)) {
                    ctx.forward(record.withValue(prefix + record.value()));
                }
            }
        };
    }

    private static Properties cycleConfig(String domainTopics) {
        Properties props = new Properties();
        props.put(ParsleyConfig.COORDINATION_DOMAIN_TOPICS, domainTopics);
        return props;
    }

    /** The {@code parsley.coordination.member-apps} roster as an overlay {@link Properties} — the
     * authoritative membership every app in a multi-app domain must declare identically. */
    private static Properties memberApps(String... appIds) {
        Properties props = new Properties();
        props.put(ParsleyConfig.COORDINATION_MEMBER_APPS, String.join(",", appIds));
        return props;
    }

    /** As {@link #causalStreamsConfig(String, String, Path)}, overlaying {@code extra} on top. */
    private static Properties causalStreamsConfig(String bootstrap, String appId, Path stateDir, Properties extra) {
        Properties props = causalStreamsConfig(bootstrap, appId, stateDir);
        props.putAll(extra);
        return props;
    }

    /** As {@link #awaitServed(String, String)}, against an explicitly named topic rather than {@link #OUT}. */
    private static boolean awaitServedOn(String bootstrap, String topic, String expected) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ConsumerConfig.GROUP_ID_CONFIG, "cycle-reader-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + 30_000;
            while (System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(300))) {
                    if (expected.equals(record.value())) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    private static CausalTopology stageATopology() {
        return new CausalStreamsBuilder()
                .stream(IN, Serdes.String(), Serdes.String())
                .process(mapper(v -> v.toUpperCase(Locale.ROOT)))
                .to("mid-sink", MID, Serdes.String(), Serdes.String())
                .build();
    }

    private static CausalTopology stageBTopology() {
        return new CausalStreamsBuilder()
                .stream(MID, Serdes.String(), Serdes.String())
                .process(mapper(v -> "B:" + v))
                .to("out-sink", OUT, Serdes.String(), Serdes.String())
                .build();
    }

    private static Properties causalStreamsConfig(String bootstrap, String appId, Path stateDir) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 200);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toAbsolutePath().toString());
        props.put(ParsleyConfig.COORDINATION_EPOCH_EVENTS_TOPIC, EPOCH_EVENTS);
        props.put(ParsleyConfig.COORDINATION_DOMAIN_TOPICS, IN + "," + MID + "," + OUT);
        return props;
    }

    /**
     * Requests epoch transitions on {@code app} until the log shows a commit at or beyond {@code target}.
     * A single request may race the round or coalesce into an already-open one, so it retries each tick —
     * the same pattern {@code ParsleyCoordinationIT} uses.
     */
    private static void requestUntilCommitted(CausalStreams app, String bootstrap, long target) {
        await().atMost(Duration.ofSeconds(90)).until(() -> {
            if (latestCommittedEpoch(bootstrap) >= target) {
                return true;
            }
            try {
                app.requestEpochTransition();
            } catch (IllegalStateException notReadyYet) {
                // No local member has initialised coordination yet; retry on the next tick.
            }
            return false;
        });
    }

    /** The highest committed epoch id on the log, or {@code 0} if none has committed yet. */
    private static long latestCommittedEpoch(String bootstrap) {
        long max = 0;
        for (ParsleyEpochEvent event : readEpochEvents(bootstrap)) {
            if (event instanceof ParsleyEpochEvent.EpochCommitted committed) {
                max = Math.max(max, committed.epochId());
            }
        }
        return max;
    }

    /** Reads the whole epoch-events log from the beginning and decodes every record. */
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
        record.headers().add(ParsleyHeader.CAUSAL_DEPENDENCIES, ParsleyVectorClock.empty().toBytes());
        return record;
    }

    private static Map<String, Object> producerConfig(String bootstrap) {
        return Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    }

    /** Awaits a record whose value equals {@code expected} arriving on {@code out}, up to the IT timeout. */
    private static boolean awaitServed(String bootstrap, String expected) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ConsumerConfig.GROUP_ID_CONFIG, "out-reader-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(List.of(OUT));
            long deadline = System.currentTimeMillis() + 30_000;
            while (System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(300))) {
                    if (expected.equals(record.value())) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    private static Topology stageA(ParsleyCoordination coordination) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(IN, Consumed.with(Serdes.String(), Serdes.String()))
                .process(ParsleyProcessorSupplier.builder(mapper(v -> v.toUpperCase(Locale.ROOT)))
                        .addBufferStore("parsley-a")
                        .addSource(new ParsleySource<>(IN, Serdes.String(), Serdes.String()))
                        .withCoordination(coordination)
                        .build())
                .to(MID, Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }

    private static Topology stageB(ParsleyCoordination coordination) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(MID, Consumed.with(Serdes.String(), Serdes.String()))
                .process(ParsleyProcessorSupplier.builder(mapper(v -> "B:" + v))
                        .addBufferStore("parsley-b")
                        .addSource(new ParsleySource<>(MID, Serdes.String(), Serdes.String()))
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
