package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Hard-crash mid-EOS-transaction: the broker-side owner of the aborted-transaction tear that the
 * crash-safety narrowing argument (docs/streams.md, "processing.guarantee") rests on and that the
 * topology simulator explicitly scopes out as broker-IT territory. A delegate processor throws after
 * a configured number of forwards while the EOS transaction carrying those forwards — and Parsley's
 * buffer/frontier/forwarded-index changelog writes — is still open, so the whole transaction aborts:
 * a real store tear, not a clean shutdown. A second incarnation with the same {@code application.id}
 * and state directory then restarts over the torn state; Kafka Streams re-fetches from the last
 * committed offsets and the aborted writes are invisible under {@code read_committed}, so the causal
 * stage must redeliver such that the sink shows each input's output exactly once, gap-free, in
 * causal order.
 *
 * <p><strong>Channels.</strong> {@code c1} and {@code c2} are the business input channels; {@code c3}
 * is the sink. The four inputs m1..m4 form a causal chain that alternates input channels
 * (m1 at {@code c1@0} &larr; m2 at {@code c2@0} &larr; m3 at {@code c1@1} &larr; m4 at {@code c2@1}),
 * so "causally ordered" is a real cross-channel constraint enforced by the engine's delivered
 * frontier — not single-partition offset order, which would hold vacuously.
 *
 * <p><strong>Crash-point sweep.</strong> One test per interesting boundary: before the first forward
 * (0 of 4), mid-batch (2 of 4), and after the last forward before any commit (4 of 4). Each test runs
 * against its own broker container (the {@code @Container} field is per-instance), so every sweep
 * iteration gets fresh topics and clean offsets.
 *
 * <p><strong>Why the throwing delegate and not {@link PoisonableBufferStore}.</strong> The tear under
 * test is the abort of a whole open transaction spanning sink writes and every Parsley changelog
 * write; a store-level poison seam faults one store operation inside a still-healthy task, which is a
 * different failure. Killing the stream thread from inside {@code process()} is the seam that
 * produces exactly the transaction abort the docs reason about.
 */
@Testcontainers(disabledWithoutDocker = true)
class ParsleyEosCrashRedeliveryIT {

    @Container
    private final KafkaContainer kafka =
            new KafkaContainer(ParsleyBrokerImage.get());

    private static final String C1 = "c1";   // business input channel; carries m1 (@0) and m3 (@1)
    private static final String C2 = "c2";   // business input channel; carries m2 (@0) and m4 (@1)
    private static final String C3 = "c3";   // sink the exactly-once assertions read

    private static final int INPUT_COUNT = 4;

    /**
     * Commit interval for the crashing incarnation, chosen so every forward up to the crash rides ONE
     * open EOS transaction. All four inputs are produced before the incarnation starts; a
     * single-instance application has no post-startup rebalance; and the stage never requests a
     * commit — so the only transaction-commit trigger is this interval, and the crash fires within
     * the first seconds of processing, far below it. The EOS producer's transaction timeout is raised
     * to match (Streams rejects a commit interval above it at startup); the broker never gets close
     * to timing the transaction out, because the crash lands in the same processing pass that opened
     * it, seconds at most after the first send.
     */
    private static final int SINGLE_TRANSACTION_COMMIT_INTERVAL_MS = 60_000;

    /** Commit interval for the restarted incarnation: short, so redelivered output commits (and so
     * becomes visible to the read_committed sink consumer) promptly. */
    private static final int REDELIVERY_COMMIT_INTERVAL_MS = 200;

    /**
     * The first incarnation crashes on delivery of m1, before its first forward, so the aborted
     * transaction holds only consumption and Parsley's changelog writes — the torn state is a node
     * that consumed but never visibly produced. A second incarnation with the same
     * {@code application.id} and state directory restarts over the tear.
     *
     * Asserts that nothing of the aborted transaction is visible to a read_committed sink consumer,
     * and that after the restart the sink shows m1..m4 exactly once each, gap-free, in causal order,
     * with no late duplicates.
     */
    @Test
    void redeliveryIsExactlyOnceAndCausallyOrderedAfterACrashBeforeTheFirstForward() throws Exception {
        assertExactlyOnceCausalRedeliveryAcrossACrash(0);
    }

    /**
     * The first incarnation crashes immediately after forwarding m2 — mid-batch, two of four forwards
     * done — so the aborted transaction holds real uncommitted sink writes alongside the changelog
     * writes. A second incarnation with the same {@code application.id} and state directory restarts
     * over the tear.
     *
     * Asserts that nothing of the aborted transaction is visible to a read_committed sink consumer,
     * and that after the restart the sink shows m1..m4 exactly once each, gap-free, in causal order,
     * with no late duplicates — in particular no duplicate m1/m2 from the aborted forwards.
     */
    @Test
    void redeliveryIsExactlyOnceAndCausallyOrderedAfterACrashMidBatch() throws Exception {
        assertExactlyOnceCausalRedeliveryAcrossACrash(2);
    }

    /**
     * The first incarnation crashes immediately after forwarding m4 — the last forward, with the
     * transaction fully written but not yet committed — the widest possible tear: every sink write
     * and every changelog write of the batch is rolled back at once. A second incarnation with the
     * same {@code application.id} and state directory restarts over the tear.
     *
     * Asserts that nothing of the aborted transaction is visible to a read_committed sink consumer,
     * and that after the restart the sink shows m1..m4 exactly once each, gap-free, in causal order,
     * with no late duplicates.
     */
    @Test
    void redeliveryIsExactlyOnceAndCausallyOrderedAfterACrashAfterTheLastForwardBeforeTheCommit()
            throws Exception {
        assertExactlyOnceCausalRedeliveryAcrossACrash(INPUT_COUNT);
    }

    /**
     * The shared sweep body: produce the causally-chained inputs, crash the first incarnation at
     * {@code crashAfterForwards} forwards inside its one open EOS transaction, assert the tear is
     * invisible, restart with the same {@code application.id} and state directory, and assert
     * exactly-once, gap-free, causally-ordered redelivery at the sink.
     */
    private void assertExactlyOnceCausalRedeliveryAcrossACrash(int crashAfterForwards) throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        createTopics(bootstrap, C1, C2, C3);
        String appId = "eos-crash-it-" + UUID.randomUUID();
        Path stateDir = Files.createTempDirectory("parsley-eos-crash");

        produceCausallyChainedInputs(bootstrap);
        CrashingForwarder.arm(crashAfterForwards);

        // Incarnation 1: the long commit interval keeps every forward inside one open transaction
        // (see SINGLE_TRANSACTION_COMMIT_INTERVAL_MS); the armed delegate kills the stream thread at
        // the crash point, the handler shuts the client down, and closing the client aborts the
        // transaction — sink writes AND buffer/frontier/forwarded-index changelog writes uncommitted.
        CausalStreams first = new CausalStreams(
                topology(), streamsConfig(bootstrap, appId, stateDir, SINGLE_TRANSACTION_COMMIT_INTERVAL_MS));
        first.setUncaughtExceptionHandler(exception ->
                StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT);
        try {
            first.start();
            await().atMost(Duration.ofSeconds(120)).until(() ->
                    first.state() == KafkaStreams.State.ERROR
                            || first.state() == KafkaStreams.State.NOT_RUNNING);
        } finally {
            first.close();
        }
        assertEquals(1, CrashingForwarder.firedCrashes(),
                "the first incarnation must crash exactly once, at the configured crash point");

        // The tear itself: the aborted transaction's output (if any forwards happened) must be
        // invisible under read_committed — everything the second incarnation's consumer later sees is
        // redelivery, so the exactly-once assertion below is not diluted by pre-crash output.
        try (KafkaConsumer<String, String> sink = new KafkaConsumer<>(consumerConfig(bootstrap))) {
            sink.subscribe(List.of(C3));
            assertNoBusinessRecords(sink, Duration.ofSeconds(5),
                    "no output of the crashed incarnation's aborted transaction may be visible to a "
                            + "read_committed sink consumer");
        }

        // Incarnation 2: same application.id and state directory. Streams discards the dirty local
        // state, restores from the changelogs (whose aborted writes are skipped), resumes from the
        // last committed input offsets, and redelivers the whole chain through the disarmed delegate.
        try (CausalStreams second = new CausalStreams(
                topology(), streamsConfig(bootstrap, appId, stateDir, REDELIVERY_COMMIT_INTERVAL_MS))) {
            second.start();
            try (KafkaConsumer<String, String> sink = new KafkaConsumer<>(consumerConfig(bootstrap))) {
                sink.subscribe(List.of(C3));
                List<String> delivered = pollBusinessValues(sink, INPUT_COUNT);
                assertEquals(List.of("m1", "m2", "m3", "m4"), delivered,
                        "after the crash/restart the sink must show each input exactly once, "
                                + "gap-free, in causal order");
                assertNoBusinessRecords(sink, Duration.ofSeconds(5),
                        "no late duplicate may follow the exactly-once redelivery");
            }
        }
    }

    /**
     * The delegate under test: forwards every business record unchanged and, while armed, throws once
     * at the configured crash point — before the first forward (crash point 0) or immediately after
     * the N-th forward — killing the stream thread with the EOS transaction that carries the forwards
     * still open. Streams constructs processor instances through the supplier, so the crash controls
     * are static; {@link #arm} resets them per test, and the throw disarms itself so the restarted
     * incarnation processes cleanly.
     */
    private static final class CrashingForwarder implements Processor<String, String, String, String> {

        private static final AtomicInteger crashAfterForwards = new AtomicInteger();
        private static final AtomicInteger forwards = new AtomicInteger();
        private static final AtomicInteger crashesFired = new AtomicInteger();
        private static final AtomicBoolean armed = new AtomicBoolean();

        private ProcessorContext<String, String> context;

        static void arm(int forwardsBeforeCrash) {
            crashAfterForwards.set(forwardsBeforeCrash);
            forwards.set(0);
            crashesFired.set(0);
            armed.set(true);
        }

        static int firedCrashes() {
            return crashesFired.get();
        }

        @Override
        public void init(ProcessorContext<String, String> context) {
            this.context = context;
        }

        @Override
        public void process(Record<String, String> record) {
            if (crashAfterForwards.get() == 0) {
                crashIfArmed("before the first forward");
            }
            context.forward(record);
            if (forwards.incrementAndGet() == crashAfterForwards.get()) {
                crashIfArmed("after forward " + crashAfterForwards.get());
            }
        }

        private static void crashIfArmed(String position) {
            if (armed.compareAndSet(true, false)) {
                crashesFired.incrementAndGet();
                throw new RuntimeException(
                        "deliberate test crash " + position + " with the EOS transaction still open");
            }
        }
    }

    private static CausalTopology topology() {
        return new CausalStreamsBuilder()
                .stream(List.of(C1, C2), Serdes.String(), Serdes.String())
                .process(crashingForwarder())
                .to(C3, Serdes.String(), Serdes.String())
                .build();
    }

    private static ProcessorSupplier<String, String, String, String> crashingForwarder() {
        return CrashingForwarder::new;
    }

    /**
     * Produces the four-record causal chain, each record stamped (via the edge API) as depending on
     * the coordinate of the previous one, alternating between the two input channels:
     * m1 at {@code c1@0}, m2 at {@code c2@0} (depends on m1), m3 at {@code c1@1} (depends on m2),
     * m4 at {@code c2@1} (depends on m3). Sends are sequential and acknowledged, so the coordinates
     * are deterministic.
     */
    private static void produceCausallyChainedInputs(String bootstrap) throws Exception {
        Properties resolverProps = new Properties();
        resolverProps.put("bootstrap.servers", bootstrap);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig(bootstrap))) {
            producer.send(CausalClock.empty()
                    .stamp(new ProducerRecord<>(C1, "k", "m1"))).get();
            producer.send(CausalClock.using(resolverProps)
                    .observe(new ConsumerRecord<>(C1, 0, 0L, "k", "m1"))
                    .stamp(new ProducerRecord<>(C2, "k", "m2"))).get();
            producer.send(CausalClock.using(resolverProps)
                    .observe(new ConsumerRecord<>(C2, 0, 0L, "k", "m2"))
                    .stamp(new ProducerRecord<>(C1, "k", "m3"))).get();
            producer.send(CausalClock.using(resolverProps)
                    .observe(new ConsumerRecord<>(C1, 0, 1L, "k", "m3"))
                    .stamp(new ProducerRecord<>(C2, "k", "m4"))).get();
        }
    }

    /** Polls until {@code count} business (non-null-message) values have been read, returning them in
     * consumption order. */
    private static List<String> pollBusinessValues(KafkaConsumer<String, String> consumer, int count) {
        List<String> values = new ArrayList<>();
        await().atMost(Duration.ofSeconds(90)).until(() -> {
            for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(300))) {
                if (!CausalClock.isNullMessage(record)) {
                    values.add(record.value());
                }
            }
            return values.size() >= count;
        });
        return values;
    }

    /** Polls for the whole {@code window} and fails with {@code message} if any business
     * (non-null-message) record arrives. */
    private static void assertNoBusinessRecords(
            KafkaConsumer<String, String> consumer, Duration window, String message) {
        long deadline = System.currentTimeMillis() + window.toMillis();
        while (System.currentTimeMillis() < deadline) {
            for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(300))) {
                assertTrue(CausalClock.isNullMessage(record),
                        message + " (saw a business record with value '" + record.value() + "')");
            }
        }
    }

    private static Properties streamsConfig(
            String bootstrap, String appId, Path stateDir, int commitIntervalMs) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, commitIntervalMs);
        // Streams rejects commit.interval.ms above the producer transaction timeout, so the timeout
        // tracks the interval (see SINGLE_TRANSACTION_COMMIT_INTERVAL_MS for why it is never reached).
        props.put(StreamsConfig.producerPrefix(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG), commitIntervalMs);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toAbsolutePath().toString());
        return props;
    }

    private static Map<String, Object> producerConfig(String bootstrap) {
        return Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    }

    /** The sink consumer's configuration; {@code read_committed} explicitly, so an aborted
     * transaction's output is invisible and the exactly-once assertions test the delivery claim, not
     * consumer defaults. */
    private static Map<String, Object> consumerConfig(String bootstrap) {
        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ConsumerConfig.GROUP_ID_CONFIG, "out-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    }

    private static void createTopics(String bootstrap, String... topics) throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap))) {
            Set<NewTopic> newTopics = new HashSet<>();
            for (String topic : topics) {
                newTopics.add(new NewTopic(topic, 1, (short) 1));
            }
            admin.createTopics(newTopics).all().get();
        }
    }
}
