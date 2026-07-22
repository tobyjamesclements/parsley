package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * T2.1 prototype (redesign doc, O1 and task T2.1): validates the residual mechanics of the
 * own-output acknowledgement design against a real broker, before T2.2 builds the production
 * registry. Three claims from the design need empirical confirmation:
 *
 * <ol>
 *   <li>a {@link ProducerInterceptor} named through the public {@code producer.} Streams config
 *       prefix reaches the EOS stream producer, and its callbacks carry the exact committed
 *       (topic, partition, offset) coordinate on the producer's network thread — not the stream
 *       thread — so marshalling to the owning task needs a concurrent registry;</li>
 *   <li>{@code KafkaProducer.flush()} (the call {@code RecordCollector.flush()} delegates to)
 *       returns only after every prior send's callback has completed, so a stamp taken after a
 *       flush can never miss an own-output coordinate from before it;</li>
 *   <li>aborting a transaction while sends are unacknowledged fails each of them with exactly one
 *       callback, releasing a latch fed by the callbacks — the crossing wait cannot hang across an
 *       abort, and the failure outcome is visible so the wait dies with the transaction (T3.0 A8).</li>
 * </ol>
 */
@Testcontainers(disabledWithoutDocker = true)
class ParsleyProducerAckMechanicsIT {

    @Container
    private final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"));

    private static final String C1 = "C1";
    private static final String C2 = "C2";

    /** Acknowledgements in arrival order; written by producer threads, read by the test thread. */
    private static final ConcurrentLinkedQueue<ObservedAck> ACKS = new ConcurrentLinkedQueue<>();
    private static final AtomicInteger SENDS = new AtomicInteger();
    private static final AtomicInteger CONFIGURE_CALLS = new AtomicInteger();
    private static final ConcurrentLinkedQueue<String> CLIENT_IDS = new ConcurrentLinkedQueue<>();
    private static volatile @Nullable CountDownLatch ackLatch;

    @BeforeEach
    void resetRegistry() {
        ACKS.clear();
        SENDS.set(0);
        CONFIGURE_CALLS.set(0);
        CLIENT_IDS.clear();
        ackLatch = null;
    }

    /** One producer acknowledgement as observed: coordinate, outcome, and the delivering thread. */
    record ObservedAck(String topic, int partition, long offset,
                       @Nullable Exception exception, String threadName) {}

    /**
     * The prototype of the D2 registry seam: installed purely through producer configuration (no
     * {@code *.internals.*} types), it publishes every acknowledgement into the static registry
     * above. The production version (T2.2) folds these into the {@code ownOutputs} clock instead.
     */
    public static final class AckRegistryInterceptor implements ProducerInterceptor<Object, Object> {
        @Override
        public void configure(Map<String, ?> configs) {
            CONFIGURE_CALLS.incrementAndGet();
            Object clientId = configs.get("client.id");
            if (clientId != null) {
                CLIENT_IDS.add(clientId.toString());
            }
        }

        @Override
        public ProducerRecord<Object, Object> onSend(ProducerRecord<Object, Object> producerRecord) {
            SENDS.incrementAndGet();
            return producerRecord;
        }

        @Override
        public void onAcknowledgement(@Nullable RecordMetadata metadata, @Nullable Exception exception) {
            ACKS.add(new ObservedAck(
                    metadata == null ? "" : metadata.topic(),
                    metadata == null ? -1 : metadata.partition(),
                    metadata == null ? -1L : metadata.offset(),
                    exception,
                    Thread.currentThread().getName()));
            CountDownLatch latch = ackLatch;
            if (latch != null) {
                latch.countDown();
            }
        }

        @Override
        public void close() {
        }
    }

    /**
     * Validates the callback-to-task marshalling path in the real Streams threading model: the
     * class named via the public {@code producer.} config prefix is instantiated by the EOS stream
     * producer, its acknowledgement for a stamped business forward names the exact committed
     * (topic, partition, offset) coordinate, and the callback runs on the producer's network
     * thread rather than the stream thread, so the T2.2 registry must be concurrent.
     *
     * Asserts the C2 ack matches the delivered record's coordinate, arrived off the stream thread
     * on a producer network thread, and that configure() exposed a client.id usable for routing.
     */
    @Test
    void streamsProducerConfigDeliversSinkAckCoordinatesOffTheStreamThread() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        createTopics(bootstrap, C1, C2);

        AtomicReference<String> streamThread = new AtomicReference<>("");
        CausalTopology topology = new CausalStreamsBuilder()
                .stream(List.of(C1), Serdes.String(), Serdes.String())
                .process(threadRecordingUpperCaser(streamThread))
                .to(C2, Serdes.String(), Serdes.String())
                .build();

        Properties props = streamsConfig(bootstrap);
        props.put("producer." + ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                AckRegistryInterceptor.class.getName());

        try (CausalStreams streams = new CausalStreams(topology, props)) {
            streams.start();

            try (KafkaProducer<String, String> producer =
                         new KafkaProducer<>(plainProducerConfig(bootstrap))) {
                producer.send(CausalClock.empty()
                        .stamp(new ProducerRecord<>(C1, "k", "ping"))).get();
            }

            ConsumerRecord<String, String> delivered = awaitOutputRecord(bootstrap, "PING");
            await().atMost(Duration.ofSeconds(30))
                    .until(() -> ackFor(C2, delivered.offset()) != null);
            ObservedAck ack = ackFor(C2, delivered.offset());
            assertNotNull(ack, "the sink send's ack must be observable through the registry");
            assertEquals(delivered.partition(), ack.partition(),
                    "the ack must name the exact partition the record landed on");
            assertNull(ack.exception(), "a committed business forward must ack successfully");
            assertNotEquals(streamThread.get(), ack.threadName(),
                    "acks arrive off the stream thread — the registry must be readable across threads");
            assertTrue(ack.threadName().contains("kafka-producer-network-thread"),
                    "acks are delivered on the producer network thread, was: " + ack.threadName());
            assertTrue(CONFIGURE_CALLS.get() >= 1,
                    "the stream producer must configure the registered class");
            assertFalse(CLIENT_IDS.isEmpty(),
                    "configure() must expose a client.id the registry can key marshalling on");
        }
    }

    /**
     * Validates ack-completeness at flush: {@code KafkaProducer.flush()} — the call
     * {@code RecordCollector.flush()} delegates to inside a Streams commit — returns only after
     * the callback of every previously sent record has completed, exercised over five
     * transactional rounds. This is what lets a stamp taken after a flush trust that no
     * own-output coordinate from before the flush is still in flight.
     *
     * Asserts after each flush the registry holds exactly one successful ack per send, and at the
     * end that offsets are distinct real broker offsets with no duplicate callbacks.
     */
    @Test
    void flushReturnsOnlyAfterEveryAckCallbackHasCompleted() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        createTopics(bootstrap, C1);

        try (KafkaProducer<String, String> producer =
                     new KafkaProducer<>(transactionalProducerConfig(bootstrap, "c21-flush"))) {
            producer.initTransactions();
            int total = 0;
            for (int round = 1; round <= 5; round++) {
                producer.beginTransaction();
                for (int i = 0; i < 200; i++) {
                    producer.send(new ProducerRecord<>(C1, "k" + i, "round-" + round));
                }
                total += 200;
                producer.flush();
                assertEquals(total, ACKS.size(),
                        "flush() must not return before every ack callback has run (round " + round + ")");
                producer.commitTransaction();
            }
            assertEquals(total, SENDS.get(), "every send must pass through onSend exactly once");
            assertEquals((long) total, ACKS.stream().map(ObservedAck::offset).distinct().count(),
                    "every ack must carry a distinct real broker offset");
            assertTrue(ACKS.stream().allMatch(a -> a.exception() == null && a.offset() >= 0),
                    "all acks in committed rounds must be successes with real offsets");
        }
    }

    /**
     * Validates latch behaviour when a transaction aborts while acks are outstanding (the
     * crossing-wait scenario, T3.0 A8): records parked in the accumulator by a long linger are
     * failed by abortTransaction() with exactly one callback each, so a latch fed by the
     * callbacks is released rather than left hanging — and the failure outcome is visible, which
     * is what lets the production crossing wait die with the transaction instead of stamping.
     *
     * Asserts the waiting thread is released by the abort and every send got exactly one failed ack.
     */
    @Test
    void abortingATransactionReleasesAPendingAckWaitWithFailedAcks() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        createTopics(bootstrap, C1);

        Map<String, Object> config = transactionalProducerConfig(bootstrap, "c21-abort");
        config.put(ProducerConfig.LINGER_MS_CONFIG, 60_000);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(config)) {
            producer.initTransactions();
            producer.beginTransaction();

            CountDownLatch latch = new CountDownLatch(3);
            ackLatch = latch;
            producer.send(new ProducerRecord<>(C1, "a", "1"));
            producer.send(new ProducerRecord<>(C1, "b", "2"));
            producer.send(new ProducerRecord<>(C1, "c", "3"));
            assertEquals(0, ACKS.size(), "sends parked by linger must not have acked yet");

            AtomicBoolean released = new AtomicBoolean(false);
            Thread waiter = new Thread(() -> {
                try {
                    released.set(latch.await(20, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, "c21-crossing-wait");
            waiter.start();

            producer.abortTransaction();
            waiter.join(Duration.ofSeconds(25).toMillis());

            assertFalse(waiter.isAlive(), "the waiter must terminate once the abort completes");
            assertTrue(released.get(),
                    "aborting the transaction must fail pending sends and release the ack latch");
            assertEquals(3, ACKS.size(), "exactly one callback per send, abort included");
            assertEquals(3, SENDS.get(), "exactly three sends passed through onSend");
            assertTrue(ACKS.stream().allMatch(a -> a.exception() != null),
                    "aborted sends must surface as failures, never as successful acks");
        }
    }

    /** Finds the observed ack for {@code (topic, offset)}, or null if none has arrived yet. */
    private static @Nullable ObservedAck ackFor(String topic, long offset) {
        for (ObservedAck ack : ACKS) {
            if (ack.topic().equals(topic) && ack.offset() == offset) {
                return ack;
            }
        }
        return null;
    }

    /** A user processor that upper-cases values and records which thread runs process(). */
    private static ProcessorSupplier<String, String, String, String> threadRecordingUpperCaser(
            AtomicReference<String> streamThread) {
        return () -> new Processor<>() {
            private ProcessorContext<String, String> ctx;

            @Override
            public void init(ProcessorContext<String, String> context) {
                this.ctx = context;
            }

            @Override
            public void process(Record<String, String> record) {
                streamThread.set(Thread.currentThread().getName());
                ctx.forward(record.withValue(record.value().toUpperCase(Locale.ROOT)));
            }
        };
    }

    /** Polls C2 (read_committed) until a record with the expected value appears, and returns it. */
    private static ConsumerRecord<String, String> awaitOutputRecord(String bootstrap, String expectedValue) {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig(bootstrap))) {
            consumer.subscribe(List.of(C2));
            AtomicReference<ConsumerRecord<String, String>> found = new AtomicReference<>();
            await().atMost(Duration.ofSeconds(60)).until(() -> {
                for (ConsumerRecord<String, String> candidate : consumer.poll(Duration.ofMillis(500))) {
                    if (expectedValue.equals(candidate.value())) {
                        found.set(candidate);
                    }
                }
                return found.get() != null;
            });
            ConsumerRecord<String, String> record = found.get();
            assertNotNull(record, "expected a committed output record with value " + expectedValue);
            return record;
        }
    }

    private static Properties streamsConfig(String bootstrap) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ack-mechanics-it-" + UUID.randomUUID());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 200);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        return props;
    }

    private static Map<String, Object> plainProducerConfig(String bootstrap) {
        return Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    }

    private static Map<String, Object> transactionalProducerConfig(String bootstrap, String prefix) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, prefix + "-" + UUID.randomUUID());
        config.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, AckRegistryInterceptor.class.getName());
        return config;
    }

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
