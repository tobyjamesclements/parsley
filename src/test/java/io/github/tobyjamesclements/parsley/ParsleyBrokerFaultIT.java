package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static java.util.Objects.requireNonNull;

/**
 * Real-broker infrastructure faults the in-JVM simulator cannot model: producer acknowledgements
 * stalled past the delivery timeout while a crossing wait is pending, and the client-broker path
 * severed and restored mid-flow. All of the Streams application's traffic runs through a Toxiproxy
 * ({@link ParsleyProxiedBroker}: the broker advertises the proxy's own endpoint for the proxied
 * listener, so metadata can never redirect the app around the proxy), while the test's own clients
 * use the broker's direct listener.
 *
 * <p>Topics: {@code c1} carries the cause record, {@code c2} the dependent record (stamped with
 * {@link CausalClock} to claim the cause's coordinate on {@code c1}), {@code c3} is the sink.
 * Messages: {@code m1} is the healthy-phase warm-up proof, {@code m2} the cause, {@code m3} the
 * dependent.
 *
 * <p>Causal safety is inviolable: under any fault the processor must block or fail — abort the EOS
 * transaction, die with a {@link CausalPendingAckException} or a producer timeout — and never
 * resolve a wait by guessing, so the sink can never show a dependent record without its cause.
 */
@Testcontainers(disabledWithoutDocker = true)
class ParsleyBrokerFaultIT {

    @Container
    private final ParsleyProxiedBroker broker = new ParsleyProxiedBroker();

    private static final String C1 = "c1";
    private static final String C2 = "c2";
    private static final String C3 = "c3";

    /**
     * Acknowledgements stall indefinitely (timeout toxic: connections open, no bytes) while a
     * crossing wait is pending: the funnel delegate's first forward to partition 0 of {@code c3}
     * never acks, so the second forward's stamp — which must claim the first send's coordinate,
     * knowable only from that ack — waits until the (shortened) producer delivery timeout and then
     * fails. The handshake latches make the moment deterministic: the stall engages only after the
     * cause record has entered {@code process()}, and the forwards happen only after the stall is
     * live, so the fault always lands exactly on a pending crossing wait. A healthy warm-up record
     * ({@code m1}) first proves the proxied path delivers end to end — which also proves the stall
     * that follows travels the same path the app does.
     *
     * Asserts that the failure surfaces fail-closed through the uncaught exception handler with a
     * {@link CausalPendingAckException} or a producer {@code TimeoutException} in its causal chain.
     * Asserts that the application leaves RUNNING rather than making silent progress. Asserts that
     * the committed end-state of the sink holds exactly the healthy-era outputs — the stalled
     * transaction aborted — and in particular never a dependent-derived record without its cause.
     */
    @Test
    void stalledAcksPastTheDeliveryTimeoutFailClosedWhileACrossingWaitIsPending() throws Exception {
        String direct = broker.directBootstrap();
        createTopics(direct, Map.of(C1, 1, C2, 1, C3, 2));

        CountDownLatch causeEntered = new CountDownLatch(1);
        CountDownLatch stallEngaged = new CountDownLatch(1);
        CausalTopology topology = new CausalStreamsBuilder()
                .stream(List.of(C1, C2), Serdes.String(), Serdes.String())
                .process(handshakeFunnel(causeEntered, stallEngaged, keyForPartition(0), keyForPartition(1)))
                .to(C3, Serdes.String(), Serdes.String())
                .build();

        List<Throwable> failures = new CopyOnWriteArrayList<>();
        try (CausalStreams streams =
                     new CausalStreams(topology, shortDeliveryTimeoutConfig(broker.proxiedBootstrap()))) {
            streams.setUncaughtExceptionHandler(failure -> {
                failures.add(failure);
                return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            });
            streams.start();
            try (KafkaConsumer<String, String> observer = new KafkaConsumer<>(consumerConfig(direct))) {
                observer.subscribe(List.of(C3));
                List<String> delivered = new ArrayList<>();

                // Healthy phase: m1 through the proxied path proves end-to-end delivery,
                // crossing wait included (the funnel's second forward claims the first's ack).
                produceDirect(direct, CausalClock.empty().stamp(new ProducerRecord<>(C1, "wk", "m1")));
                awaitDeliveredCount(observer, delivered, 2);
                assertEquals(Set.of("first:m1", "second:m1"), Set.copyOf(delivered),
                        "the healthy warm-up record must flow end to end through the proxied path "
                                + "before any fault is injected — otherwise the fault phase proves nothing");

                try {
                    // The dependent m3 claims the cause's future coordinate C1@1, produced FIRST;
                    // then the cause m2. The app fetches both while the path is still healthy.
                    CausalClock dependentClock = CausalClock.using(resolverProps(direct))
                            .observe(new ConsumerRecord<>(C1, 0, 1L, "ck", "m2"));
                    produceDirect(direct, dependentClock.stamp(new ProducerRecord<>(C2, "dk", "m3")));
                    produceDirect(direct, CausalClock.empty().stamp(new ProducerRecord<>(C1, "ck", "m2")));

                    assertTrue(causeEntered.await(60, TimeUnit.SECONDS),
                            "the cause record must reach process() while the path is healthy — the "
                                    + "stall is only engaged once the handshake confirms it");
                    broker.stallAllTraffic();
                    stallEngaged.countDown();

                    await().atMost(Duration.ofSeconds(90)).until(() -> !failures.isEmpty());
                    assertTrue(failures.stream().anyMatch(ParsleyBrokerFaultIT::isFailClosedSignature),
                            "the stalled-ack failure must surface as a CausalPendingAckException "
                                    + "(the crossing wait refusing to resolve by proceeding) or a "
                                    + "producer TimeoutException (the aborted EOS transaction) — got: "
                                    + failures);
                    await().atMost(Duration.ofSeconds(90)).until(() -> {
                        KafkaStreams.State state = streams.state();
                        return state != KafkaStreams.State.RUNNING && state != KafkaStreams.State.REBALANCING;
                    });
                } finally {
                    // Unblock the delegate and heal the proxy on every path so shutdown never hangs.
                    stallEngaged.countDown();
                    broker.restoreTraffic();
                }
            }
        }

        // The app is dead and its transactions resolved; read the sink's committed end-state.
        List<String> committed = readAllCommittedValues(direct, C3);
        assertEquals(Set.of("first:m1", "second:m1"), Set.copyOf(committed),
                "only the healthy-era outputs may ever commit — the stalled-ack transaction must "
                        + "abort in full, never deliver partially or out of order");
        assertEquals(2, committed.size(),
                "exactly the two healthy-era outputs must be committed, each exactly once");
        assertFalse(committed.stream().anyMatch(value -> value.endsWith(":m3")),
                "no dependent-derived record may appear at the sink: its cause's outputs were never "
                        + "committed, and delivering it anyway would be an out-of-order delivery");
    }

    /**
     * The client-broker path is severed (connections closed and refused, a broker outage) and
     * later restored, with default generous client timeouts so retries ride the outage out. While
     * severed, the dependent {@code m3} (claiming the cause's coordinate {@code C1@1}) is produced
     * before its cause {@code m2} via the direct listener; the application can see neither. The
     * no-progress window also proves the outage actually cut the app's traffic — that all of it
     * traverses the proxy.
     *
     * Asserts that no new record is committed to the sink while the path is severed. Asserts that
     * after the path is restored, delivery resumes and the sink shows the cause's output before the
     * dependent's — causal order preserved across a broker outage, with the dependent held rather
     * than delivered early.
     */
    @Test
    void deliveryResumesInCausalOrderAfterTheBrokerPathIsSeveredAndRestored() throws Exception {
        String direct = broker.directBootstrap();
        createTopics(direct, Map.of(C1, 1, C2, 1, C3, 1));

        CausalTopology topology = new CausalStreamsBuilder()
                .stream(List.of(C1, C2), Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to(C3, Serdes.String(), Serdes.String())
                .build();

        try (CausalStreams streams =
                     new CausalStreams(topology, defaultTimeoutsConfig(broker.proxiedBootstrap()));
             KafkaConsumer<String, String> observer = new KafkaConsumer<>(consumerConfig(direct))) {
            streams.start();
            observer.subscribe(List.of(C3));
            List<String> delivered = new ArrayList<>();

            // Healthy phase: m1 through the proxied path proves end-to-end delivery.
            produceDirect(direct, CausalClock.empty().stamp(new ProducerRecord<>(C1, "wk", "m1")));
            awaitDeliveredCount(observer, delivered, 1);
            assertEquals(List.of("M1"), delivered,
                    "the healthy warm-up record must flow end to end through the proxied path "
                            + "before the outage begins");

            try {
                broker.severConnections();

                // Dependent first (claims the cause's future coordinate C1@1), then the cause —
                // both landing while the app cannot reach the broker.
                CausalClock dependentClock = CausalClock.using(resolverProps(direct))
                        .observe(new ConsumerRecord<>(C1, 0, 1L, "ck", "m2"));
                produceDirect(direct, dependentClock.stamp(new ProducerRecord<>(C2, "dk", "m3")));
                produceDirect(direct, CausalClock.empty().stamp(new ProducerRecord<>(C1, "ck", "m2")));

                await().during(Duration.ofSeconds(4)).atMost(Duration.ofSeconds(30)).until(() -> {
                    collectDelivered(observer, delivered);
                    return delivered.size() == 1;
                });
                assertEquals(List.of("M1"), delivered,
                        "no new record may commit while the app's path to the broker is severed — "
                                + "progress here would mean the app's traffic bypasses the proxy "
                                + "and the outage is vacuous");
            } finally {
                broker.restoreConnections();
            }

            awaitDeliveredCount(observer, delivered, 3);
            assertEquals(List.of("M1", "M2", "M3"), delivered,
                    "after the outage, delivery must resume in causal order: the cause's output "
                            + "before the dependent's, the dependent held — never guessed early — "
                            + "while its cause was undelivered");
        }
    }

    /**
     * The scenario-1 delegate: every input funnels into two forwards — {@code first:<value>} keyed
     * onto partition 0 and {@code second:<value>} keyed onto partition 1 of the sink — so the
     * second forward's stamp always crosses partitions and must wait on the first send's ack. The
     * cause record additionally handshakes with the test: it signals {@code causeEntered} and holds
     * before forwarding until {@code stallEngaged}, so the stall is always live before the sends
     * and the crossing wait deterministically finds its ack pending.
     */
    private static ProcessorSupplier<String, String, String, String> handshakeFunnel(
            CountDownLatch causeEntered, CountDownLatch stallEngaged, String keyForP0, String keyForP1) {
        return () -> new Processor<>() {
            private ProcessorContext<String, String> ctx;

            @Override
            public void init(ProcessorContext<String, String> context) {
                this.ctx = context;
            }

            @Override
            public void process(Record<String, String> record) {
                if ("m2".equals(record.value())) {
                    causeEntered.countDown();
                    try {
                        if (!stallEngaged.await(120, TimeUnit.SECONDS)) {
                            throw new IllegalStateException("handshake: the test never engaged the stall");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("handshake interrupted", e);
                    }
                }
                ctx.forward(new Record<>(keyForP0, "first:" + record.value(), record.timestamp()));
                ctx.forward(new Record<>(keyForP1, "second:" + record.value(), record.timestamp()));
            }
        };
    }

    /** The scenario-2 delegate: forwards each value upper-cased, so outputs are distinguishable. */
    private static ProcessorSupplier<String, String, String, String> upperCaser() {
        return () -> new Processor<>() {
            private ProcessorContext<String, String> ctx;

            @Override
            public void init(ProcessorContext<String, String> context) {
                this.ctx = context;
            }

            @Override
            public void process(Record<String, String> record) {
                ctx.forward(record.withValue(record.value().toUpperCase(Locale.ROOT)));
            }
        };
    }

    /**
     * Whether a stream-thread failure is one of the two legitimate fail-closed signatures: a
     * {@link CausalPendingAckException} (the crossing wait refusing to proceed past a stalled or
     * failed own-output ack) or a producer {@code TimeoutException} (the delivery timeout aborting
     * the EOS transaction), anywhere in the causal chain.
     */
    private static boolean isFailClosedSignature(Throwable failure) {
        Throwable current = failure;
        for (int depth = 0; current != null && depth < 20; depth++) {
            if (current instanceof CausalPendingAckException
                    || current instanceof org.apache.kafka.common.errors.TimeoutException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    /**
     * A key Kafka's default (murmur2) partitioner routes to {@code partition} of a two-partition
     * topic — searched deterministically so the test never depends on a lucky constant.
     */
    private static String keyForPartition(int partition) {
        for (int i = 0; i < 100; i++) {
            String candidate = "key-" + i;
            int routed = Utils.toPositive(Utils.murmur2(candidate.getBytes(StandardCharsets.UTF_8))) % 2;
            if (routed == partition) {
                return candidate;
            }
        }
        throw new AssertionError("no key found for partition " + partition + " in 100 candidates");
    }

    /** A record with a value and no Parsley marker header — a business record, not a null message. */
    private static boolean isBusinessRecord(ConsumerRecord<String, String> record) {
        return record.value() != null && record.headers().lastHeader(ParsleyHeader.NULL_MESSAGE) == null;
    }

    /** Polls once and appends every committed business value, preserving consumption order. */
    private static void collectDelivered(KafkaConsumer<String, String> observer, List<String> delivered) {
        observer.poll(Duration.ofMillis(200)).forEach(record -> {
            if (isBusinessRecord(record)) {
                delivered.add(record.value());
            }
        });
    }

    /** Polls the observer until at least {@code count} committed business values have arrived. */
    private static void awaitDeliveredCount(
            KafkaConsumer<String, String> observer, List<String> delivered, int count) {
        await().atMost(Duration.ofSeconds(90)).until(() -> {
            collectDelivered(observer, delivered);
            return delivered.size() >= count;
        });
    }

    /**
     * Reads every committed business value on {@code topic} from the beginning to the (stable) end
     * of the log. Awaits the read-committed end offsets because the last-stable-offset only passes
     * an aborted transaction once its abort marker is written — bounded by the transaction timeout
     * after the app dies, so this settles deterministically without sleeping.
     */
    private static List<String> readAllCommittedValues(String bootstrap, String topic) {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig(bootstrap))) {
            List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
                    .map(info -> new TopicPartition(topic, info.partition()))
                    .toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            List<String> values = new ArrayList<>();
            await().atMost(Duration.ofSeconds(90)).until(() -> {
                collectDelivered(consumer, values);
                Map<TopicPartition, Long> end = consumer.endOffsets(partitions);
                return partitions.stream().allMatch(tp -> consumer.position(tp) >= requireNonNull(end.get(tp)));
            });
            return values;
        }
    }

    /** Sends one record through the broker's direct (unproxied) listener and awaits its ack. */
    private static void produceDirect(String bootstrap, ProducerRecord<String, String> record)
            throws Exception {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig(bootstrap))) {
            producer.send(record).get();
        }
    }

    /**
     * Scenario-1 Streams config: the proxied bootstrap, plus a shortened producer delivery timeout
     * (8 s; request timeout 4 s below it, satisfying Kafka's {@code delivery >= request + linger})
     * so the stalled-ack failure surfaces quickly — this is also the value ParsleyProcessor
     * resolves as the crossing-wait bound. The transaction timeout is raised above the delivery
     * timeout so the failure signature is the delivery timeout, not a coordinator fencing. The
     * funnel fans two 1-partition sources into a 2-partition sink — a wider sink passes the
     * sink-width startup check.
     */
    private static Properties shortDeliveryTimeoutConfig(String bootstrap) {
        Properties props = baseStreamsConfig(bootstrap);
        props.put("producer." + ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 8000);
        props.put("producer." + ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 4000);
        props.put("producer." + ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 30000);
        return props;
    }

    /** Scenario-2 Streams config: default (generous) client timeouts, so retries ride out the outage. */
    private static Properties defaultTimeoutsConfig(String bootstrap) {
        return baseStreamsConfig(bootstrap);
    }

    private static Properties baseStreamsConfig(String bootstrap) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "broker-fault-" + UUID.randomUUID());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 200);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        return props;
    }

    private static Properties resolverProps(String bootstrap) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        return props;
    }

    private static Map<String, Object> producerConfig(String bootstrap) {
        return Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    }

    private static Map<String, Object> consumerConfig(String bootstrap) {
        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ConsumerConfig.GROUP_ID_CONFIG, "broker-fault-observer-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    }

    private static void createTopics(String bootstrap, Map<String, Integer> topicPartitions) throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap))) {
            Set<NewTopic> newTopics = new HashSet<>();
            topicPartitions.forEach((topic, partitions) ->
                    newTopics.add(new NewTopic(topic, partitions, (short) 1)));
            admin.createTopics(newTopics).all().get();
        }
    }
}
