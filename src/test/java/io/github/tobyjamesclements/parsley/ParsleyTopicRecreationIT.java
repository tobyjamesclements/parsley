package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Proves the mid-run enforcement of stable channel identity.
 * Topic name → UUID resolution is bound once per task lifetime at {@code init()}, so a causal
 * topic deleted and recreated <em>while a member runs</em> would silently rebind coordinates:
 * once the recreated topic's offsets re-pass the member's committed position, its records are
 * fetched and ingested under the old UUID. No per-record identity signal exists in the public
 * Streams API, so {@link ParsleyTopicIdentityWatch} bounds the window instead: the instance polls
 * the broker's current topic IDs, and the tasks' per-record checks fail the member fast once a
 * recreation is detected — the violation can never continue silently. (Records fetched inside one
 * poll interval remain the documented residual window; recreating causal topics under running
 * members is an operational constraint of the same kind as the retention rule.)
 */
@Testcontainers(disabledWithoutDocker = true)
class ParsleyTopicRecreationIT {

    @Container
    private final KafkaContainer kafka =
            new KafkaContainer(ParsleyBrokerImage.get());

    private static final String C1 = "c1";
    private static final String C2 = "c2";

    /**
     * An <em>input</em> topic deleted and recreated mid-run, with enough new records appended that
     * the member's committed offset is back in range — the dangerous window, where the consumer
     * can resume fetching the recreated topic's records under the stale UUID with no fetch error
     * at all. The member must reach {@code ERROR}, and the failure must be one of the identity
     * paths — which one wins is a race the environment decides: the identity watch's recreation
     * detection (the guaranteed backstop), the out-of-range failure under {@code none()} (when
     * the consumer's position check beats the refill), or Streams' own
     * {@code MissingSourceTopicException} (when a rebalance lands inside the deletion window).
     * Either way, the member never keeps running against the recreated topic.
     */
    @Test
    void aRecreatedInputTopicFailsTheMemberFast() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        createTopics(bootstrap, C1, C2);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig(bootstrap))) {
            for (int i = 0; i < 5; i++) {
                producer.send(new ProducerRecord<>(C1, "k", "pre-" + i)).get();
            }
        }

        AtomicReference<Throwable> uncaught = new AtomicReference<>();
        try (CausalStreams streams = causalApp(bootstrap)) {
            streams.setUncaughtExceptionHandler(throwable -> {
                uncaught.compareAndSet(null, throwable);
                return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            });
            streams.start();
            awaitBusinessOutputs(bootstrap, 5);

            // Delete and recreate C1, then immediately refill it past the member's committed
            // offset (5), so a consumer that resumes fetching sees a valid position — the silent
            // mislabelling path the watch exists to close.
            try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap))) {
                admin.deleteTopics(List.of(C1)).all().get();
                await().atMost(Duration.ofSeconds(30)).until(() ->
                        !admin.listTopics().names().get().contains(C1));
                admin.createTopics(List.of(new NewTopic(C1, 1, (short) 1))).all().get();
            }
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig(bootstrap))) {
                for (int i = 0; i < 10; i++) {
                    producer.send(new ProducerRecord<>(C1, "k", "post-" + i)).get();
                }
            }

            await().atMost(Duration.ofSeconds(60)).until(() -> streams.state() == KafkaStreams.State.ERROR);
            assertSame(KafkaStreams.State.ERROR, streams.state(),
                    "a mid-run input recreation must fail the member fast — it must never keep "
                            + "consuming the recreated topic under the stale UUID");
            Throwable failure = uncaught.get();
            assertNotNull(failure, "the member must die with an uncaught exception, not stall silently");
            assertTrue(isRecreationFailFast(failure),
                    "the failure must be an identity fail-fast path — the identity watch's recreation "
                            + "detection or the out-of-range failure under none() — not an incidental "
                            + "error: " + failure);
        }
    }

    /**
     * A <em>sink</em> topic deleted and recreated mid-run is as unsafe as an input's: the ack
     * registry folds by topic name through the stale UUID map, and the recreated topic's restarted
     * offsets make every fold a monotone no-op, so stamps silently stop claiming this node's own
     * new outputs, so downstream stamps under-claim. With no traffic in flight, only
     * the identity watch can detect it — the poll marks the watch broken, and the next record's
     * pre-ingest check fails the member with the watch's own exception, naming the sink, before
     * anything is stamped under the stale identity.
     */
    @Test
    void aRecreatedSinkTopicFailsTheMemberOnItsNextRecord() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        createTopics(bootstrap, C1, C2);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig(bootstrap))) {
            producer.send(new ProducerRecord<>(C1, "k", "pre-0")).get();
        }

        AtomicReference<Throwable> uncaught = new AtomicReference<>();
        try (CausalStreams streams = causalApp(bootstrap)) {
            streams.setUncaughtExceptionHandler(throwable -> {
                uncaught.compareAndSet(null, throwable);
                return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            });
            streams.start();
            awaitBusinessOutputs(bootstrap, 1);

            try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap))) {
                admin.deleteTopics(List.of(C2)).all().get();
                await().atMost(Duration.ofSeconds(30)).until(() ->
                        !admin.listTopics().names().get().contains(C2));
                admin.createTopics(List.of(new NewTopic(C2, 1, (short) 1))).all().get();
            }
            // Give the identity poll (5s interval) time to observe the recreation while the tasks
            // sit idle, then feed one record: its pre-ingest check must kill the task rather than
            // let it be processed and stamped under the stale sink identity.
            Thread.sleep(12_000);
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig(bootstrap))) {
                producer.send(new ProducerRecord<>(C1, "k", "post-0")).get();
            }

            await().atMost(Duration.ofSeconds(60)).until(() -> streams.state() == KafkaStreams.State.ERROR);
            Throwable failure = uncaught.get();
            assertNotNull(failure, "the member must die with an uncaught exception, not stall silently");
            assertTrue(chainContains(failure, CausalTopicRecreatedException.class),
                    "the failure must be the identity watch's recreation detection: " + failure);
            assertTrue(chainMessageContains(failure, C2),
                    "the failure must name the recreated sink topic: " + failure);
        }
    }

    /**
     * Any of the fail-fast paths for a recreated input: the identity watch, out-of-range under
     * none(), or Streams' own missing-source-topic rebalance failure (the deletion window itself).
     */
    private static boolean isRecreationFailFast(Throwable failure) {
        if (chainContains(failure, CausalTopicRecreatedException.class)) {
            return true;
        }
        for (Throwable t = failure; t != null; t = t.getCause()) {
            if (t instanceof org.apache.kafka.clients.consumer.OffsetOutOfRangeException
                    || t instanceof UnknownTopicOrPartitionException
                    || t instanceof org.apache.kafka.streams.errors.MissingSourceTopicException) {
                return true;
            }
            String message = t.getMessage();
            if (message != null && message.contains("No valid committed offset")) {
                return true;
            }
        }
        return false;
    }

    private static boolean chainContains(Throwable failure, Class<? extends Throwable> type) {
        for (Throwable t = failure; t != null; t = t.getCause()) {
            if (type.isInstance(t)) {
                return true;
            }
        }
        return false;
    }

    private static boolean chainMessageContains(Throwable failure, String needle) {
        for (Throwable t = failure; t != null; t = t.getCause()) {
            if (t.getMessage() != null && t.getMessage().contains(needle)) {
                return true;
            }
        }
        return false;
    }

    private CausalStreams causalApp(String bootstrap) {
        CausalTopology topology = new CausalStreamsBuilder()
                .stream(C1, Serdes.String(), Serdes.String())
                .process(passthrough())
                .to(C2, Serdes.String(), Serdes.String())
                .build();
        return new CausalStreams(topology, streamsConfig(bootstrap));
    }

    /** Waits until {@code count} business records (non-marker, non-null value) have reached C2. */
    private static void awaitBusinessOutputs(String bootstrap, int count) {
        List<ConsumerRecord<String, String>> outputs = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig(bootstrap))) {
            consumer.subscribe(List.of(C2));
            await().atMost(Duration.ofSeconds(90)).until(() -> {
                consumer.poll(Duration.ofMillis(500)).forEach(record -> {
                    if (record.value() != null
                            && record.headers().lastHeader(ParsleyHeader.NULL_MESSAGE) == null) {
                        outputs.add(record);
                    }
                });
                return outputs.size() >= count;
            });
        }
    }

    private static ProcessorSupplier<String, String, String, String> passthrough() {
        return () -> new Processor<>() {
            private ProcessorContext<String, String> ctx;

            @Override
            public void init(ProcessorContext<String, String> context) {
                this.ctx = context;
            }

            @Override
            public void process(Record<String, String> record) {
                ctx.forward(record);
            }
        };
    }

    private static Properties streamsConfig(String bootstrap) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "recreation-it-" + UUID.randomUUID());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 200);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
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
                ConsumerConfig.GROUP_ID_CONFIG, "observer-" + UUID.randomUUID(),
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
