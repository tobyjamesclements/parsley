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
import org.apache.kafka.common.utils.Utils;
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

import java.nio.charset.StandardCharsets;
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
 * Live rebalance across two members of one {@code application.id} — the broker-side owner of the
 * documented claim that the frontier and the causal buffer survive rebalances
 * (docs/concepts.md; docs/streams.md § Restart and recovery). Until now no test ran two members:
 * {@code CausalProcessorRestartIT} is single-instance with a graceful close, proving changelog
 * restore but not task migration.
 *
 * <p><strong>Channels.</strong> {@code c1} carries the causes, {@code c2} the dependents (each
 * stamped to claim a cause coordinate that does not exist yet — the upstream gap that genuinely
 * buffers them), {@code c3} is the sink. All topics have two partitions, so the two members split
 * the task set and an unclean death genuinely migrates tasks. Keys are chosen per partition
 * through Kafka's own default (murmur2) partitioner.
 */
@Testcontainers(disabledWithoutDocker = true)
class ParsleyLiveRebalanceIT {

    @Container
    private final KafkaContainer kafka = new KafkaContainer(ParsleyBrokerImage.get());

    private static final String C1 = "c1";
    private static final String C2 = "c2";
    private static final String C3 = "c3";

    /**
     * Two members split a two-partition topology; both partitions hold a genuinely buffered
     * dependent (its stamped cause coordinate does not exist yet); one member is killed uncleanly
     * ({@code close(Duration.ZERO)}: no drain, immediate stop). The survivor takes over the dead
     * member's tasks, restores their frontier and buffer from the changelogs, and — once the
     * missing causes are finally produced — drains everything in causal order.
     *
     * Asserts that no dependent-derived record reaches the sink while its cause is missing (the
     * records were genuinely held, before and after the kill), and that after the causes arrive
     * every partition's sink output shows the cause before the dependent, each record exactly
     * once across the whole timeline — held records buffered before the kill are delivered
     * exactly once, in causal order, by the survivor.
     */
    @Test
    void survivorRestoresMigratedBuffersAndDrainsInCausalOrderAfterAnUncleanKill() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        createTopics(bootstrap, C1, C2, C3);
        String appId = "rebalance-it-" + UUID.randomUUID();
        String keyP0 = keyForPartition(0);
        String keyP1 = keyForPartition(1);

        try (CausalStreams a = new CausalStreams(topology(),
                     memberConfig(bootstrap, appId, Files.createTempDirectory("rebalance-a")));
             CausalStreams b = new CausalStreams(topology(),
                     memberConfig(bootstrap, appId, Files.createTempDirectory("rebalance-b")));
             KafkaConsumer<String, String> observer = new KafkaConsumer<>(consumerConfig(bootstrap))) {
            a.start();
            b.start();
            await().atMost(Duration.ofSeconds(120)).until(() ->
                    a.state() == KafkaStreams.State.RUNNING && b.state() == KafkaStreams.State.RUNNING);

            // The upstream gap: each partition's dependent claims a cause coordinate (c1@0 on its
            // own partition) that has not been produced — both members buffer genuinely.
            produceDependents(bootstrap, keyP0, keyP1);

            observer.subscribe(List.of(C3));
            List<String> delivered = new ArrayList<>();
            assertHeld(observer, delivered,
                    "no dependent may reach the sink before its cause exists — the upstream gap "
                            + "must genuinely buffer on both members");

            // The unclean kill: zero budget means no drain and an immediate stop — the held
            // records' only survival path is the changelog restore on the survivor.
            a.close(Duration.ZERO);

            // Produce the missing causes; only the survivor is left to deliver both partitions.
            produceCauses(bootstrap, keyP0, keyP1);

            await().atMost(Duration.ofSeconds(180)).until(() -> {
                collectDelivered(observer, delivered);
                return delivered.size() >= 4;
            });
            assertEquals(4, delivered.size(),
                    "exactly the two causes and the two dependents must reach the sink — each "
                            + "exactly once across the kill and the migration, never a duplicate "
                            + "from replayed changelogs");
            assertOrderPerKey(delivered, keyP0);
            assertOrderPerKey(delivered, keyP1);

            // No stragglers: nothing beyond the four expected records may ever commit.
            await().during(Duration.ofSeconds(4)).atMost(Duration.ofSeconds(30)).until(() -> {
                collectDelivered(observer, delivered);
                return delivered.size() == 4;
            });
        }
    }

    /**
     * The drain-wait contract while the group is mid-rebalance ({@code CausalStreamsCloseTest}
     * covers ERROR and zero-task states, not a live rebalance): a member holding a genuinely
     * buffered record calls {@code close(Duration)} in the rebalance window opened by a second
     * member joining. The documented contract: the bounded close never trades causal order for
     * its deadline — the drain cannot complete (the cause is missing), so the call returns
     * {@code false}, the held record stays in the changelog-backed buffer, and the surviving
     * member later delivers it in causal order once the cause arrives.
     *
     * Asserts that the bounded close returns {@code false} with the buffer undrained, that the
     * dependent never reached the sink early, and that the survivor delivers cause before
     * dependent after the causes are produced.
     */
    @Test
    void boundedCloseDuringALiveRebalanceRefusesToTradeCausalOrderForItsDeadline() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        createTopics(bootstrap, C1, C2, C3);
        String appId = "rebalance-close-it-" + UUID.randomUUID();
        String keyP0 = keyForPartition(0);
        String keyP1 = keyForPartition(1);

        try (CausalStreams b = new CausalStreams(topology(),
                     memberConfig(bootstrap, appId, Files.createTempDirectory("rebalance-close-b")));
             KafkaConsumer<String, String> observer = new KafkaConsumer<>(consumerConfig(bootstrap))) {
            CausalStreams a = new CausalStreams(topology(),
                    memberConfig(bootstrap, appId, Files.createTempDirectory("rebalance-close-a")));
            try {
                a.start();
                await().atMost(Duration.ofSeconds(120)).until(() -> a.state() == KafkaStreams.State.RUNNING);
                produceDependents(bootstrap, keyP0, keyP1);

                observer.subscribe(List.of(C3));
                List<String> delivered = new ArrayList<>();
                assertHeld(observer, delivered,
                        "the dependents must be genuinely buffered before the close is attempted");

                // The second member joining opens the rebalance window the bounded close runs in.
                // The close outcome does not depend on catching the REBALANCING instant (the drain
                // wait spans RUNNING and REBALANCING alike; only ERROR/stopped end it early) — but
                // starting b here makes the window real, not simulated.
                b.start();
                boolean fullyStopped = a.close(Duration.ofSeconds(2));
                assertFalse(fullyStopped,
                        "a bounded close with an undrainable buffer (its cause is missing) must "
                                + "report failure rather than invent progress — the drain never "
                                + "trades causal order for its deadline");

                assertHeld(observer, delivered,
                        "the truncated drain must leave the held records in the changelog-backed "
                                + "buffer, never deliver them to meet the deadline");

                produceCauses(bootstrap, keyP0, keyP1);
                await().atMost(Duration.ofSeconds(180)).until(() -> {
                    collectDelivered(observer, delivered);
                    return delivered.size() >= 4;
                });
                assertEquals(4, delivered.size(),
                        "the surviving member must deliver exactly the two causes and the two "
                                + "dependents restored from the abandoned member's changelogs");
                assertOrderPerKey(delivered, keyP0);
                assertOrderPerKey(delivered, keyP1);
            } finally {
                a.close();
            }
        }
    }

    /** Forwards each value upper-cased, so sink output is distinguishable from input. */
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

    private static CausalTopology topology() {
        return new CausalStreamsBuilder()
                .stream(List.of(C1, C2), Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to(C3, Serdes.String(), Serdes.String())
                .build();
    }

    /**
     * One dependent per partition on {@code c2}, each stamped to claim its own partition's
     * not-yet-produced cause coordinate {@code c1@0} — the upstream gap that buffers genuinely.
     */
    private static void produceDependents(String bootstrap, String keyP0, String keyP1) throws Exception {
        Properties resolverProps = new Properties();
        resolverProps.put("bootstrap.servers", bootstrap);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig(bootstrap))) {
            producer.send(CausalClock.using(resolverProps)
                    .observe(new ConsumerRecord<>(C1, 0, 0L, keyP0, "m1"))
                    .stamp(new ProducerRecord<>(C2, keyP0, "m2"))).get();
            producer.send(CausalClock.using(resolverProps)
                    .observe(new ConsumerRecord<>(C1, 1, 0L, keyP1, "m1"))
                    .stamp(new ProducerRecord<>(C2, keyP1, "m2"))).get();
        }
    }

    /** The missing causes, one per partition on {@code c1}, causally minimal. */
    private static void produceCauses(String bootstrap, String keyP0, String keyP1) throws Exception {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig(bootstrap))) {
            producer.send(CausalClock.empty().stamp(new ProducerRecord<>(C1, keyP0, "m1"))).get();
            producer.send(CausalClock.empty().stamp(new ProducerRecord<>(C1, keyP1, "m1"))).get();
        }
    }

    /** The per-key causal-order assertion: the cause's output strictly before the dependent's. */
    private static void assertOrderPerKey(List<String> delivered, String key) {
        int cause = delivered.indexOf(key + ":M1");
        int dependent = delivered.indexOf(key + ":M2");
        assertTrue(cause >= 0 && dependent >= 0 && cause < dependent,
                "partition key " + key + ": the cause's output must precede the dependent's at the "
                        + "sink — got " + delivered);
    }

    /** Asserts, over a settle window, that no business record has reached the sink. */
    private static void assertHeld(KafkaConsumer<String, String> observer, List<String> delivered,
                                   String message) {
        await().during(Duration.ofSeconds(4)).atMost(Duration.ofSeconds(30)).until(() -> {
            collectDelivered(observer, delivered);
            return delivered.isEmpty();
        });
        assertTrue(delivered.isEmpty(), message + " — saw " + delivered);
    }

    /** Polls once and appends every committed business value as {@code key:value}. */
    private static void collectDelivered(KafkaConsumer<String, String> observer, List<String> delivered) {
        observer.poll(Duration.ofMillis(200)).forEach(record -> {
            if (record.value() != null && record.headers().lastHeader(ParsleyHeader.NULL_MESSAGE) == null) {
                delivered.add(record.key() + ":" + record.value());
            }
        });
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

    private static Properties memberConfig(String bootstrap, String appId, Path stateDir) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 200);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toAbsolutePath().toString());
        // Faster failover: the survivor should take over the killed member's tasks in seconds,
        // not the default 45s session timeout.
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), 6000);
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG), 2000);
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
                ConsumerConfig.GROUP_ID_CONFIG, "rebalance-observer-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    }

    private static void createTopics(String bootstrap, String... topics) throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap))) {
            Set<NewTopic> newTopics = new HashSet<>();
            for (String topic : topics) {
                newTopics.add(new NewTopic(topic, 2, (short) 1));
            }
            admin.createTopics(newTopics).all().get();
        }
    }
}
