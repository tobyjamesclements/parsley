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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
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
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static java.util.Objects.requireNonNull;

/**
 * End-to-end proof of the scope-change rules (#21) against a real broker: a
 * redeploy that changes the input-topic set while causal state survives must neither replay an
 * added input's already-carried prefix into the surviving state (growth — "skip what you already
 * ignored") nor stop stamping the ancestry a removed input contributed (shrink — carried ancestry
 * re-homes, never drops). Each deployment reuses the {@code application.id} and restores from the
 * changelogs into a fresh state directory, exactly the operational shape the defect described.
 */
@Testcontainers(disabledWithoutDocker = true)
class CausalScopeChangeIT {

    @Container
    private final KafkaContainer kafka =
            new KafkaContainer(ParsleyBrokerImage.get());

    private static final String C1 = "c1";
    private static final String C3 = "c3";
    private static final String C2 = "c2";

    /**
     * Growth: an input consumed and delivered in deployment 1, removed in deployment 2, and
     * re-added in deployment 3 after its committed offsets were deleted (modelling offset expiry on
     * a re-added input) is re-fetched from log-start — the offset seeder's added-input branch permits
     * the start — but the already-delivered prefix is skipped at delivery, never forwarded to the
     * delegate a second time. Only records above the carried ancestry deliver.
     *
     * Asserts the start succeeds, the re-added topic's old record is not re-delivered (exactly one
     * output for it across all three deployments), and a fresh record on the re-added topic delivers.
     */
    @Test
    void reAddingAnInputSkipsTheAlreadyDeliveredPrefixAndDeliversFreshRecords() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        createTopics(bootstrap, C1, C3, C2);
        String appId = "scope-growth-it-" + UUID.randomUUID();

        // Deployment 1: consume C1 + C3; both records deliver.
        try (CausalStreams streams = new CausalStreams(
                topology(List.of(C1, C3)), streamsConfig(bootstrap, appId))) {
            streams.start();
            produce(bootstrap, C3, "x1");
            produce(bootstrap, C1, "a1");
            awaitOutputCount(bootstrap, 2);
        }

        // Deployment 2: C3 removed; its delivered ancestry re-homes into the carried clock.
        try (CausalStreams streams = new CausalStreams(
                topology(List.of(C1)), streamsConfig(bootstrap, appId))) {
            streams.start();
            produce(bootstrap, C1, "a2");
            awaitOutputCount(bootstrap, 3);
        }

        // The seeder's alterConsumerGroupOffsets needs an empty group, and a closed Streams app's
        // consumer only leaves at session timeout (Streams does not send LeaveGroup on close) — so
        // wait for the group to settle first, exactly the "retry once the group settles" remedy the
        // seeder's failure message prescribes for a too-fast redeploy.
        awaitGroupEmpty(bootstrap, appId);

        // The re-added input's committed offsets are gone (offset expiry / manual delete while the
        // causal state survives) — the exact shape that previously either refused to start or, keyed
        // on blob presence alone, replayed C3's history as live (#21).
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap))) {
            admin.deleteConsumerGroupOffsets(appId, Set.of(new TopicPartition(C3, 0))).all().get();
        }

        // Deployment 3: C3 re-added. The seeder's added-input branch seeds it to log-start; the
        // rescope seeds its frontier at the carried ancestry; the receive path skips the re-fetched
        // prefix. A fresh C3 record then delivers normally.
        try (CausalStreams streams = new CausalStreams(
                topology(List.of(C1, C3)), streamsConfig(bootstrap, appId))) {
            streams.start();
            produce(bootstrap, C3, "x2");
            List<ConsumerRecord<String, String>> outputs = awaitOutputCount(bootstrap, 4);

            List<String> values = new ArrayList<>();
            outputs.forEach(record -> values.add(record.value()));
            assertEquals(1, values.stream().filter("X1"::equals).count(),
                    "the re-added input's already-delivered record must be skipped on replay, not "
                            + "delivered a second time into the surviving state: " + values);
            assertEquals(1, values.stream().filter("X2"::equals).count(),
                    "a fresh record on the re-added input must deliver normally: " + values);
            assertEquals(4, values.size(),
                    "exactly the four distinct deliveries may reach the output: " + values);
        }
    }

    /**
     * Shrink: after a redeploy that removes an input, the survivor's outbound stamps must still
     * dominate every coordinate the removed input contributed — the retired channel's delivered
     * ancestry re-homes into the carried-ancestry clock the stamp keeps merging. Dropping it (the old
     * {@code pruneToScope} behaviour) would let a third party downstream deliver this node's new
     * outputs without waiting for their retired-channel causes.
     *
     * Asserts the post-shrink output's dependency header still claims the removed topic's delivered
     * offset alongside the surviving input's frontier.
     */
    @Test
    void removingAnInputKeepsStampsDominatingItsDeliveredAncestry() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        createTopics(bootstrap, C1, C3, C2);
        String appId = "scope-shrink-it-" + UUID.randomUUID();
        Uuid c1Id;
        Uuid c3Id;
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap))) {
            c1Id = requireNonNull(admin.describeTopics(List.of(C1, C3)).allTopicNames().get().get(C1)).topicId();
            c3Id = requireNonNull(admin.describeTopics(List.of(C1, C3)).allTopicNames().get().get(C3)).topicId();
        }

        // Deployment 1: consume C1 + C3; C3@0 and C1@0 both deliver into the causal state.
        try (CausalStreams streams = new CausalStreams(
                topology(List.of(C1, C3)), streamsConfig(bootstrap, appId))) {
            streams.start();
            produce(bootstrap, C3, "x1");
            produce(bootstrap, C1, "a1");
            awaitOutputCount(bootstrap, 2);
        }

        // Deployment 2: C3 removed. The new output's stamp must still dominate C3@0.
        try (CausalStreams streams = new CausalStreams(
                topology(List.of(C1)), streamsConfig(bootstrap, appId))) {
            streams.start();
            produce(bootstrap, C1, "a2");
            List<ConsumerRecord<String, String>> outputs = awaitOutputCount(bootstrap, 3);

            ConsumerRecord<String, String> postShrink = outputs.stream()
                    .filter(record -> "A2".equals(record.value()))
                    .findFirst().orElseThrow();
            Header stamp = postShrink.headers().lastHeader(ParsleyHeader.CAUSAL_CLOCK);
            assertNotNull(stamp, "every causal output must carry the dependency stamp header");
            ParsleyVectorClock clock = ParsleyVectorClock.fromBytes(stamp.value());
            assertEquals(0L, clock.offsetFor(c3Id, 0),
                    "the post-shrink stamp must still dominate the removed input's delivered "
                            + "ancestry (C3@0) — carried ancestry re-homes, never drops: " + clock);
            assertTrue(clock.offsetFor(c1Id, 0) >= 1L,
                    "the post-shrink stamp must also cover the surviving input's frontier through "
                            + "the record that produced it: " + clock);
        }
    }

    private CausalTopology topology(List<String> inputs) {
        return new CausalStreamsBuilder()
                .stream(inputs, Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to(C2, Serdes.String(), Serdes.String())
                .build();
    }

    /** A user processor that forwards each value upper-cased, so output values are distinguishable. */
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

    private static void produce(String bootstrap, String topic, String value) throws Exception {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()))) {
            producer.send(CausalClock.empty().stamp(new ProducerRecord<>(topic, "k", value))).get();
        }
    }

    /**
     * Reads {@code C2} from the beginning with a fresh {@code read_committed} consumer until it
     * holds {@code count} records, then keeps them (committed records only, so the count is exact
     * under EOS). Returns the records, headers included.
     */
    private static List<ConsumerRecord<String, String>> awaitOutputCount(String bootstrap, int count) {
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ConsumerConfig.GROUP_ID_CONFIG, "out-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()))) {
            consumer.subscribe(List.of(C2));
            await().atMost(Duration.ofSeconds(60)).until(() -> {
                consumer.poll(Duration.ofMillis(500)).forEach(records::add);
                return records.size() >= count;
            });
        }
        return records;
    }

    private Properties streamsConfig(String bootstrap, String appId) throws Exception {
        Path stateDir = Files.createTempDirectory("parsley-scope-it");
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 200);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        // A closed instance's group membership lingers until session timeout; keep it short so the
        // between-deployment group-settle waits stay fast.
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), 6000);
        // A fresh state directory per deployment forces a changelog restore, so the persisted
        // frontier value's carried-ancestry and declared-input sections are what carries the
        // scope knowledge across deployments — the exact mechanism under test.
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toAbsolutePath().toString());
        return props;
    }

    /** Waits until {@code appId}'s consumer group has no members, so seed commits are accepted. */
    private static void awaitGroupEmpty(String bootstrap, String appId) throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap))) {
            await().atMost(Duration.ofSeconds(60)).until(() -> requireNonNull(admin
                    .describeConsumerGroups(List.of(appId)).all().get()
                    .get(appId)).members().isEmpty());
        }
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
