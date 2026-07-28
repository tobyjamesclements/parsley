package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The production runtime end to end on a real broker: {@link Parsley#streams} under
 * exactly-once, records stamped on the wire with parseable protocol headers, and a restart
 * of the same application resuming without loss or duplication. The suite proves the
 * assembled plumbing works against a live cluster; causal-order correctness stays the
 * simulator's obligation.
 */
@Testcontainers
class ParsleyStreamsIT {

    @Container
    private static final KafkaContainer KAFKA = new KafkaContainer(
            DockerImageName.parse(System.getProperty("parsley.broker.image", "apache/kafka:3.7.2")));

    private static final Duration DEADLINE = Duration.ofSeconds(90);

    private static Admin admin;

    @TempDir
    Path stateDir;

    @BeforeAll
    static void connect() {
        admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()));
    }

    @AfterAll
    static void disconnect() {
        admin.close();
    }

    /**
     * A single stage delivers through the broker: records produced to the source come out of
     * the sink transformed, in per-partition order, each stamped with a clock, a sender tag,
     * and strictly increasing sender sequences.
     */
    @Test
    void singleStageDeliversStampedRecords() throws Exception {
        Topic<String, String> t1 = Topic.of("t1", Codec.utf8(), Codec.utf8());
        Topic<String, String> mid = Topic.of("mid", Codec.utf8(), Codec.utf8());
        createTopic("t1");
        createTopic("mid");

        Stage edge = Stage.named("edge")
                .on(t1, m -> List.of(mid.send(m.key(), m.value().toUpperCase(Locale.ROOT))))
                .into(mid)
                .build();

        try (CausalStreams streams = Parsley.of(edge).streams(props("parsley-it-single"))) {
            streams.start();
            produce("t1", "k1", "v1", "k2", "v2", "k3", "v3");

            List<ConsumerRecord<String, String>> out = consume("mid", 3);
            assertEquals(List.of("V1", "V2", "V3"),
                    out.stream().map(ConsumerRecord::value).toList(),
                    "the sink must carry the transformed records in per-partition order");

            UUID sender = null;
            long previousSeq = -1;
            for (ConsumerRecord<String, String> r : out) {
                assertNotNull(CausalHeaders.read(r.headers()),
                        "every stamped record must carry a parseable clock header");
                UUID s = CausalHeaders.readSender(r.headers());
                assertNotNull(s, "every stamped record must carry a sender tag");
                if (sender == null) sender = s;
                assertEquals(sender, s, "one task must stamp one sender identity");
                long seq = CausalHeaders.readSeq(r.headers());
                assertTrue(seq > previousSeq,
                        "sender sequences on one channel must strictly increase");
                previousSeq = seq;
            }
        }
    }

    /**
     * A restart of the same application on the same state directory resumes cleanly: the
     * record processed before the stop is not emitted again, and a record produced after the
     * restart flows through. The deep crash-recovery obligations stay with the simulator;
     * this is the live-cluster init path (restore, end-offset seed) actually running.
     */
    @Test
    void restartResumesWithoutLossOrDuplication() throws Exception {
        Topic<String, String> orders = Topic.of("orders", Codec.utf8(), Codec.utf8());
        Topic<String, String> shipped = Topic.of("shipped", Codec.utf8(), Codec.utf8());
        createTopic("orders");
        createTopic("shipped");

        Stage ship = Stage.named("ship")
                .on(orders, m -> List.of(shipped.send(m.key(), m.value())))
                .into(shipped)
                .build();

        try (CausalStreams first = Parsley.of(ship).streams(props("parsley-it-restart"))) {
            first.start();
            produce("orders", "o1", "packed");
            consume("shipped", 1);
            assertTrue(first.close(Duration.ofSeconds(60)),
                    "the first instance must stop cleanly before the restart");
        }

        try (CausalStreams second = Parsley.of(ship).streams(props("parsley-it-restart"))) {
            second.start();
            produce("orders", "o2", "packed");

            List<ConsumerRecord<String, String>> out = consume("shipped", 2);
            assertEquals(List.of("o1", "o2"),
                    out.stream().map(ConsumerRecord::key).toList(),
                    "the restarted instance must emit the new record exactly once and never"
                            + " re-emit the record processed before the stop");
        }
    }

    private Properties props(String applicationId) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        return props;
    }

    private static void createTopic(String name) throws Exception {
        admin.createTopics(Set.of(new NewTopic(name, 1, (short) 1))).all().get();
    }

    private static void produce(String topic, String... keyValuePairs) throws Exception {
        try (var producer = new KafkaProducer<>(
                Map.<String, Object>of("bootstrap.servers", KAFKA.getBootstrapServers()),
                new StringSerializer(), new StringSerializer())) {
            for (int i = 0; i < keyValuePairs.length; i += 2) {
                producer.send(new ProducerRecord<>(topic, keyValuePairs[i], keyValuePairs[i + 1]))
                        .get();
            }
        }
    }

    /** Reads the topic from the beginning, read-committed, until {@code count} records arrive. */
    private static List<ConsumerRecord<String, String>> consume(String topic, int count) {
        try (var consumer = new KafkaConsumer<>(
                Map.<String, Object>of(
                        "bootstrap.servers", KAFKA.getBootstrapServers(),
                        "isolation.level", "read_committed",
                        "auto.offset.reset", "earliest",
                        "group.id", "parsley-it-reader-" + topic),
                new StringDeserializer(), new StringDeserializer())) {
            TopicPartition tp = new TopicPartition(topic, 0);
            consumer.assign(List.of(tp));
            consumer.seekToBeginning(List.of(tp));
            List<ConsumerRecord<String, String>> out = new ArrayList<>();
            long deadline = System.nanoTime() + DEADLINE.toNanos();
            while (out.size() < count && System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(250)).forEach(out::add);
            }
            assertEquals(count, out.size(),
                    "expected " + count + " committed records on " + topic + " within "
                            + DEADLINE.toSeconds() + "s but saw " + out.size());
            return out;
        }
    }
}
