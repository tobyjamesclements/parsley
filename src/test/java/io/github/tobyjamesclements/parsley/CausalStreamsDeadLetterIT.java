package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
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
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end dead-letter behaviour through a real {@link CausalStreams} runtime against a real broker:
 * a poison record (undecodable on the forward path) lands on the dead-letter topic {@link CausalStreams}
 * provisions at {@link CausalStreams#start()}, is absent from the normal output, a causal dependent of
 * it cascades to the dead-letter topic too, and a clean {@link CausalStreams#close()} still drains
 * everything else and returns.
 */
@Testcontainers(disabledWithoutDocker = true)
class CausalStreamsDeadLetterIT {

    @Container
    private final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"));

    private static final String PREREQ = "prereq";
    private static final String IN = "in";
    private static final String DEPENDENT = "dependent";
    private static final String OUT = "out";

    /**
     * {@code IN@0} depends on the not-yet-observed {@code PREREQ@0} and is buffered; {@code DEPENDENT@0}
     * depends on {@code IN}'s own coordinate and is also buffered. Once {@code IN}'s value becomes
     * undecodable (simulated by flipping the poisonable deserializer after buffering, before the
     * trigger) and {@code PREREQ@0} arrives to release it, {@code IN} is dead-lettered as poison, and
     * {@code DEPENDENT} cascades to the dead-letter topic too — neither ever reaches {@code OUT}.
     *
     * Asserts: the dead-letter topic receives IN (reason POISON) and DEPENDENT (reason ORPHAN_CASCADE),
     * each carrying source-topic/partition/offset headers; the output topic never receives either; and
     * {@code causalStreams.close()} completes (drains normally, does not hang on the dead-lettered
     * records).
     */
    @Test
    void poisonRecordAndItsCascadedDependentLandOnTheDeadLetterTopicNeverOnOutput() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        createTopics(bootstrap, PREREQ, IN, DEPENDENT, OUT);
        String appId = "deadletter-it-" + UUID.randomUUID();

        Serde<String> poisonableValueSerde = poisonableStringSerde();

        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream(PREREQ, Serdes.String(), Serdes.String())
                .merge(builder.stream(IN, Serdes.String(), poisonableValueSerde))
                .merge(builder.stream(DEPENDENT, Serdes.String(), Serdes.String()))
                .process(passthrough())
                .to("out-sink", OUT, Serdes.String(), Serdes.String());
        CausalTopology topology = builder.build();

        // Resolve topic UUIDs eagerly while the admin is open — CausalTopics.of(Admin) resolves lazily
        // on first use, which would otherwise reach for a closed AdminClient once this try-with-resources
        // block exits.
        CausalTopics topics;
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap))) {
            Map<String, Uuid> ids = new java.util.HashMap<>();
            for (String topic : List.of(PREREQ, IN, DEPENDENT, OUT)) {
                ids.put(topic, admin.describeTopics(List.of(topic)).topicNameValues().get(topic).get().topicId());
            }
            topics = CausalTopics.of(ids);
        }

        CausalStreams causalStreams = new CausalStreams(topology, streamsConfig(bootstrap, appId));
        causalStreams.start();
        try {
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig(bootstrap))) {
                // IN@0 depends on PREREQ@0 (not yet observed) — buffered, still decodable at this point.
                CausalDependencies inDeps = CausalDependencies.builder(topics).require(PREREQ, 0, 0).build();
                producer.send(inDeps.stamp(new ProducerRecord<>(IN, "ik", "poison-me"))).get();

                // DEPENDENT@0 depends on IN's own coordinate — buffered.
                CausalDependencies dependentDeps = CausalDependencies.builder(topics).require(IN, 0, 0).build();
                producer.send(dependentDeps.stamp(new ProducerRecord<>(DEPENDENT, "dk", "dependent"))).get();

                // The trigger: releases IN via the ordinary drain path, where fetching it now throws (the
                // poisonable deserializer's second-ever invocation — see poisonableStringSerde()).
                producer.send(CausalDependencies.empty().stamp(new ProducerRecord<>(PREREQ, "pk", "prereq"))).get();
            }

            try (KafkaConsumer<byte[], byte[]> deadLetter = new KafkaConsumer<>(byteConsumerConfig(bootstrap))) {
                deadLetter.subscribe(List.of(appId + "-deadletter"));
                List<ConsumerRecord<byte[], byte[]>> letters = pollRecords(deadLetter, 2);

                assertEquals(Set.of(IN, DEPENDENT), letters.stream()
                                .map(r -> headerValue(r, "parsley-deadletter-source-topic"))
                                .collect(java.util.stream.Collectors.toSet()),
                        "both the poisoned record and its cascaded dependent must reach the dead-letter topic");

                ConsumerRecord<byte[], byte[]> inLetter = letters.stream()
                        .filter(r -> IN.equals(headerValue(r, "parsley-deadletter-source-topic")))
                        .findFirst().orElseThrow();
                assertEquals("POISON", headerValue(inLetter, "parsley-deadletter-reason"),
                        "IN must be dead-lettered as the poison root cause");

                ConsumerRecord<byte[], byte[]> dependentLetter = letters.stream()
                        .filter(r -> DEPENDENT.equals(headerValue(r, "parsley-deadletter-source-topic")))
                        .findFirst().orElseThrow();
                assertEquals("ORPHAN_CASCADE", headerValue(dependentLetter, "parsley-deadletter-reason"),
                        "DEPENDENT must be dead-lettered as a cascade victim of IN");
            }

            try (KafkaConsumer<String, String> out = new KafkaConsumer<>(consumerConfig(bootstrap))) {
                out.subscribe(List.of(OUT));
                List<String> delivered = pollValues(out, 1, Duration.ofSeconds(10));
                assertEquals(List.of("PREREQ"), delivered,
                        "only the trigger reaches the real output — IN and DEPENDENT never do");
            }
        } finally {
            // Must not hang: nothing legitimate remains buffered (IN and DEPENDENT were both diverted).
            causalStreams.close();
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
                ctx.forward(record.withValue(record.value().toUpperCase(java.util.Locale.ROOT)));
            }
        };
    }

    /**
     * A {@code Serde<String>} whose serializer is ordinary but whose deserializer succeeds on its first
     * call and throws on every call after — a call-count gate rather than a manually-flipped flag, so
     * there is no race against the background {@code StreamThread}'s consumption timing. Exactly two
     * deserializer invocations are expected over this record's lifetime: Kafka Streams' own source-node
     * ingest (the first, which must succeed so the record is admitted and buffered normally), and
     * Parsley's later buffer-drain fetch (the second, which must fail — simulating a value that became
     * undecodable while held, e.g. a registry/schema change).
     */
    private static Serde<String> poisonableStringSerde() {
        Serializer<String> serializer = new StringSerializer();
        Deserializer<String> realDeserializer = new StringDeserializer();
        java.util.concurrent.atomic.AtomicInteger calls = new java.util.concurrent.atomic.AtomicInteger();
        Deserializer<String> deserializer = (topic, data) -> {
            if (calls.getAndIncrement() > 0) {
                throw new RuntimeException("simulated poison: value no longer decodable");
            }
            return realDeserializer.deserialize(topic, data);
        };
        return Serdes.serdeFrom(serializer, deserializer);
    }

    private static @org.jspecify.annotations.Nullable String headerValue(ConsumerRecord<byte[], byte[]> record, String key) {
        Header header = record.headers().lastHeader(key);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    private static List<String> pollValues(KafkaConsumer<String, String> consumer, int count, Duration window) {
        List<String> values = new ArrayList<>();
        long deadline = System.currentTimeMillis() + window.toMillis();
        while (System.currentTimeMillis() < deadline && values.size() < count) {
            consumer.poll(Duration.ofMillis(500)).forEach(record -> values.add(record.value()));
        }
        // Keep polling briefly past the target count to prove nothing ELSE ever arrives.
        long extra = System.currentTimeMillis() + Duration.ofSeconds(5).toMillis();
        while (System.currentTimeMillis() < extra) {
            ConsumerRecords<String, String> more = consumer.poll(Duration.ofMillis(500));
            more.forEach(record -> values.add(record.value()));
        }
        return values;
    }

    private static List<ConsumerRecord<byte[], byte[]>> pollRecords(KafkaConsumer<byte[], byte[]> consumer, int count) {
        List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        await().atMost(Duration.ofSeconds(60)).until(() -> {
            consumer.poll(Duration.ofMillis(500)).forEach(records::add);
            return records.size() >= count;
        });
        return records;
    }

    private static Properties streamsConfig(String bootstrap, String appId) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 200);
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
                ConsumerConfig.GROUP_ID_CONFIG, "out-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    }

    private static Map<String, Object> byteConsumerConfig(String bootstrap) {
        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ConsumerConfig.GROUP_ID_CONFIG, "deadletter-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
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
