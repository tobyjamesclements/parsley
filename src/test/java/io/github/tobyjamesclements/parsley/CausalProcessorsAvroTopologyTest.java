package io.github.tobyjamesclements.parsley;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.github.tobyjamesclements.parsley.avro.Order;
import io.github.tobyjamesclements.parsley.avro.Price;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises the causal decorator with <strong>real Avro + Confluent Schema Registry</strong> serdes
 * over multiple input topics, each carrying its own schema, against an in-process
 * {@link MockSchemaRegistry} (no broker, no Docker).
 *
 * <p>The point under test is {@code ParsleyResolver}'s per-source-topic serde resolution: a held
 * {@link Order} must be Avro-serialised into the buffer store under the {@code orders-value} subject
 * (its source topic), then deserialised back to an equal {@code Order} when it drains — never under
 * the changelog/store name.
 */
class CausalProcessorsAvroTopologyTest {

    // The scope is the part after mock:// — MockSchemaRegistry keys its in-JVM client by it.
    private static final String SCOPE = "parsley-avro-topology";
    private static final String REGISTRY_URL = "mock://" + SCOPE;

    private static final String PRICES = "prices";
    private static final String ORDERS = "orders";
    private static final Uuid PRICES_ID = Uuid.randomUuid();
    private static final Uuid ORDERS_ID = Uuid.randomUuid();
    private static final CausalTopics TOPICS = CausalTopics.of(Map.of(PRICES, PRICES_ID, ORDERS, ORDERS_ID));

    private final List<SpecificRecord> processed = new ArrayList<>();

    @AfterEach
    void dropRegistryScope() {
        // Keep the shared in-JVM registry isolated between tests.
        MockSchemaRegistry.dropScope(SCOPE);
    }

    /**
     * An Avro {@link Order} held in the buffer while waiting for a {@link Price} dependency is
     * Avro-serialised into the store under the {@code orders-value} Schema Registry subject (its
     * source topic), and is deserialized back to an equal {@code Order} when it drains — never
     * under the buffer-store or changelog subject name.
     *
     * <p>The test uses a shared {@link SpecificAvroSerde} instance wired to an in-process
     * {@link MockSchemaRegistry}, so every buffer read/write hits the same scope. The registered
     * subjects are inspected directly after the driver closes to confirm subject isolation.
     *
     * Asserts that the held record is not delivered to the delegate before the Price arrives,
     * the buffer store contains exactly one entry while held, and after the Price drains it both
     * values arrive in causal order and the buffer is empty. Also asserts that {@code orders-value}
     * and {@code prices-value} subjects are registered and no subject contains {@code buffer} or
     * {@code changelog}.
     */
    @Test
    void heldAvroRecordRoundTripsThroughTheBufferUnderItsSourceTopicSubject() {
        // One serde instance, shared by both the input deserialisation (Consumed) and Parsley's
        // per-topic buffer function, so every path hits the same mock registry scope.
        SpecificAvroSerde<SpecificRecord> avro = specificAvroSerde();

        ProcessorSupplier<String, SpecificRecord, String, SpecificRecord> user = capturing();
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(List.of(PRICES, ORDERS), Consumed.with(Serdes.String(), avro))
                .process(CausalProcessors.builder(user)
                        .addBufferStore("parsley", CausalBufferLimit.ofSize(100))
                        .addBuffers(List.of(PRICES, ORDERS), Serdes.String(), avro)
                        .topicAdmin(TestTopicAdmin.of(Map.of(PRICES, PRICES_ID, ORDERS, ORDERS_ID)))
                        .build());
        Topology topology = builder.build();

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            TestInputTopic<String, SpecificRecord> prices =
                    driver.createInputTopic(PRICES, new StringSerializer(), avro.serializer());
            TestInputTopic<String, SpecificRecord> orders =
                    driver.createInputTopic(ORDERS, new StringSerializer(), avro.serializer());
            KeyValueStore<String, byte[]> bufferStore = driver.getKeyValueStore("parsley-buffer");

            Order order = new Order("o-1", "ACME", 5);

            // The order depends on prices-0@0, which has not arrived: it is held (Avro-serialised
            // into the buffer store), not delivered.
            orders.pipeInput(new TestRecord<>("k", order, depsHeader(CausalDependencies.builder(TOPICS).require(PRICES, 0, 0).build())));
            assertTrue(processed.isEmpty(), "held record must not reach the delegate");
            assertEquals(1, storeSize(bufferStore), "held record must be persisted (Avro bytes) to the buffer store");

            // The price arrives and advances the frontier, draining the order back through the codec.
            Price price = new Price("ACME", 42.5);
            prices.pipeInput(new TestRecord<>("k", price, depsHeader(CausalDependencies.empty())));

            assertEquals(List.of(price, order), processed,
                    "the price is admitted, then the order drains as an Avro Order equal to the original");
            assertEquals(0, storeSize(bufferStore), "drained record must be removed from the buffer store");
        }

        // Per-topic subject resolution: the held order round-tripped under orders-value (its source
        // topic), never under the buffer store / changelog name.
        List<String> subjects = registeredSubjects();
        assertTrue(subjects.contains("orders-value"),
                "the buffered order must register the orders-value subject; got " + subjects);
        assertTrue(subjects.contains("prices-value"),
                "the prices topic must register the prices-value subject; got " + subjects);
        assertTrue(subjects.stream().noneMatch(s -> s.contains("buffer") || s.contains("changelog")),
                "no subject may be derived from the buffer/changelog name; got " + subjects);
    }

    /**
     * When a held Avro record can no longer be deserialised on drain — modelling the registry's schema
     * for its subject changing incompatibly while the record was buffered — Parsley fails fast with a
     * typed {@link ParsleyBufferDeserializationException} (a {@link RuntimeException}, so the JVM is
     * never crashed) and leaves the record in the buffer for recovery rather than dropping it.
     *
     * <p>The buffer serialises the held {@link Order} with the real {@link SpecificAvroSerde} (genuine
     * Confluent wire bytes, schema registered under {@code orders-value}), so the failure path also
     * exercises the writer-schema-id extraction in the exception. Only the drain-time decode is forced
     * to fail, via a serde whose deserializer throws.
     *
     * Asserts that the drain raises {@code ParsleyBufferDeserializationException}, naming the source
     * topic {@code orders}, and that the undecodable record remains in the buffer store.
     */
    @Test
    void heldAvroRecordThatCanNoLongerBeDecodedFailsFastAndStaysBuffered() {
        SpecificAvroSerde<SpecificRecord> avro = specificAvroSerde();

        try (TopologyTestDriver driver = new TopologyTestDriver(poisonTopology(avro, failPolicy()), config())) {
            KeyValueStore<String, byte[]> bufferStore = driver.getKeyValueStore("parsley-buffer");
            bufferAnUndecodableOrder(driver, avro);
            assertEquals(1, storeSize(bufferStore), "the order must be buffered before the price arrives");

            // The price advances the frontier and triggers the drain — which fails to decode the order.
            Exception thrown = org.junit.jupiter.api.Assertions.assertThrows(Exception.class,
                    () -> arrivePrice(driver, avro),
                    "an undecodable held record must surface an exception on drain");

            ParsleyBufferDeserializationException cause = causeOfType(thrown, ParsleyBufferDeserializationException.class);
            assertTrue(cause != null, "the failure must be a typed ParsleyBufferDeserializationException; got " + thrown);
            assertEquals(ORDERS, cause.topic(), "the exception must name the held record's source topic");
            assertEquals(1, storeSize(bufferStore), "the undecodable record must remain buffered for recovery");
        }
    }

    /**
     * The same scenario, but with {@code parsley.buffer.deserialization.failure.policy = continue}:
     * Parsley <strong>skips</strong> the undecodable held record on drain instead of failing — the
     * price is still delivered, the order is dropped from the buffer, and no exception propagates.
     * Proves the processor reads the policy from {@link ParsleyConfig} and threads it into the engine.
     */
    @Test
    void heldAvroRecordIsSkippedWhenPolicyIsContinue() {
        SpecificAvroSerde<SpecificRecord> avro = specificAvroSerde();

        try (TopologyTestDriver driver = new TopologyTestDriver(poisonTopology(avro, continuePolicy()), config())) {
            KeyValueStore<String, byte[]> bufferStore = driver.getKeyValueStore("parsley-buffer");
            bufferAnUndecodableOrder(driver, avro);
            assertEquals(1, storeSize(bufferStore), "the order must be buffered before the price arrives");

            // In continue-mode the drain skips the undecodable order rather than throwing.
            org.junit.jupiter.api.Assertions.assertDoesNotThrow(() -> arrivePrice(driver, avro),
                    "continue-mode must skip the undecodable record, not propagate the failure");

            assertEquals(List.of(new Price("ACME", 42.5)), processed,
                    "the price is delivered; the undecodable order is dropped, never delivered");
            assertEquals(0, storeSize(bufferStore), "the skipped order must be removed from the buffer");
        }
    }

    // --- helpers -----------------------------------------------------------------------------

    /** Topology whose ORDERS buffer serde writes real Avro but throws on read (registry change). */
    private Topology poisonTopology(SpecificAvroSerde<SpecificRecord> avro, ParsleyConfig config) {
        Serde<SpecificRecord> undecodableOnRead = new Serde<>() {
            @Override public org.apache.kafka.common.serialization.Serializer<SpecificRecord> serializer() {
                return avro.serializer();
            }
            @Override public org.apache.kafka.common.serialization.Deserializer<SpecificRecord> deserializer() {
                return (topic, data) -> {
                    throw new org.apache.kafka.common.errors.SerializationException(
                            "incompatible schema for subject " + topic + "-value");
                };
            }
        };
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(List.of(PRICES, ORDERS), Consumed.with(Serdes.String(), avro))
                .process(CausalProcessors.builder(capturing()).addBufferStore("parsley", CausalBufferLimit.ofSize(100))
                        .addBuffer(CausalBuffer.of(PRICES, Serdes.String(), avro))
                        .addBuffer(CausalBuffer.of(ORDERS, Serdes.String(), undecodableOnRead))
                        .topicAdmin(TestTopicAdmin.of(Map.of(PRICES, PRICES_ID, ORDERS, ORDERS_ID)))
                        .config(config)
                        .build());
        return builder.build();
    }

    private static ParsleyConfig failPolicy() {
        return ParsleyConfig.from(new Properties());  // default policy = fail
    }

    private static ParsleyConfig continuePolicy() {
        Properties props = new Properties();
        props.put(ParsleyConfig.DESERIALIZATION_FAILURE_POLICY, "continue");
        return ParsleyConfig.from(props);
    }

    /** Feeds an Order depending on prices-0@0 (not yet arrived) → held, serialised with real Avro. */
    private static void bufferAnUndecodableOrder(TopologyTestDriver driver, SpecificAvroSerde<SpecificRecord> avro) {
        TestInputTopic<String, SpecificRecord> orders =
                driver.createInputTopic(ORDERS, new StringSerializer(), avro.serializer());
        orders.pipeInput(new TestRecord<>("k", new Order("o-1", "ACME", 5),
                depsHeader(CausalDependencies.builder(TOPICS).require(PRICES, 0, 0).build())));
    }

    /** The price (empty deps) advances the frontier and triggers the drain of the held order. */
    private static void arrivePrice(TopologyTestDriver driver, SpecificAvroSerde<SpecificRecord> avro) {
        TestInputTopic<String, SpecificRecord> prices =
                driver.createInputTopic(PRICES, new StringSerializer(), avro.serializer());
        prices.pipeInput(new TestRecord<>("k", new Price("ACME", 42.5), depsHeader(CausalDependencies.empty())));
    }

    private static <T extends Throwable> T causeOfType(Throwable thrown, Class<T> type) {
        for (Throwable t = thrown; t != null; t = t.getCause()) {
            if (type.isInstance(t)) return type.cast(t);
        }
        return null;
    }

    /** A user processor that records every (Avro) value it sees and forwards it unchanged. */
    private ProcessorSupplier<String, SpecificRecord, String, SpecificRecord> capturing() {
        return () -> new Processor<>() {
            private ProcessorContext<String, SpecificRecord> ctx;

            @Override
            public void init(ProcessorContext<String, SpecificRecord> context) {
                this.ctx = context;
            }

            @Override
            public void process(Record<String, SpecificRecord> record) {
                processed.add(record.value());
                ctx.forward(record);
            }
        };
    }

    private static SpecificAvroSerde<SpecificRecord> specificAvroSerde() {
        SpecificAvroSerde<SpecificRecord> serde = new SpecificAvroSerde<>();
        serde.configure(Map.of(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, REGISTRY_URL,
                KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true), false);
        return serde;
    }

    private static List<String> registeredSubjects() {
        try {
            return new ArrayList<>(MockSchemaRegistry.getClientForScope(SCOPE).getAllSubjects());
        } catch (Exception e) {
            throw new IllegalStateException("could not read mock registry subjects", e);
        }
    }

    private static Properties config() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "avro-decorator-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return props;
    }

    private static Headers depsHeader(CausalDependencies deps) {
        Headers headers = ParsleyHeader.mutableHeaders();
        headers.add("parsley-causal-dependencies", deps.toBytes());
        return headers;
    }

    private static int storeSize(KeyValueStore<String, byte[]> store) {
        int n = 0;
        try (var it = store.all()) {
            while (it.hasNext()) {
                it.next();
                n++;
            }
        }
        return n;
    }
}