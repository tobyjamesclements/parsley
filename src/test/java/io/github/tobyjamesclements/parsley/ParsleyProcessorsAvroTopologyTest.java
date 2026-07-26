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
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.jspecify.annotations.Nullable;

/**
 * Exercises the causal decorator with <strong>real Avro + Confluent Schema Registry</strong> serdes
 * over multiple input topics, each carrying its own schema, against an in-process
 * {@link MockSchemaRegistry} (no broker, no Docker).
 *
 * <p>The point under test is {@code ParsleyResolver}'s per-source-topic serde resolution: a held
 * {@link Order} must be Avro-serialised into the buffer store under the {@code c2-value} subject
 * (its source topic), then deserialised back to an equal {@code Order} when it drains — never under
 * the changelog/store name.
 */
class ParsleyProcessorsAvroTopologyTest {

    private static final String C1 = "c1";
    private static final String C2 = "c2";
    private static final Uuid PRICES_ID = Uuid.randomUuid();
    private static final Uuid ORDERS_ID = Uuid.randomUuid();
    private static final ParsleyTopics TOPICS = ParsleyTopics.of(Map.of(C1, PRICES_ID, C2, ORDERS_ID));

    private final List<SpecificRecord> processed = new ArrayList<>();

    // The scope is the part after mock:// — MockSchemaRegistry keys its in-JVM client by it. One
    // scope per test, since the registry is a JVM-wide map and the teardown drops a whole scope:
    // on a shared one, a test finishing drops the subjects a concurrent test is still asserting on.
    private final String scope = "parsley-avro-topology-" + UUID.randomUUID();
    private final String registryUrl = "mock://" + scope;

    @AfterEach
    void dropRegistryScope() {
        MockSchemaRegistry.dropScope(scope);
    }

    /**
     * An Avro {@link Order} held in the buffer while waiting for a {@link Price} dependency is
     * Avro-serialised into the store under the {@code c2-value} Schema Registry subject (its
     * source topic), and is deserialized back to an equal {@code Order} when it drains — never
     * under the buffer-store or changelog subject name.
     *
     * <p>The test uses a shared {@link SpecificAvroSerde} instance wired to an in-process
     * {@link MockSchemaRegistry}, so every buffer read/write hits the same scope. The registered
     * subjects are inspected directly after the driver closes to confirm subject isolation.
     *
     * Asserts that the held record is not delivered to the delegate before the Price arrives,
     * the buffer store contains exactly one entry while held, and after the Price drains it both
     * values arrive in causal order and the buffer is empty. Also asserts that {@code c2-value}
     * and {@code c1-value} subjects are registered and no subject contains {@code buffer} or
     * {@code changelog}.
     */
    @Test
    void heldAvroRecordRoundTripsThroughTheBufferUnderItsSourceTopicSubject() {
        // One serde instance, shared by both the input deserialisation (Consumed) and Parsley's
        // per-topic buffer function, so every path hits the same mock registry scope.
        SpecificAvroSerde<SpecificRecord> avro = specificAvroSerde();

        ProcessorSupplier<String, SpecificRecord, String, SpecificRecord> user = capturing();
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(List.of(C1, C2), Consumed.with(Serdes.String(), avro))
                .process(ParsleyProcessorSupplier.builder(user)
                        .addBufferStore("parsley")
                        .addSources(List.of(C1, C2), Serdes.String(), avro)
                        .topicAdmin(TestTopicAdmin.of(Map.of(C1, PRICES_ID, C2, ORDERS_ID)))
                        .build());
        Topology topology = builder.build();

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config())) {
            TestInputTopic<String, SpecificRecord> c1 =
                    driver.createInputTopic(C1, new StringSerializer(), avro.serializer());
            TestInputTopic<String, SpecificRecord> c2 =
                    driver.createInputTopic(C2, new StringSerializer(), avro.serializer());
            KeyValueStore<String, byte[]> bufferStore = driver.getKeyValueStore("parsley-buffer");

            Order order = new Order("o-1", "ACME", 5);

            // The order depends on c1-0@0, which has not arrived: it is held (Avro-serialised
            // into the buffer store), not delivered.
            c2.pipeInput(new TestRecord<>("k", order, depsHeader(CausalClock.builder(TOPICS).require(C1, 0, 0).build())));
            assertTrue(processed.isEmpty(), "held record must not reach the delegate");
            assertEquals(1, storeSize(bufferStore), "held record must be persisted (Avro bytes) to the buffer store");

            // The price arrives and advances the frontier, draining the order back through the codec.
            Price price = new Price("ACME", 42.5);
            c1.pipeInput(new TestRecord<>("k", price, depsHeader(CausalClock.empty())));

            assertEquals(List.of(price, order), processed,
                    "the price is admitted, then the order drains as an Avro Order equal to the original");
            assertEquals(0, storeSize(bufferStore), "drained record must be removed from the buffer store");
        }

        // Per-topic subject resolution: the held order round-tripped under c2-value (its source
        // topic), never under the buffer store / changelog name.
        List<String> subjects = registeredSubjects();
        assertTrue(subjects.contains("c2-value"),
                "the buffered order must register the c2-value subject; got " + subjects);
        assertTrue(subjects.contains("c1-value"),
                "the c1 topic must register the c1-value subject; got " + subjects);
        assertTrue(subjects.stream().noneMatch(s -> s.contains("buffer") || s.contains("changelog")),
                "no subject may be derived from the buffer/changelog name; got " + subjects);
    }

    /**
     * When a buffered Avro record can no longer be deserialised — modelling the registry's schema for
     * its subject changing incompatibly while the record was buffered — Parsley fails fast with a
     * typed {@link CausalBufferDeserializationException} (a {@link RuntimeException}, so the JVM is
     * never crashed) rather than dropping the record.
     *
     * <p>Exercised directly against {@link StoreBackedBufferStore} with the real {@link SpecificAvroSerde}
     * (genuine Confluent wire bytes, schema registered under {@code c2-value}), rather than
     * through a full topology: a record's own declared dependency is folded into its own channel
     * before its own gate check runs, so under single-witness merge it always proves itself
     * immediately, so a normal record's own dependency cannot force it to sit genuinely buffered at
     * the topology level. Constructing the buffered entry directly exercises the exact same
     * encode/decode path
     * ({@code StoreBackedBufferStore} + {@code ParsleySerializer} + real Avro wire bytes) that a genuinely
     * buffered record would have gone through, including the writer-schema-id extraction in the
     * exception.
     *
     * Asserts that decoding raises {@code CausalBufferDeserializationException}, naming the record's
     * source topic {@code c2}, and that {@code indexEntries()} (which never touches the value
     * serde) still succeeds despite the undecodable value.
     */
    @Test
    void undecodableBufferedAvroRecordFailsFastWithSourceTopicNamed() {
        SpecificAvroSerde<SpecificRecord> avro = specificAvroSerde();
        ParsleySerializer<String, SpecificRecord> writable =
                new ParsleySerializer<>(new ParsleyResolver<>(topic -> Serdes.String(), topic -> avro));
        KeyValueStore<Long, byte[]> backing = new TestKeyValueStore<Long, byte[]>(java.util.Comparator.naturalOrder());
        StoreBackedBufferStore<String, SpecificRecord> store = new StoreBackedBufferStore<>(backing, writable);

        ParsleyVectorClock deps = ParsleyVectorClock.empty().observe(PRICES_ID, 0, 0);
        ParsleyMessage<String, SpecificRecord> orderMessage = new ParsleyMessage<>(
                C2, ORDERS_ID, 0, 0, 0L, "k", new Order("o-1", "ACME", 5), List.of(), deps);
        long seq = store.add(orderMessage, 100L);

        // A second store over the same backing bytes, but with a deserializer that always throws —
        // modelling the registry schema for "c2-value" having changed incompatibly since.
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
        ParsleySerializer<String, SpecificRecord> unreadable =
                new ParsleySerializer<>(new ParsleyResolver<>(topic -> Serdes.String(), topic -> undecodableOnRead));
        StoreBackedBufferStore<String, SpecificRecord> poisonedView = new StoreBackedBufferStore<>(backing, unreadable);

        Exception thrown = org.junit.jupiter.api.Assertions.assertThrows(Exception.class,
                () -> poisonedView.get(seq), "an undecodable buffered record must surface an exception on decode");

        CausalBufferDeserializationException cause = causeOfType(thrown, CausalBufferDeserializationException.class);
        assertTrue(cause != null, "the failure must be a typed CausalBufferDeserializationException; got " + thrown);
        assertEquals(C2, cause.topic(), "the exception must name the record's source topic");

        List<ParsleyBufferStore.IndexEntry> indexEntries = org.junit.jupiter.api.Assertions.assertDoesNotThrow(
                poisonedView::indexEntries, "indexEntries() must not touch the value serde");
        assertEquals(1, indexEntries.size(), "the undecodable record's metadata must still be returned");
    }

    // --- helpers -----------------------------------------------------------------------------

    private static <T extends Throwable> @Nullable T causeOfType(Throwable thrown, Class<T> type) {
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

    private SpecificAvroSerde<SpecificRecord> specificAvroSerde() {
        SpecificAvroSerde<SpecificRecord> serde = new SpecificAvroSerde<>();
        serde.configure(Map.of(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl,
                KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true), false);
        return serde;
    }

    private List<String> registeredSubjects() {
        try {
            return new ArrayList<>(MockSchemaRegistry.getClientForScope(scope).getAllSubjects());
        } catch (Exception e) {
            throw new IllegalStateException("could not read mock registry subjects", e);
        }
    }

    /** A state directory per driver: the application id is fixed, so a shared one would collide. */
    @RegisterExtension
    static final TestStateDirectories STATE_DIRS = new TestStateDirectories("avro-decorator-test-");

    private static Properties config() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "avro-decorator-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.STATE_DIR_CONFIG, STATE_DIRS.create().toAbsolutePath().toString());
        return props;
    }

    private static Headers depsHeader(CausalClock deps) {
        Headers headers = ParsleyHeader.mutableHeaders();
        headers.add("parsley-causal-clock", deps.toBytes());
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