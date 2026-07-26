package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static java.util.Objects.requireNonNull;

/**
 * The shared-sink scenario the two-branch gate closes: app A both produces to and consumes the
 * {@code shared} topic, which app B also produces to. A gate-side own-sink strip ("it's my sink,
 * strip the claim") would satisfy a claim about <em>B's</em> record on that topic vacuously, even
 * though it is a genuine cause A may not have delivered yet — the blindspot that makes such a strip
 * unsound. Under the two-branch gate the shared topic is simply one of A's consumed channels: the
 * claim gates on A's own frontier like any other dependency, so A delivers B's record before any
 * effect derived from it.
 *
 * <p>Topology: {@code ty} → app B → {@code shared}; app C consumes {@code shared} and re-keys
 * B-derived records to {@code c1}; app A consumes {@code c1} and {@code shared}, produces
 * {@code shared} (from seed records) and tags every delivery to {@code observed}.
 */
@Testcontainers(disabledWithoutDocker = true)
class ParsleySharedSinkOrderingIT {

    @Container
    private final KafkaContainer kafka =
            new KafkaContainer(ParsleyBrokerImage.get());

    private static final String C1 = "c1";
    private static final String TY = "ty";
    private static final String SHARED = "shared";
    private static final String OBSERVED = "observed";

    /**
     * The effect record C derives from B's shared-topic record must carry a wire claim on that
     * record's exact shared coordinate, and A — for which {@code shared} is both a sink and an
     * input — must deliver B's shared record before the derived effect arriving on {@code c1}:
     * a claim about another producer's record on A's own sink topic is genuinely gated, never
     * vacuously satisfied (finding (iii)).
     *
     * Asserts the effect's clock names B's shared coordinate and A's observed output shows the
     * shared cause strictly before the c1 effect.
     */
    @Test
    void claimAboutAnotherProducersRecordOnOwnSharedSinkGatesInsteadOfVacuouslySatisfying() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        createTopics(bootstrap, C1, TY, SHARED, OBSERVED);
        Uuid sharedId = topicId(bootstrap, SHARED);

        // App A: consumes c1 and shared (shared is ALSO its declared sink — the finding-(iii)
        // shape); a "seed:" c1 record forwards to the shared sink, and every delivery is tagged
        // to observed.
        CausalTopology aTopology = new CausalStreamsBuilder()
                .stream(List.of(C1, SHARED), Serdes.String(), Serdes.String())
                .process(appAProcessor())
                .to("shared-sink", SHARED, Serdes.String(), Serdes.String())
                .to("observed-sink", OBSERVED, Serdes.String(), Serdes.String())
                .build();
        // App B: the sibling producer on the shared topic.
        CausalTopology bTopology = new CausalStreamsBuilder()
                .stream(List.of(TY), Serdes.String(), Serdes.String())
                .process(prefixingProcessor("b:"))
                .to(SHARED, Serdes.String(), Serdes.String())
                .build();
        // App C: consumes the shared topic and derives an effect onto c1 from B's records only
        // (A's own shared records are not re-derived, so the cycle terminates).
        CausalTopology cTopology = new CausalStreamsBuilder()
                .stream(List.of(SHARED), Serdes.String(), Serdes.String())
                .process(deriveFromBProcessor())
                .to(C1, Serdes.String(), Serdes.String())
                .build();

        try (CausalStreams a = new CausalStreams(aTopology, streamsConfig(bootstrap, "shared-a"));
             CausalStreams b = new CausalStreams(bTopology, streamsConfig(bootstrap, "shared-b"));
             CausalStreams c = new CausalStreams(cTopology, streamsConfig(bootstrap, "shared-c"))) {
            a.start();
            b.start();
            c.start();

            try (KafkaProducer<String, String> input = new KafkaProducer<>(producerConfig(bootstrap))) {
                // A seed for A so `shared` is genuinely A's sink in this run, then the trigger
                // for the B → C → A causal chain.
                input.send(CausalClock.empty().stamp(new ProducerRecord<>(C1, "k", "seed:x"))).get();
                input.send(CausalClock.empty().stamp(new ProducerRecord<>(TY, "k", "y"))).get();
            }

            // The wire evidence: the effect C derives claims B's exact shared coordinate.
            List<ConsumerRecord<String, String>> sharedRecords = new ArrayList<>();
            List<ConsumerRecord<String, String>> effectRecords = new ArrayList<>();
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig(bootstrap))) {
                consumer.subscribe(List.of(SHARED, C1));
                await().atMost(Duration.ofSeconds(90)).until(() -> {
                    consumer.poll(Duration.ofMillis(500)).forEach(record -> {
                        if (isBusinessRecord(record)) {
                            if (record.topic().equals(SHARED) && record.value().startsWith("b:")) {
                                sharedRecords.add(record);
                            }
                            if (record.topic().equals(C1) && record.value().startsWith("c:")) {
                                effectRecords.add(record);
                            }
                        }
                    });
                    return !sharedRecords.isEmpty() && !effectRecords.isEmpty();
                });
            }
            ConsumerRecord<String, String> cause = sharedRecords.get(0);
            ConsumerRecord<String, String> effect = effectRecords.get(0);
            assertTrue(wireClock(effect).offsetFor(sharedId, 0) >= cause.offset(),
                    "the derived effect's clock must claim B's shared record at or above offset "
                            + cause.offset() + " — the cause A must gate on");

            // The delivery evidence: A delivers the sibling's shared cause before the effect.
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig(bootstrap))) {
                consumer.subscribe(List.of(OBSERVED));
                List<String> observed = new ArrayList<>();
                await().atMost(Duration.ofSeconds(90)).until(() -> {
                    consumer.poll(Duration.ofMillis(500)).forEach(record -> {
                        if (isBusinessRecord(record)) {
                            observed.add(record.value());
                        }
                    });
                    return observed.contains(SHARED + ":b:y") && observed.contains(C1 + ":c:b:y");
                });
                assertTrue(observed.indexOf(SHARED + ":b:y") < observed.indexOf(C1 + ":c:b:y"),
                        "A must deliver another producer's shared-sink record before the effect "
                                + "derived from it — never vacuously satisfy the claim because the "
                                + "topic is A's own sink (finding (iii)); observed: " + observed);
            }
        }
    }

    /**
     * App A's delegate: a {@code seed:}-prefixed {@code c1} record forwards {@code a:<value>} to
     * the shared sink (making {@code shared} a genuine sink of A); every delivery — whatever its
     * source — is also tagged {@code <sourceTopic>:<value>} to the observed sink.
     */
    private static ProcessorSupplier<String, String, String, String> appAProcessor() {
        return () -> new Processor<>() {
            private ProcessorContext<String, String> ctx;

            @Override
            public void init(ProcessorContext<String, String> context) {
                this.ctx = context;
            }

            @Override
            public void process(Record<String, String> record) {
                String source = ctx.recordMetadata().orElseThrow().topic();
                ctx.forward(record.withValue(source + ":" + record.value()), "observed-sink");
                if (C1.equals(source) && record.value().startsWith("seed:")) {
                    ctx.forward(record.withValue("a:" + record.value()), "shared-sink");
                }
            }
        };
    }

    /** A delegate that forwards every delivery with {@code prefix} prepended to its value. */
    private static ProcessorSupplier<String, String, String, String> prefixingProcessor(String prefix) {
        return () -> new Processor<>() {
            private ProcessorContext<String, String> ctx;

            @Override
            public void init(ProcessorContext<String, String> context) {
                this.ctx = context;
            }

            @Override
            public void process(Record<String, String> record) {
                ctx.forward(record.withValue(prefix + record.value()));
            }
        };
    }

    /** App C's delegate: derives {@code c:<value>} onto c1 from B's shared records only. */
    private static ProcessorSupplier<String, String, String, String> deriveFromBProcessor() {
        return () -> new Processor<>() {
            private ProcessorContext<String, String> ctx;

            @Override
            public void init(ProcessorContext<String, String> context) {
                this.ctx = context;
            }

            @Override
            public void process(Record<String, String> record) {
                if (record.value().startsWith("b:")) {
                    ctx.forward(record.withValue("c:" + record.value()));
                }
            }
        };
    }

    /** A record with a value and no Parsley marker header — a business record, not a null message. */
    private static boolean isBusinessRecord(ConsumerRecord<String, String> record) {
        return record.value() != null && record.headers().lastHeader(ParsleyHeader.NULL_MESSAGE) == null;
    }

    private static ParsleyVectorClock wireClock(ConsumerRecord<String, String> record) {
        Header header = record.headers().lastHeader(ParsleyHeader.CAUSAL_CLOCK);
        assertNotNull(header, "every stamped business record must carry the causal-dependencies header");
        return ParsleyVectorClock.fromBytes(header.value());
    }

    private static Uuid topicId(String bootstrap, String topic) throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap))) {
            return requireNonNull(admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic)).topicId();
        }
    }

    private static Properties streamsConfig(String bootstrap, String appPrefix) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appPrefix + "-" + UUID.randomUUID());
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
