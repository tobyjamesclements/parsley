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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The #22 scenario, end to end against a real broker (T2.3 IT a): a stage that consumes its own
 * sink topic must stamp that sink's coordinates onto its <em>other</em> outputs, so a distinct
 * third-party app consuming both topics sees the true ancestry and delivers cause before effect.
 * Before T2.3 the stamp-side own-sink strip erased exactly that ancestor: app A's derived output
 * carried no claim on the shared topic, and the third party could deliver the effect first.
 *
 * <p>Topology: app A consumes {@code C1} and {@code shared} and produces {@code shared} (from C1
 * records) and {@code derived} (from shared records) — {@code shared} is both A's sink and A's
 * input. Third-party app B consumes all three and forwards each delivery, tagged with its source
 * topic, to {@code observed}.
 */
@Testcontainers(disabledWithoutDocker = true)
class ParsleyOwnSinkAncestryIT {

    @Container
    private final KafkaContainer kafka =
            new KafkaContainer(ParsleyBrokerImage.get());

    private static final String C1 = "c1";
    private static final String SHARED = "shared";
    private static final String DERIVED = "derived";
    private static final String OBSERVED = "observed";

    /**
     * App A's derived-topic record must carry a dependency claim on the shared-sink record it was
     * derived from (the stamp is {@code completeness ∪ ownOutputs} — no own-sink strip), and app B
     * must deliver the shared record before the derived record that depends on it.
     *
     * Asserts the derived record's wire clock names the shared record's coordinate, and B's
     * observed output shows the shared delivery strictly before the derived delivery.
     */
    @Test
    void thirdPartyConsumingASharedSinkSeesTrueAncestryAndCausalOrder() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        createTopics(bootstrap, C1, SHARED, DERIVED, OBSERVED);
        Uuid sharedId = topicId(bootstrap, SHARED);

        CausalTopology producerTopology = new CausalStreamsBuilder()
                .stream(List.of(C1, SHARED), Serdes.String(), Serdes.String())
                .process(routingProcessor())
                .to("shared-sink", SHARED, Serdes.String(), Serdes.String())
                .to("derived-sink", DERIVED, Serdes.String(), Serdes.String())
                .build();
        CausalTopology observerTopology = new CausalStreamsBuilder()
                .stream(List.of(C1, SHARED, DERIVED), Serdes.String(), Serdes.String())
                .process(taggingProcessor())
                .to(OBSERVED, Serdes.String(), Serdes.String())
                .build();

        try (CausalStreams producer = new CausalStreams(producerTopology, streamsConfig(bootstrap, "own-sink-a"));
             CausalStreams observer = new CausalStreams(observerTopology, streamsConfig(bootstrap, "own-sink-b"))) {
            producer.start();
            observer.start();

            try (KafkaProducer<String, String> input = new KafkaProducer<>(producerConfig(bootstrap))) {
                input.send(CausalClock.empty().stamp(new ProducerRecord<>(C1, "k", "hello"))).get();
            }

            // The wire evidence (#22): the derived business record's stamp claims the shared
            // record's coordinate — the ancestor the old stamp-side strip erased.
            List<ConsumerRecord<String, String>> sharedRecords = new ArrayList<>();
            List<ConsumerRecord<String, String>> derivedRecords = new ArrayList<>();
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig(bootstrap))) {
                consumer.subscribe(List.of(SHARED, DERIVED));
                await().atMost(Duration.ofSeconds(90)).until(() -> {
                    consumer.poll(Duration.ofMillis(500)).forEach(record -> {
                        if (isBusinessRecord(record)) {
                            (record.topic().equals(SHARED) ? sharedRecords : derivedRecords).add(record);
                        }
                    });
                    return !sharedRecords.isEmpty() && !derivedRecords.isEmpty();
                });
            }
            ConsumerRecord<String, String> sharedRecord = sharedRecords.get(0);
            ConsumerRecord<String, String> derivedRecord = derivedRecords.get(0);
            assertEquals("s:hello", sharedRecord.value(), "the shared record must be A's C1 derivation");
            assertEquals("d:s:hello", derivedRecord.value(), "the derived record must be A's shared derivation");
            ParsleyVectorClock derivedClock = wireClock(derivedRecord);
            assertTrue(derivedClock.offsetFor(sharedId, 0) >= sharedRecord.offset(),
                    "the derived record's stamp must claim its shared-sink ancestor at or above "
                            + sharedRecord.offset() + " (the #22 fix) but claimed "
                            + derivedClock.offsetFor(sharedId, 0));

            // The delivery evidence: B (a distinct app gating on both topics) delivers the shared
            // cause before the derived effect.
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig(bootstrap))) {
                consumer.subscribe(List.of(OBSERVED));
                List<String> observed = new ArrayList<>();
                await().atMost(Duration.ofSeconds(90)).until(() -> {
                    consumer.poll(Duration.ofMillis(500)).forEach(record -> {
                        if (isBusinessRecord(record)) {
                            observed.add(record.value());
                        }
                    });
                    return observed.contains(SHARED + ":s:hello") && observed.contains(DERIVED + ":d:s:hello");
                });
                assertTrue(observed.indexOf(SHARED + ":s:hello") < observed.indexOf(DERIVED + ":d:s:hello"),
                        "the third party must deliver the shared cause before the derived effect; "
                                + "observed order: " + observed);
            }
        }
    }

    /**
     * App A's delegate: a C1 record forwards {@code s:<value>} to the shared sink; a shared record
     * forwards {@code d:<value>} to the derived sink. Routing is by the delivered record's source
     * topic, addressed to the named sink so each output reaches exactly one topic.
     */
    private static ProcessorSupplier<String, String, String, String> routingProcessor() {
        return () -> new Processor<>() {
            private ProcessorContext<String, String> ctx;

            @Override
            public void init(ProcessorContext<String, String> context) {
                this.ctx = context;
            }

            @Override
            public void process(Record<String, String> record) {
                String source = ctx.recordMetadata().orElseThrow().topic();
                if (C1.equals(source)) {
                    ctx.forward(record.withValue("s:" + record.value()), "shared-sink");
                } else {
                    ctx.forward(record.withValue("d:" + record.value()), "derived-sink");
                }
            }
        };
    }

    /** App B's delegate: forwards every delivery tagged {@code <sourceTopic>:<value>}. */
    private static ProcessorSupplier<String, String, String, String> taggingProcessor() {
        return () -> new Processor<>() {
            private ProcessorContext<String, String> ctx;

            @Override
            public void init(ProcessorContext<String, String> context) {
                this.ctx = context;
            }

            @Override
            public void process(Record<String, String> record) {
                String source = ctx.recordMetadata().orElseThrow().topic();
                ctx.forward(record.withValue(source + ":" + record.value()));
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
            return admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic).topicId();
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
