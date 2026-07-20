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
import org.testcontainers.utility.DockerImageName;

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

/**
 * The two-branch gate's ignore branch across a chain with an unconsumed intermediate topic (T3.1
 * IT a): observer app Z consumes the chain's root ({@code t1}) and its terminus ({@code t3}) but
 * not the intermediate ({@code mid}), so every {@code t3} record's clock names {@code mid}
 * coordinates Z has no channel for. Before T3.1 that was a hard {@code
 * ParsleyUnreachableDependencyException} at Z's gate, and joining such a topology needed the
 * {@code domain-topics} passthrough; under the two-branch gate (D1) the {@code mid} entries fall
 * to the ignore branch while the same clock's {@code t1} entries — the transitively complete
 * custody the ignore's soundness rests on (I2/I9) — are genuinely gated, so Z still delivers the
 * root cause before the terminus effect.
 *
 * <p>Topology: {@code t1} → app A → {@code mid} → app B → {@code t3}; observer Z consumes
 * {@code t1} and {@code t3} only and tags every delivery to {@code observed}.
 */
@Testcontainers(disabledWithoutDocker = true)
class ParsleyUnconsumedIntermediateIT {

    @Container
    private final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"));

    private static final String T1 = "t1";
    private static final String MID = "mid";
    private static final String T3 = "t3";
    private static final String OBSERVED = "observed";

    /**
     * The {@code t3} record's wire clock must name both the unconsumed intermediate (the entries
     * Z ignores) and the root {@code t1} coordinate (the entry Z gates on), and Z must deliver
     * the {@code t1} cause before the {@code t3} effect — in causal order, with no passthrough,
     * no coordination, and no failure.
     *
     * Asserts the t3 record's clock claims {@code mid} and {@code t1}, and Z's observed output
     * shows the t1 delivery strictly before the t3 delivery.
     */
    @Test
    void unconsumedIntermediateClaimsAreIgnoredAndTheRootStillGates() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        createTopics(bootstrap, T1, MID, T3, OBSERVED);
        Uuid t1Id = topicId(bootstrap, T1);
        Uuid midId = topicId(bootstrap, MID);

        CausalTopology aTopology = new CausalStreamsBuilder()
                .stream(List.of(T1), Serdes.String(), Serdes.String())
                .process(prefixingProcessor("a:"))
                .to(MID, Serdes.String(), Serdes.String())
                .build();
        CausalTopology bTopology = new CausalStreamsBuilder()
                .stream(List.of(MID), Serdes.String(), Serdes.String())
                .process(prefixingProcessor("b:"))
                .to(T3, Serdes.String(), Serdes.String())
                .build();
        CausalTopology observerTopology = new CausalStreamsBuilder()
                .stream(List.of(T1, T3), Serdes.String(), Serdes.String())
                .process(taggingProcessor())
                .to(OBSERVED, Serdes.String(), Serdes.String())
                .build();

        try (CausalStreams a = new CausalStreams(aTopology, streamsConfig(bootstrap, "chain-a"));
             CausalStreams b = new CausalStreams(bTopology, streamsConfig(bootstrap, "chain-b"));
             CausalStreams observer = new CausalStreams(observerTopology, streamsConfig(bootstrap, "chain-z"))) {
            a.start();
            b.start();
            observer.start();

            try (KafkaProducer<String, String> input = new KafkaProducer<>(producerConfig(bootstrap))) {
                input.send(CausalDependencies.empty().stamp(new ProducerRecord<>(T1, "k", "hello"))).get();
            }

            // The wire evidence: the terminus record's clock names the unconsumed intermediate
            // (what Z ignores) AND the root coordinate (what Z gates on — the I9 custody chain
            // through B, which never consumes t1 itself).
            List<ConsumerRecord<String, String>> t3Records = new ArrayList<>();
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig(bootstrap))) {
                consumer.subscribe(List.of(T3));
                await().atMost(Duration.ofSeconds(90)).until(() -> {
                    consumer.poll(Duration.ofMillis(500)).forEach(record -> {
                        if (isBusinessRecord(record)) {
                            t3Records.add(record);
                        }
                    });
                    return !t3Records.isEmpty();
                });
            }
            ParsleyVectorClock t3Clock = wireClock(t3Records.get(0));
            assertTrue(t3Clock.offsetFor(midId, 0) >= 0,
                    "the terminus record's clock must claim the unconsumed intermediate — the "
                            + "coordinate Z's ignore branch handles");
            assertTrue(t3Clock.offsetFor(t1Id, 0) >= 0,
                    "the terminus record's clock must claim the root t1 coordinate directly "
                            + "(I2/I9 custody through B) — the entry Z's consumed branch gates on");

            // The delivery evidence: Z ignores the mid claims and still delivers cause first.
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig(bootstrap))) {
                consumer.subscribe(List.of(OBSERVED));
                List<String> observed = new ArrayList<>();
                await().atMost(Duration.ofSeconds(90)).until(() -> {
                    consumer.poll(Duration.ofMillis(500)).forEach(record -> {
                        if (isBusinessRecord(record)) {
                            observed.add(record.value());
                        }
                    });
                    return observed.contains(T1 + ":hello") && observed.contains(T3 + ":b:a:hello");
                });
                assertTrue(observed.indexOf(T1 + ":hello") < observed.indexOf(T3 + ":b:a:hello"),
                        "Z must deliver the root cause before the terminus effect — the ignore "
                                + "branch must not weaken the consumed branch; observed: " + observed);
            }
        }
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

    /** Z's delegate: forwards every delivery tagged {@code <sourceTopic>:<value>}. */
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
        return record.value() != null && record.headers().lastHeader(ParsleyHeader.WATERMARK) == null;
    }

    private static ParsleyVectorClock wireClock(ConsumerRecord<String, String> record) {
        Header header = record.headers().lastHeader(ParsleyHeader.CAUSAL_DEPENDENCIES);
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
