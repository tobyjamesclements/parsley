package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static java.util.Objects.requireNonNull;

/**
 * End-to-end proof, against a real broker, of the two frontier guarantees a fan-in/fan-out pipeline
 * relies on:
 *
 * <ol>
 *   <li><strong>An upstream client reliably maintains a max frontier.</strong> A plain
 *       {@link KafkaConsumer} reading several topics folds every record into one running
 *       {@link CausalClock} with {@link CausalClock#observe(ConsumerRecord)}, ending at
 *       the per-coordinate maximum of everything it has seen, and stamps that onto its outbound
 *       records.</li>
 *   <li><strong>Each processor maintains its own contiguous, scoped frontier.</strong> Two
 *       independent causal processors share one input topic but also each read a topic unique to them.
 *       Each stamps forwarded records with the high-water of <em>only</em> its own source topics: the
 *       other processor's unique topic never leaks in.</li>
 * </ol>
 *
 * <p>Every topic is single-partition, so source offsets are deterministic and the records are chained
 * by dependency, which makes each processor's admission frontier — and therefore each stamp's
 * input-topic entries — exact rather than racy. A stamp's own-OUTPUT-topic entry, which it carries
 * because the stamp is {@code completeness ∪ ownOutputs}, is asserted structurally instead, since
 * its exact offset depends on where EOS commit markers land in the output log.
 */
@Testcontainers(disabledWithoutDocker = true)
class CausalFanOutScopedFrontierIT {

    @Container
    private final KafkaContainer kafka =
            new KafkaContainer(ParsleyBrokerImage.get());

    // Upstream source topics the client consumes to build its max frontier.
    private static final String SRC1 = "src-1";
    private static final String SRC2 = "src-2";

    // Topics the two processors read: SHARED is consumed by both, A_ONLY/B_ONLY each by one.
    private static final String SHARED = "shared";
    private static final String A_ONLY = "a-only";
    private static final String B_ONLY = "b-only";

    // Per-processor output topics.
    private static final String A_OUT = "a-out";
    private static final String B_OUT = "b-out";

    /**
     * A client consuming several topics with a plain {@link KafkaConsumer} maintains a single running
     * frontier by {@link CausalClock#observe(ConsumerRecord) observing} every record it reads.
     * After draining three records from {@code SRC1} and two from {@code SRC2}, the accumulated
     * frontier must be the per-coordinate maximum ({@code SRC1@2}, {@code SRC2@1}), and stamping an
     * outbound record must carry exactly that frontier.
     *
     * Asserts the folded frontier equals the max across all consumed positions, and that it is what
     * gets stamped onto a produced record.
     */
    @Test
    void consumerMaintainsMaxFrontierAcrossTopics() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        createTopics(bootstrap, SRC1, SRC2, A_ONLY);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig(bootstrap))) {
            producer.send(new ProducerRecord<>(SRC1, "k", "s1-0")).get();
            producer.send(new ProducerRecord<>(SRC1, "k", "s1-1")).get();
            producer.send(new ProducerRecord<>(SRC1, "k", "s1-2")).get();
            producer.send(new ProducerRecord<>(SRC2, "k", "s2-0")).get();
            producer.send(new ProducerRecord<>(SRC2, "k", "s2-1")).get();
        }

        Properties resolverProps = new Properties();
        resolverProps.put("bootstrap.servers", bootstrap);
        ParsleyTopics topics = ParsleyTopics.of(resolverProps);

        List<ConsumerRecord<String, String>> consumed = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig(bootstrap))) {
            consumer.subscribe(List.of(SRC1, SRC2));
            await().atMost(Duration.ofSeconds(60)).until(() -> {
                consumer.poll(Duration.ofMillis(500)).forEach(consumed::add);
                return consumed.size() >= 5;
            });
        }

        // Fold every consumed record into one running frontier — the consumer-side max frontier.
        CausalClock frontier = CausalClock.using(topics);
        for (ConsumerRecord<String, String> record : consumed) {
            frontier = frontier.observe(record);
        }

        CausalClock expected = CausalClock.builder(topics)
                .require(SRC1, 0, 2)
                .require(SRC2, 0, 1)
                .build();
        assertEquals(expected, frontier,
                "the maintained frontier must be the per-coordinate max of every consumed position");

        // The frontier the client maintains is exactly what rides outbound records it produces.
        ProducerRecord<String, String> outbound =
                frontier.stamp(new ProducerRecord<>(A_ONLY, "k", "v"));
        assertEquals(Optional.of(expected), CausalClock.fromHeaders(outbound.headers()),
                "the maintained max frontier must be what gets stamped onto outbound records");
    }

    /**
     * Two independent causal processors share the input topic {@code SHARED} while each also reads a
     * topic unique to it ({@code A_ONLY} for processor A, {@code B_ONLY} for B). They run as two
     * separate Streams applications (distinct application ids, so distinct consumer groups) — Kafka
     * Streams forbids two source nodes within one topology from subscribing to overlapping-but-unequal
     * topic sets, and independent applications are anyway the faithful model of two independent
     * processors. The unique-topic records are produced first, each depending on {@code SHARED@1} —
     * every record is checked against this node's actual, already-proven state, never against its own
     * declared claim, so each is genuinely held until its own processor's direct consumption of
     * {@code SHARED} actually, contiguously reaches offset 1.
     *
     * <p>Completeness is this processor's own frontier max-merged with every input channel's advertised
     * dependencies ({@link ParsleyChannels#completeness()} / {@link ParsleyVectorClock#merge}). Once
     * {@code SHARED@1} genuinely delivers, the same {@code receive} call's cascade
     * ({@link ParsleyCausalBroadcast#receive}'s {@code propagate()}) releases the held unique-topic record in
     * the same pass — so both records are stamped with the same post-cascade completeness, and
     * {@code SHARED@1}'s own stamp already carries the unique-topic coordinate too. Each processor's
     * own unique topic is that processor's own directly-consumed coordinate — part of its own
     * contiguous frontier — so once delivered, every subsequent stamp from that processor carries it.
     * Neither unique topic ever leaks into the sibling processor's stamp, since neither channel nor
     * frontier for the sibling ever observes it.
     */
    @Test
    void scopedFrontiersStayIndependentAcrossTwoProcessorsSharingATopic() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        createTopics(bootstrap, SHARED, A_ONLY, B_ONLY, A_OUT, B_OUT);

        // Two independent applications: each reads the shared topic plus its own unique topic, into
        // its own buffer-store namespace, forwarding to its own output topic.
        CausalStreams streamsA = new CausalStreams(
                scopedTopology(SHARED, A_ONLY, A_OUT, "alpha"), streamsConfig(bootstrap));
        CausalStreams streamsB = new CausalStreams(
                scopedTopology(SHARED, B_ONLY, B_OUT, "beta"), streamsConfig(bootstrap));
        try (streamsA; streamsB) {
            streamsA.start();
            streamsB.start();

            Properties resolverProps = new Properties();
            resolverProps.put("bootstrap.servers", bootstrap);
            ParsleyTopics topics = ParsleyTopics.of(resolverProps);

            CausalClock afterShared1 = CausalClock.builder(topics).require(SHARED, 0, 1).build();

            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig(bootstrap))) {
                // Unique-topic records depend on SHARED@1 — genuinely buffered until it arrives.
                producer.send(afterShared1.stamp(new ProducerRecord<>(A_ONLY, "ak", "a0"))).get();
                producer.send(afterShared1.stamp(new ProducerRecord<>(B_ONLY, "bk", "b0"))).get();
                // SHARED@0 then SHARED@1, in that order, advancing both scoped frontiers contiguously
                // and releasing the held unique-topic records once SHARED@1 genuinely delivers.
                producer.send(CausalClock.empty().stamp(new ProducerRecord<>(SHARED, "sk", "s0"))).get();
                producer.send(CausalClock.empty().stamp(new ProducerRecord<>(SHARED, "sk", "s1"))).get();
            }

            // SHARED@1's delivery and the cascade release of the held unique-topic record happen in
            // the same receive call, so both are stamped with the same post-cascade input frontier:
            // SHARED@1 plus this processor's own unique-topic coordinate. A stamp is
            // completeness ∪ ownOutputs, so every stamp after a processor's first emission also
            // carries its OWN OUTPUT topic's acked position — asserted structurally rather than
            // by exact value, because the claimed offset depends on where EOS commit markers land.
            try (KafkaConsumer<String, byte[]> aConsumer = new KafkaConsumer<>(stampConsumerConfig(bootstrap));
                 KafkaConsumer<String, byte[]> bConsumer = new KafkaConsumer<>(stampConsumerConfig(bootstrap))) {
                aConsumer.subscribe(List.of(A_OUT));
                bConsumer.subscribe(List.of(B_OUT));
                Map<String, CausalClock> aStamps = pollStamps(aConsumer, 3);
                Map<String, CausalClock> bStamps = pollStamps(bConsumer, 3);

                for (String output : List.of("S1", "A0")) {
                    ParsleyVectorClock stamp = clockOf(requireNonNull(aStamps.get(output)));
                    assertEquals(1L, stamp.offsetFor(topics.topicId(SHARED), 0),
                            "processor A's " + output + " stamp must carry SHARED@1 — its genuine "
                                    + "dependency, reached only once A's own SHARED channel got there");
                    assertEquals(0L, stamp.offsetFor(topics.topicId(A_ONLY), 0),
                            "processor A's " + output + " stamp must carry A_ONLY@0: the held "
                                    + "unique-topic record was released by the same receive cascade "
                                    + "that delivered SHARED@1, so both share one input frontier");
                    assertEquals(-1L, stamp.offsetFor(topics.topicId(B_ONLY), 0),
                            "processor A's " + output + " stamp must never carry B_ONLY, which this "
                                    + "processor never observes — the scoped-frontier guarantee");
                }
                for (String output : List.of("S1", "B0")) {
                    ParsleyVectorClock stamp = clockOf(requireNonNull(bStamps.get(output)));
                    assertEquals(1L, stamp.offsetFor(topics.topicId(SHARED), 0),
                            "processor B's " + output + " stamp must carry SHARED@1");
                    assertEquals(0L, stamp.offsetFor(topics.topicId(B_ONLY), 0),
                            "processor B's " + output + " stamp must carry B_ONLY@0 — released by the "
                                    + "same cascade as SHARED@1's delivery");
                    assertEquals(-1L, stamp.offsetFor(topics.topicId(A_ONLY), 0),
                            "processor B's " + output + " stamp must never carry A_ONLY — the "
                                    + "scoped-frontier guarantee");
                }

                // Each processor's stamps carry its own output topic's acked
                // position, and the record forwarded LATER in the cascade (A0, after S1's output was
                // sent and acked) claims a strictly higher own-output position than S1's stamp did —
                // process order made provable by the crossing wait.
                long aOutClaimAtS1 = clockOf(requireNonNull(aStamps.get("S1"))).offsetFor(topics.topicId(A_OUT), 0);
                long aOutClaimAtA0 = clockOf(requireNonNull(aStamps.get("A0"))).offsetFor(topics.topicId(A_OUT), 0);
                assertTrue(aOutClaimAtS1 >= 0,
                        "processor A's S1 stamp must claim its own output topic's acked position "
                                + "(completeness ∪ ownOutputs) but claimed nothing");
                assertTrue(aOutClaimAtA0 > aOutClaimAtS1,
                        "the cascade's second forward (A0) must claim a strictly higher own-output "
                                + "position than the first (S1): the crossing wait folds the first "
                                + "forward's ack before the second is stamped; got " + aOutClaimAtS1
                                + " then " + aOutClaimAtA0);
            }
        }
    }

    /**
     * Builds a single-processor topology reading {@code shared} plus {@code unique} into the named
     * buffer-store namespace and forwarding to {@code out}. Each processor's frontier is scoped to
     * exactly these two source topics.
     */
    private static CausalTopology scopedTopology(String shared, String unique, String out, String namespace) {
        return new CausalStreamsBuilder()
                .stream(List.of(shared, unique), Serdes.String(), Serdes.String())
                .process(namespace, upperCaser())
                .to(out, Serdes.String(), Serdes.String())
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

    /** Polls until {@code count} stamped records are seen, keyed by their (upper-cased) value. */
    /** The underlying vector clock of a wire stamp, for per-entry assertions. */
    private static ParsleyVectorClock clockOf(CausalClock stamp) {
        return ParsleyVectorClock.fromBytes(stamp.toBytes());
    }

    private static Map<String, CausalClock> pollStamps(KafkaConsumer<String, byte[]> consumer, int count) {
        Map<String, CausalClock> byValue = new HashMap<>();
        await().atMost(Duration.ofSeconds(60)).until(() -> {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, byte[]> record : records) {
                byValue.put(new String(record.value()),
                        CausalClock.fromHeaders(record.headers()).orElseThrow());
            }
            return byValue.size() >= count;
        });
        return byValue;
    }

    private static Properties streamsConfig(String bootstrap) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "fanout-it-" + UUID.randomUUID());
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
                ConsumerConfig.GROUP_ID_CONFIG, "src-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    }

    private static Map<String, Object> stampConsumerConfig(String bootstrap) {
        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ConsumerConfig.GROUP_ID_CONFIG, "out-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
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
