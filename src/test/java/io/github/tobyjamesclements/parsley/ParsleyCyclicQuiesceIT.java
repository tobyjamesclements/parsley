package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
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
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The tightest topology cycle against a real broker (T2.3 IT c): an app that consumes its own sink.
 * With the stamp-side own-sink strip gone (T2.3), relay settling rests entirely on the I6
 * knowledge-based rule — a null message is relayed only when its carried clock teaches the node
 * something outside {@code frontier ∪ channel clocks ∪ ownOutputs}. A reflected own coordinate is
 * dominated by {@code ownOutputs}, so the cycle must quiesce: after the input stops, the sink topic
 * must stop growing rather than ping-pong markers forever.
 */
@Testcontainers(disabledWithoutDocker = true)
class ParsleyCyclicQuiesceIT {

    @Container
    private final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"));

    private static final String T1 = "t1";
    private static final String LOOP = "loop";

    /**
     * After one input record flows through the cycle (T1 → loop → the app's own gate again), the
     * loop topic's end offset must stabilise — the I6 relay settles once nothing new is being
     * learned — and the app must still be running (the cycle neither crashes nor spins).
     *
     * Asserts the business record traverses the loop, the loop end offset then holds still for a
     * sustained window, and the Streams instance is still RUNNING at the end.
     */
    @Test
    void ownSinkCycleQuiescesAfterTheInputStops() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        createTopics(bootstrap, T1, LOOP);

        CausalTopology topology = new CausalStreamsBuilder()
                .stream(List.of(T1, LOOP), Serdes.String(), Serdes.String())
                .process(loopingProcessor())
                .to(LOOP, Serdes.String(), Serdes.String())
                .build();

        try (CausalStreams streams = new CausalStreams(topology, streamsConfig(bootstrap))) {
            streams.start();

            try (KafkaProducer<String, String> input = new KafkaProducer<>(producerConfig(bootstrap))) {
                input.send(CausalClock.empty().stamp(new ProducerRecord<>(T1, "k", "hello"))).get();
            }

            // The business record must traverse the cycle: T1's delivery forwards s:hello onto the
            // loop topic, and the app must then deliver its own sink record (a delay, never a
            // deadlock — the self-consumed sink's claims are genuinely gated by the consumed
            // branch, and reflected claims are ownOutputs-known at relay).
            List<String> loopValues = new ArrayList<>();
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig(bootstrap))) {
                consumer.subscribe(List.of(LOOP));
                await().atMost(Duration.ofSeconds(90)).until(() -> {
                    consumer.poll(Duration.ofMillis(500)).forEach(record -> {
                        if (record.value() != null && record.headers().lastHeader(ParsleyHeader.NULL_MESSAGE) == null) {
                            loopValues.add(record.value());
                        }
                    });
                    return loopValues.contains("s:hello");
                });
            }
            assertEquals(List.of("s:hello"), loopValues,
                    "exactly one business record must traverse the loop — the cycle must not "
                            + "re-derive or duplicate it");

            // Quiescence: the loop topic's end offset must hold still for a sustained window once
            // the marker exchange settles. A ping-ponging relay would grow it on every commit.
            await().atMost(Duration.ofSeconds(120)).until(() -> {
                long before = endOffset(bootstrap, LOOP);
                Thread.sleep(5_000);
                return endOffset(bootstrap, LOOP) == before;
            });

            assertTrue(streams.state() == KafkaStreams.State.RUNNING,
                    "the cyclic app must still be RUNNING after quiescing — settling must come "
                            + "from the I6 relay rule, never from a crash");
        }
    }

    /**
     * The cyclic delegate: a T1 record forwards {@code s:<value>} onto the loop sink; a loop
     * record — the app's own sink coming back around — forwards nothing (a non-emitting delivery,
     * so the processor advertises progress with a null message instead, exercising marker relay
     * around the cycle).
     */
    private static ProcessorSupplier<String, String, String, String> loopingProcessor() {
        return () -> new Processor<>() {
            private ProcessorContext<String, String> ctx;

            @Override
            public void init(ProcessorContext<String, String> context) {
                this.ctx = context;
            }

            @Override
            public void process(Record<String, String> record) {
                if (T1.equals(ctx.recordMetadata().orElseThrow().topic())) {
                    ctx.forward(record.withValue("s:" + record.value()));
                }
            }
        };
    }

    private static long endOffset(String bootstrap, String topic) throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap))) {
            TopicPartition tp = new TopicPartition(topic, 0);
            return admin.listOffsets(Map.of(tp, OffsetSpec.latest()))
                    .partitionResult(tp).get().offset();
        }
    }

    private static Properties streamsConfig(String bootstrap) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "cyclic-quiesce-" + UUID.randomUUID());
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
                ConsumerConfig.GROUP_ID_CONFIG, "loop-observer-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    }

    private static void createTopics(String bootstrap, String... topics) throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap))) {
            List<NewTopic> newTopics = new ArrayList<>();
            for (String topic : topics) {
                newTopics.add(new NewTopic(topic, 1, (short) 1));
            }
            admin.createTopics(Set.copyOf(newTopics)).all().get();
        }
    }
}
