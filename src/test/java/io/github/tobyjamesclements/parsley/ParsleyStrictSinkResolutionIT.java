package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
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
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Proves the strict sink-resolution rule end to end against a real broker: a causal sink must
 * exist before the stage starts. A declared sink that cannot be resolved to a UUID at init fails
 * the member loudly — never a warn-and-continue that would silently disable own-output stamping
 * (an I2 under-claim downstream) for the task's whole lifetime — and once the operator creates
 * the topic, the same application starts cleanly and processes normally.
 */
@Testcontainers(disabledWithoutDocker = true)
class ParsleyStrictSinkResolutionIT {

    @Container
    private final KafkaContainer kafka =
            new KafkaContainer(ParsleyBrokerImage.get());

    private static final String C1 = "c1";
    private static final String C2 = "c2";

    /**
     * Startup against a not-yet-created sink topic fails the member fast with the strict
     * sink-resolution failure naming the sink and the sinks-must-exist rule; after the operator
     * creates the sink topic, a fresh start of the same application comes up cleanly and a
     * produced record flows through to the sink.
     */
    @Test
    void aMissingSinkTopicFailsStartupAndCreatingItHealsTheNextStart() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        createTopics(bootstrap, C1);
        String applicationId = "strict-sink-it-" + UUID.randomUUID();

        AtomicReference<Throwable> uncaught = new AtomicReference<>();
        try (CausalStreams streams = causalApp(bootstrap, applicationId)) {
            streams.setUncaughtExceptionHandler(throwable -> {
                uncaught.compareAndSet(null, throwable);
                return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            });
            streams.start();

            await().atMost(Duration.ofSeconds(60)).until(() -> streams.state() == KafkaStreams.State.ERROR);
            assertSame(KafkaStreams.State.ERROR, streams.state(),
                    "a declared sink that does not exist must fail startup loudly, never start "
                            + "with own-output stamping silently disabled");
            Throwable failure = uncaught.get();
            assertNotNull(failure, "the member must die with an uncaught exception, not stall silently");
            assertTrue(chainMessageContains(failure, "declared sink topic '" + C2 + "'"),
                    "the failure must name the unresolvable sink: " + failure);
            assertTrue(chainMessageContains(failure, "must exist before the stage starts"),
                    "the failure must state the sinks-must-exist rule: " + failure);
        }

        createTopics(bootstrap, C2);
        try (CausalStreams streams = causalApp(bootstrap, applicationId)) {
            streams.start();
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig(bootstrap))) {
                producer.send(new ProducerRecord<>(C1, "k", "healed")).get();
            }
            List<String> outputs = awaitBusinessOutputs(bootstrap, 1);
            assertTrue(outputs.contains("healed"),
                    "after the sink is created, the same application must start cleanly and "
                            + "process through to the sink: " + outputs);
        }
    }

    private CausalStreams causalApp(String bootstrap, String applicationId) {
        CausalTopology topology = new CausalStreamsBuilder()
                .stream(C1, Serdes.String(), Serdes.String())
                .process(passthrough())
                .to(C2, Serdes.String(), Serdes.String())
                .build();
        return new CausalStreams(topology, streamsConfig(bootstrap, applicationId));
    }

    /** Waits until {@code count} business records (non-null-message) reach C2; returns their values. */
    private static List<String> awaitBusinessOutputs(String bootstrap, int count) {
        List<String> outputs = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig(bootstrap))) {
            consumer.subscribe(List.of(C2));
            await().atMost(Duration.ofSeconds(90)).until(() -> {
                consumer.poll(Duration.ofMillis(500)).forEach(record -> {
                    if (record.value() != null
                            && record.headers().lastHeader(ParsleyHeader.NULL_MESSAGE) == null) {
                        outputs.add(record.value());
                    }
                });
                return outputs.size() >= count;
            });
        }
        return outputs;
    }

    private static boolean chainMessageContains(Throwable failure, String needle) {
        for (Throwable t = failure; t != null; t = t.getCause()) {
            if (t.getMessage() != null && t.getMessage().contains(needle)) {
                return true;
            }
        }
        return false;
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
                ctx.forward(record);
            }
        };
    }

    private static Properties streamsConfig(String bootstrap, String applicationId) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
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
