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
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * End-to-end proof that dependencies stamped by the decorator at {@code forward} ride the record
 * headers through a Streams sink ({@code .to(topic)}) out to the output topic — so nothing extra is
 * needed on egress. A raw {@link KafkaConsumer} reads the output topic and checks the header.
 */
@Testcontainers(disabledWithoutDocker = true)
class CausalProcessorsSinkPropagationIT {

    @Container
    private final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"));

    private static final String IN = "decorator-in";
    private static final String OUT = "decorator-out";

    /**
     * A record forwarded by the causal decorator carries Parsley's stamped causal-dependencies header
     * through a Kafka Streams sink ({@code .to(topic)}) onto the output topic, so a downstream raw
     * consumer can read the header without anything extra on the egress path.
     *
     * <p>The input record carries empty dependencies so it is admitted immediately and the frontier
     * advances. The output is read by a raw {@link org.apache.kafka.clients.consumer.KafkaConsumer}
     * that only understands byte headers.
     *
     * Asserts that the value is the delegate's transform, and that the causal-dependencies header
     * on the output record decodes to a position on {@code decorator-in} at offset 0.
     */
    @Test
    void stampedClockSurvivesTheSinkToTheOutputTopic() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        Map<String, Uuid> topicIds = createTopics(bootstrap, IN, OUT);
        CausalTopics topics = CausalTopics.of(topicIds);

        ProcessorSupplier<String, String, String, String> user = () -> new Processor<>() {
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

        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(IN, Consumed.with(Serdes.String(), Serdes.String()))
                .process(CausalProcessors.builder(user).addBufferStore("parsley")
                        .addBuffer(CausalBuffer.of(IN, Serdes.String(), Serdes.String()))
                        .build())
                .to(OUT, Produced.with(Serdes.String(), Serdes.String()));

        try (KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig(bootstrap))) {
            streams.start();

            // Produce one record carrying empty dependencies so it is admitted and the frontier advances.
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(Map.of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()))) {
                ProducerRecord<String, String> record = new ProducerRecord<>(IN, "k", "hello");
                record.headers().add("parsley-causal-dependencies", CausalDependencies.empty().toBytes());
                producer.send(record).get();
            }

            try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(Map.of(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                    ConsumerConfig.GROUP_ID_CONFIG, "out-" + UUID.randomUUID(),
                    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName()))) {
                consumer.subscribe(List.of(OUT));

                ConsumerRecord<String, byte[]> out = poll(consumer);
                assertEquals("HELLO", new String(out.value()), "the delegate's transform reached the sink");

                Optional<CausalDependencies> stamped = CausalDependencies.fromHeaders(out.headers());
                assertEquals(Optional.of(CausalDependencies.builder(topics).require(IN, 0, 0).build()), stamped,
                        "the dependencies stamped at forward must survive the sink to the output topic");
            }
        }
    }

    private static ConsumerRecord<String, byte[]> poll(KafkaConsumer<String, byte[]> consumer) {
        @SuppressWarnings("unchecked")
        ConsumerRecord<String, byte[]>[] captured = new ConsumerRecord[1];
        await().atMost(Duration.ofSeconds(60)).until(() -> {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(500));
            if (records.isEmpty()) {
                return false;
            }
            captured[0] = records.iterator().next();
            return true;
        });
        return captured[0];
    }

    private static Properties streamsConfig(String bootstrap) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "decorator-it-" + UUID.randomUUID());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 200);
        return props;
    }

    private static Map<String, Uuid> createTopics(String bootstrap, String... topics) throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap))) {
            Set<NewTopic> newTopics = new java.util.HashSet<>();
            for (String topic : topics) {
                newTopics.add(new NewTopic(topic, 1, (short) 1));
            }
            CreateTopicsResult result = admin.createTopics(newTopics);
            result.all().get();
            Map<String, Uuid> ids = new java.util.HashMap<>();
            for (String topic : topics) {
                ids.put(topic, result.topicId(topic).get());
            }
            return ids;
        }
    }
}
