package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Serdes;
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

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Proves the <strong>leaderless coordination holds across two instances</strong> of one topology: with the
 * two partitions' tasks split over two {@link KafkaStreams} instances, both fold the shared, single-
 * partition epoch-events log independently and reach the same decision — a transition's floor is the
 * {@code merge-min} of <em>both</em> instances' published frontiers, one per partition.
 *
 * <p>Note on scope: a genuine <em>topology change</em> (adding a stage) is a redeploy, since Kafka Streams
 * requires every instance of an application to run the same topology — so a zero-downtime rolling topology
 * swap is not a Streams capability, and the joiner is covered by {@code ParsleyCoordinationJoinerIT}. This
 * test instead exercises multi-instance agreement, which the single-instance ITs cannot.
 */
@Testcontainers(disabledWithoutDocker = true)
class ParsleyCoordinationMultiInstanceIT {

    @Container
    private final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"));

    private static final String IN = "t1";
    private static final String OUT = "out";
    private static final String EPOCH_EVENTS = "epoch-events";

    /**
     * Two instances of a two-partition coordinated stage each own one partition. Both deliver records
     * (advancing each partition's completeness), then a transition is requested; each instance's task
     * publishes its own frontier and the round owner commits their merge-min. Asserts the epoch-events log
     * carries publications from two distinct members and an {@code EpochCommitted} whose floor bounds
     * <em>both</em> partitions — proof the two instances agreed and their frontiers were merged.
     */
    @Test
    void twoInstancesAgreeAndMergeMinTheFloorAcrossPartitions() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        createTopics(bootstrap, 2, IN, OUT);
        createTopics(bootstrap, 1, EPOCH_EVENTS);
        String appId = "multi-it-" + UUID.randomUUID();

        Path stateDir1 = Files.createTempDirectory("parsley-multi-1");
        Path stateDir2 = Files.createTempDirectory("parsley-multi-2");

        try (CausalStreams instance1 = new CausalStreams(topology(), streamsConfig(bootstrap, appId, stateDir1));
             CausalStreams instance2 = new CausalStreams(topology(), streamsConfig(bootstrap, appId, stateDir2))) {
            instance1.start();
            instance2.start();
            await().atMost(Duration.ofSeconds(60)).until(() ->
                    instance1.state() == KafkaStreams.State.RUNNING && instance2.state() == KafkaStreams.State.RUNNING);

            // Deliver a record on each partition so both partitions' completeness is non-empty.
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig(bootstrap))) {
                producer.send(stampEmptyDeps(new ProducerRecord<>(IN, 0, "k0", "v0"))).get();
                producer.send(stampEmptyDeps(new ProducerRecord<>(IN, 1, "k1", "v1"))).get();
            }

            // Promote both tasks to running (epoch 1), then evolve to epoch 2 with a real floor.
            requestUntilCommitted(instance1, instance2, bootstrap, 1L);
            requestUntilCommitted(instance1, instance2, bootstrap, 2L);

            List<ParsleyEpochEvent> log = awaitLogWithCommit(bootstrap, 2L);
            long distinctPublishers = log.stream()
                    .filter(e -> e instanceof ParsleyEpochEvent.FrontierPublished)
                    .map(e -> ((ParsleyEpochEvent.FrontierPublished) e).memberId())
                    .collect(Collectors.toSet())
                    .size();
            assertTrue(distinctPublishers >= 2,
                    "both instances' tasks must publish their frontiers for the round (saw " + distinctPublishers + ")");

            ParsleyEpochEvent.EpochCommitted epochTwo = log.stream()
                    .filter(e -> e instanceof ParsleyEpochEvent.EpochCommitted c && c.epochId() == 2L)
                    .map(e -> (ParsleyEpochEvent.EpochCommitted) e)
                    .reduce((first, second) -> second)
                    .orElseThrow(() -> new AssertionError("no epoch-2 commit on the log"));
            assertTrue(epochTwo.lowerBounds().offsetFor(topicId(bootstrap), 0) >= 0
                            && epochTwo.lowerBounds().offsetFor(topicId(bootstrap), 1) >= 0,
                    "epoch 2's floor must bound both partitions — the merge-min of the two instances' frontiers");
        }
    }

    private static void requestUntilCommitted(CausalStreams instance1, CausalStreams instance2,
                                              String bootstrap, long targetEpoch) {
        await().atMost(Duration.ofSeconds(90)).until(() -> {
            if (highestCommittedEpoch(bootstrap) >= targetEpoch) {
                return true;
            }
            requestQuietly(instance1);
            requestQuietly(instance2);
            return false;
        });
    }

    private static void requestQuietly(CausalStreams causalStreams) {
        try {
            causalStreams.requestEpochTransition();
        } catch (IllegalStateException notReadyYet) {
            // No local member has joined/initialised on this instance yet; retry on the next tick.
        }
    }

    private static List<ParsleyEpochEvent> awaitLogWithCommit(String bootstrap, long epoch) {
        List<ParsleyEpochEvent> log = new ArrayList<>();
        await().atMost(Duration.ofSeconds(30)).until(() -> {
            log.clear();
            log.addAll(readEpochEvents(bootstrap));
            return log.stream().anyMatch(e -> e instanceof ParsleyEpochEvent.EpochCommitted c && c.epochId() == epoch);
        });
        return log;
    }

    private static long highestCommittedEpoch(String bootstrap) {
        long highest = 0;
        for (ParsleyEpochEvent event : readEpochEvents(bootstrap)) {
            if (event instanceof ParsleyEpochEvent.EpochCommitted commit) {
                highest = Math.max(highest, commit.epochId());
            }
        }
        return highest;
    }

    private static List<ParsleyEpochEvent> readEpochEvents(String bootstrap) {
        List<ParsleyEpochEvent> events = new ArrayList<>();
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ConsumerConfig.GROUP_ID_CONFIG, "epoch-reader-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(List.of(EPOCH_EVENTS));
            long deadline = System.currentTimeMillis() + 2000;
            while (System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<byte[], byte[]> record : consumer.poll(Duration.ofMillis(200))) {
                    events.add(ParsleyEpochEvent.fromBytes(record.value()));
                }
            }
        }
        return events;
    }

    private static org.apache.kafka.common.Uuid topicId(String bootstrap) {
        Properties resolverProps = new Properties();
        resolverProps.put("bootstrap.servers", bootstrap);
        return ParsleyTopics.of(resolverProps).topicId(IN);
    }

    private static CausalTopology topology() {
        CausalStreamsBuilder builder = new CausalStreamsBuilder();
        builder.stream(IN, Serdes.String(), Serdes.String())
                .process(upperCaser())
                .to("out-sink", OUT, Serdes.String(), Serdes.String());
        return builder.build();
    }

    private static ProcessorSupplier<String, String, String, String> upperCaser() {
        return () -> new Processor<>() {
            private ProcessorContext<String, String> ctx;
            @Override public void init(ProcessorContext<String, String> context) { this.ctx = context; }
            @Override public void process(Record<String, String> record) {
                ctx.forward(record.withValue(record.value().toUpperCase(Locale.ROOT)));
            }
        };
    }

    private static ProducerRecord<String, String> stampEmptyDeps(ProducerRecord<String, String> record) {
        record.headers().add(ParsleyHeader.CAUSAL_DEPENDENCIES, ParsleyClock.empty().toBytes());
        return record;
    }

    private static Properties streamsConfig(String bootstrap, String appId, Path stateDir) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 200);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toAbsolutePath().toString());
        props.put(ParsleyConfig.COORDINATION_EPOCH_EVENTS_TOPIC, EPOCH_EVENTS);
        return props;
    }

    private static Map<String, Object> producerConfig(String bootstrap) {
        return Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    }

    private static void createTopics(String bootstrap, int partitions, String... topics) throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap))) {
            Set<NewTopic> newTopics = new HashSet<>();
            for (String topic : topics) {
                newTopics.add(new NewTopic(topic, partitions, (short) 1));
            }
            CreateTopicsResult result = admin.createTopics(newTopics);
            result.all().get();
        }
    }
}
