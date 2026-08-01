package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Composes {@link Stage}s into a runnable application.
 *
 * <p>Run broker-less against {@code TopologyTestDriver} with {@link #testTopology()}, or on a
 * cluster with {@link #streams(Properties)}.
 *
 * <p>Stages connect through ordinary topics. One stage's sink is another's source, and the
 * connecting topic is a causal channel like any other, stamped on write and gated on read. It
 * must exist before the application starts.
 *
 * <p>A topic may be a sink of several stages, but a source of only one.
 */
public final class Parsley {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(Parsley.class);

    private final String applicationId;
    private final Map<String, Stage> stages;

    private Parsley(String applicationId, Map<String, Stage> stages) {
        this.applicationId = applicationId;
        this.stages = stages;
    }

    /**
     * Returns the application id, which qualifies every topic and store name it owns.
     *
     * @return the application id
     */
    public String applicationId() {
        return applicationId;
    }

    /**
     * Returns the composed stages, in declaration order.
     *
     * @return the stages, in the order they were given to {@link #named}
     */
    java.util.Collection<Stage> stageSet() {
        return stages.values();
    }

    /**
     * Composes stages into an application under the given id.
     *
     * <p>Keep the id stable across deployments. It names the state stores, their changelog
     * topics and the tick topics, and {@link #streams(Properties)} pins
     * {@code application.id} to it.
     *
     * @param applicationId the application id, matching {@code [a-zA-Z0-9._-]+}
     * @param stages the stages, in declaration order
     * @return the composed application
     * @throws IllegalArgumentException if the application id is null or does not match, if no
     *         stages are given, if two stages share a name, or if a topic is a source of more
     *         than one stage
     */
    public static Parsley named(String applicationId, Stage... stages) {
        if (applicationId == null || !applicationId.matches("[a-zA-Z0-9._-]+")) {
            throw new IllegalArgumentException("application id must be [a-zA-Z0-9._-]+: "
                    + applicationId + " (it prefixes the names of the topics Parsley owns)");
        }
        if (stages.length == 0) throw new IllegalArgumentException("no stages");
        Map<String, Stage> byName = new LinkedHashMap<>();
        Set<String> sourceTopics = new HashSet<>();
        for (Stage stage : stages) {
            if (byName.put(stage.name(), stage) != null) {
                throw new IllegalArgumentException("duplicate stage name: " + stage.name()
                        + " (name each stage with " + Stage.class.getSimpleName() + ".named)");
            }
            for (String topic : stage.sourceTopics(applicationId)) {
                if (!sourceTopics.add(topic)) {
                    throw new IllegalArgumentException("topic " + topic + " is a source of two"
                            + " stages; Kafka Streams allows one source node per topic");
                }
            }
        }
        return new Parsley(applicationId, byName);
    }

    /**
     * Builds a broker-less topology for {@code TopologyTestDriver}.
     *
     * <p>Topic identity is synthesized, each topic has one partition, and there is nothing to
     * seed against or truncate. Every call assembles a new {@link Topology}, because Kafka
     * Streams consumes one on construction.
     *
     * <p>Use it with the test driver only. Against a real cluster it captures no consumer
     * positions and resolves no real topic identity, and fails closed at the first delivered
     * record.
     *
     * @return a fresh topology
     */
    public Topology testTopology() {
        Topology t = new Topology();
        for (Stage stage : stages.values()) {
            stage.addToForTest(t, applicationId);
        }
        return t;
    }

    /**
     * Assembles the cluster runtime, not yet started.
     *
     * <p>Forces {@code processing.guarantee=exactly_once_v2} and
     * {@code isolation.level=read_committed}, and sets {@code application.id} to the
     * composition's. Resolves topic identity through an {@link Admin} client built from the
     * same properties, which the returned handle closes with itself.
     *
     * <p>Set listeners on the returned handle, then {@link CausalStreams#start()} it.
     *
     * @param props the Kafka Streams configuration
     * @return the runtime handle, not yet started
     * @throws IllegalArgumentException if the properties name a {@code processing.guarantee}
     *         other than {@code exactly_once_v2}, or an {@code application.id} other than the
     *         composition's
     */
    public CausalStreams streams(Properties props) {
        Properties p = new Properties();
        p.putAll(props);
        Object guarantee = p.get(StreamsConfig.PROCESSING_GUARANTEE_CONFIG);
        if (guarantee != null && !StreamsConfig.EXACTLY_ONCE_V2.equals(guarantee)) {
            throw new IllegalArgumentException("processing.guarantee must be exactly_once_v2");
        }
        p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        p.put(StreamsConfig.consumerPrefix(ConsumerConfig.ISOLATION_LEVEL_CONFIG), "read_committed");
        // The composition already named every topic and store from the application id; running
        // under a different one would consume tick topics nobody writes to.
        Object declared = p.get(StreamsConfig.APPLICATION_ID_CONFIG);
        if (declared != null && !applicationId.equals(declared)) {
            throw new IllegalArgumentException("application.id is " + declared
                    + " but the composition is named " + applicationId
                    + "; the composition's id names its topics and stores");
        }
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);

        Admin admin = Admin.create(Map.of("bootstrap.servers",
                p.getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG)));
        try {
            TopicIds topicIds = TopicIds.fromAdmin(admin);
            Topology t = new Topology();
            for (Stage stage : stages.values()) {
                stage.addTo(t, applicationId, topicIds,
                        (ids, sinkTopics) -> new AdminBrokerOffsets(ids, admin, sinkTopics), false);
            }
            KafkaStreams streams = new KafkaStreams(t, p, Positions.capturingClientSupplier());
            LOG.info("assembled application {} with stages {}", applicationId, stages.keySet());
            return new CausalStreams(streams, admin, applicationId);
        } catch (RuntimeException e) {
            admin.close();
            throw e;
        }
    }
}
