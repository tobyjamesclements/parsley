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
 * The front door: a composition of {@link Stage}s, and the two ways to run it — a broker-less
 * topology for {@code TopologyTestDriver}, or a {@link CausalStreams} runtime against a real
 * cluster.
 *
 * <p>Stages connect through ordinary topics: one stage's sink is another's source, and the
 * connecting topic is a causal channel like any other — stamped on write, gated on read, and
 * required to exist before the application starts. Stamping is synchronous at every hop
 * (sequence claims), so a pipeline of stages costs only the broker round trip per hop.
 *
 * <p>Constraints checked at composition: stage names must be distinct (each names its state
 * stores), and source topics must be disjoint across stages — Kafka Streams allows a topic to
 * feed only one source node per topology. A topic may be a sink of several stages.
 */
public final class Parsley {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(Parsley.class);

    private final Map<String, Stage> stages;

    private Parsley(Map<String, Stage> stages) {
        this.stages = stages;
    }

    public static Parsley of(Stage... stages) {
        if (stages.length == 0) throw new IllegalArgumentException("no stages");
        Map<String, Stage> byName = new LinkedHashMap<>();
        Set<String> sourceTopics = new HashSet<>();
        for (Stage stage : stages) {
            if (byName.put(stage.name(), stage) != null) {
                throw new IllegalArgumentException("duplicate stage name: " + stage.name()
                        + " (name each stage with " + Stage.class.getSimpleName() + ".named)");
            }
            for (String topic : stage.sourceTopics()) {
                if (!sourceTopics.add(topic)) {
                    throw new IllegalArgumentException("topic " + topic + " is a source of two"
                            + " stages; Kafka Streams allows one source node per topic");
                }
            }
        }
        return new Parsley(byName);
    }

    /**
     * A fresh broker-less topology for {@code TopologyTestDriver}: topic identity synthesized,
     * one partition per topic, nothing to seed against or truncate. Every call assembles a new
     * {@code Topology}, because Kafka Streams consumes a topology on construction. The
     * returned topology is for the test driver only — under a real cluster it neither
     * captures consumer positions nor resolves real topic identity, and the adapter fails
     * closed at the first delivered record.
     */
    public Topology testTopology() {
        Topology t = new Topology();
        for (Stage stage : stages.values()) {
            stage.addToForTest(t);
        }
        return t;
    }

    /**
     * Assembles the production runtime, not yet started: forces
     * {@code processing.guarantee=exactly_once_v2} and {@code isolation.level=read_committed}
     * (the protocol's transactional state and liveness are defined against them), installs
     * the position-capturing client supplier — the consumer's position advance past
     * transaction markers is the protocol's liveness signal — and resolves topic identity
     * through an admin client built from the same properties. Set listeners on the returned
     * handle, then {@link CausalStreams#start()} it.
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

        Admin admin = Admin.create(Map.of("bootstrap.servers",
                p.getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG)));
        try {
            TopicIds topicIds = TopicIds.fromAdmin(admin);
            Topology t = new Topology();
            for (Stage stage : stages.values()) {
                stage.addTo(t, topicIds,
                        (ids, sinkTopics) -> new AdminBrokerOffsets(ids, admin, sinkTopics), false);
            }
            KafkaStreams streams = new KafkaStreams(t, p, Positions.capturingClientSupplier());
            LOG.info("assembled application {} with stages {}",
                    p.getProperty(StreamsConfig.APPLICATION_ID_CONFIG), stages.keySet());
            return new CausalStreams(streams, admin);
        } catch (RuntimeException e) {
            admin.close();
            throw e;
        }
    }
}
