package io.parsley;

import org.apache.kafka.streams.StreamsConfig;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/**
 * Shared JMH state that manages external infrastructure for all benchmark classes: a Kafka broker
 * (via TestContainers or an externally provided address) and a state-store directory (temp dir or
 * an externally provided path).
 *
 * <p>Override the defaults with system properties:
 * <ul>
 *   <li>{@code parsley.bench.bootstrap.servers} — broker address; if absent, TestContainers starts a broker
 *   <li>{@code parsley.bench.state.store.dir} — RocksDB root; if absent, a temp dir is created
 *   <li>{@code parsley.bench.replication.factor} — default {@code 1}
 *   <li>{@code parsley.bench.partition.count} — default {@code 1}
 * </ul>
 */
@State(Scope.Benchmark)
public class BenchmarkInfra {

    private static final String BOOTSTRAP_PROP = "parsley.bench.bootstrap.servers";
    private static final String STATE_DIR_PROP  = "parsley.bench.state.store.dir";
    private static final String REPLICATION_PROP = "parsley.bench.replication.factor";
    private static final String PARTITION_PROP   = "parsley.bench.partition.count";

    private KafkaContainer kafka;
    private String bootstrapServers;
    private Path stateDir;
    private String replicationFactor;
    private String partitionCount;

    @Setup(Level.Trial)
    public void setUp() throws IOException {
        replicationFactor = System.getProperty(REPLICATION_PROP, "1");
        partitionCount    = System.getProperty(PARTITION_PROP,   "1");

        String bootstrapProp = System.getProperty(BOOTSTRAP_PROP);
        if (bootstrapProp != null && !bootstrapProp.isBlank()) {
            bootstrapServers = bootstrapProp;
        } else {
            kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"));
            kafka.start();
            bootstrapServers = kafka.getBootstrapServers();
        }

        String stateDirProp = System.getProperty(STATE_DIR_PROP);
        if (stateDirProp != null && !stateDirProp.isBlank()) {
            stateDir = Path.of(stateDirProp);
            Files.createDirectories(stateDir);
        } else {
            stateDir = Files.createTempDirectory("parsley-bench");
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        if (kafka != null) {
            kafka.stop();
        }
    }

    /** Returns the Kafka bootstrap servers string. */
    public String bootstrapServers() {
        return bootstrapServers;
    }

    /** Returns the root directory under which benchmark state stores are created. */
    public Path stateDir() {
        return stateDir;
    }

    public String replicationFactor() {
        return replicationFactor;
    }

    public String partitionCount() {
        return partitionCount;
    }

    /**
     * Returns a Kafka Streams {@link Properties} block for the given application id. Uses
     * {@code "dummy:1234"} as bootstrap servers so the config is valid for
     * {@link org.apache.kafka.streams.TopologyTestDriver} (which does not need a real broker).
     */
    public Properties streamsConfig(String applicationId) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toAbsolutePath().toString());
        return props;
    }
}
