package io.parsley;

import org.apache.kafka.streams.StreamsConfig;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/**
 * Shared JMH state for all benchmark classes. The benchmarks run entirely in-process on
 * {@link org.apache.kafka.streams.TopologyTestDriver} and local RocksDB — there is no Kafka broker
 * in the measurement path — so the only infrastructure this manages is the state-store directory.
 *
 * <p>Override the default (a fresh temp dir) with a system property:
 * <ul>
 *   <li>{@code parsley.bench.state.store.dir} — RocksDB root; point this at the target machine's real
 *       storage class (e.g. NVMe) for representative scan/restore figures.
 * </ul>
 */
@State(Scope.Benchmark)
public class BenchmarkInfra {

    private static final String STATE_DIR_PROP = "parsley.bench.state.store.dir";

    private Path stateDir;

    @Setup(Level.Trial)
    public void setUp() throws IOException {
        String stateDirProp = System.getProperty(STATE_DIR_PROP);
        if (stateDirProp != null && !stateDirProp.isBlank()) {
            stateDir = Path.of(stateDirProp);
            Files.createDirectories(stateDir);
        } else {
            stateDir = Files.createTempDirectory("parsley-bench");
        }
    }

    /** Returns the root directory under which benchmark state stores are created. */
    public Path stateDir() {
        return stateDir;
    }

    /**
     * Returns a Kafka Streams {@link Properties} block for the given application id. Uses
     * {@code "dummy:1234"} as bootstrap servers because {@link org.apache.kafka.streams.TopologyTestDriver}
     * never connects to a broker; the value only needs to be a syntactically valid address.
     */
    public Properties streamsConfig(String applicationId) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toAbsolutePath().toString());
        return props;
    }
}
