package io.parsley;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Measures the cost of restoring Parsley's internal state after a restart or task reassignment.
 *
 * <p>Two operations are benchmarked independently:
 * <ul>
 *   <li>{@link #frontierRestore} — O(1) single-key RocksDB read and deserialisation; no parameter
 *       dimension (expected to be effectively constant); run with all {@code bufferSize} values but
 *       only one curve will appear.
 *   <li>{@link #bufferRestore} — O(n) scans to rebuild {@link RocksBufferStore}'s sequence counter
 *       and {@link ParsleyEngine}'s position-index from existing RocksDB data; varies buffer size n.
 * </ul>
 *
 * <p>State is pre-populated once at {@code Level.Trial} and held read-only by the benchmark
 * methods, so no per-invocation reset is needed.
 *
 * <p>This benchmark corresponds to the <em>recovery latency</em> latency category in the Parsley
 * documentation.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@Fork(3)
@State(Scope.Benchmark)
public class StateRestorationBenchmark {

    /** Buffer size at the time of a simulated crash. */
    @Param({"1", "8", "32", "128", "512", "1024"})
    public int bufferSize;

    private TopologyTestDriver driver;
    private KeyValueStore<String, byte[]> frontierKV;
    private KeyValueStore<Long, byte[]> bufferKV;
    private KeyValueStore<byte[], byte[]> waitKV;
    private ParsleySerializer<String, String> serializer;
    private CausalBufferLimit limit;

    @Setup(Level.Trial)
    public void setUp(BenchmarkInfra infra) throws IOException {
        ProcessorSupplier<String, String, String, String> noOp = () -> new Processor<>() {
            @Override public void init(ProcessorContext<String, String> ctx) {}
            @Override public void process(Record<String, String> record) {}
        };
        limit = CausalBufferLimit.ofSize(Integer.MAX_VALUE / 2);

        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("bench-in", Consumed.with(Serdes.String(), Serdes.String()))
               .process(CausalProcessors.builder(noOp, limit)
                       .serdes(Serdes.String(), Serdes.String())
                       .addCausalTopic(new CausalTopic("bench-in", CausalPosition.deriveUuid("bench-in")))
                       .build())
               .to("bench-out", Produced.with(Serdes.String(), Serdes.String()));

        Path trialDir = Files.createTempDirectory(infra.stateDir(), "bench-restore-");
        Properties config = infra.streamsConfig("bench-restore");
        config.put(org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG,
                   trialDir.toAbsolutePath().toString());
        driver = new TopologyTestDriver(builder.build(), config);

        frontierKV = driver.getKeyValueStore("parsley-frontier");
        bufferKV   = driver.getKeyValueStore("parsley-buffer");
        waitKV     = driver.getKeyValueStore("parsley-position-index");
        serializer = new ParsleySerializer<>(
                new ParsleyResolver<>(t -> Serdes.String(), t -> Serdes.String()));

        // Pre-populate the frontier store with 4 positions (realistic width).
        CausalFrontier frontier = CausalFrontier.empty();
        for (int i = 0; i < 4; i++) {
            frontier = frontier.observe(
                    new CausalPosition(CausalPosition.deriveUuid("bench-" + i), 0, (long) i));
        }
        frontierKV.put(ParsleyAttributes.FRONTIER_KEY, frontier.toBytes());

        // Pre-populate the buffer store with bufferSize records, each with an unsatisfied
        // dependency, as if a crash occurred with that many held records.
        ParsleyEngine<String, String> engine = new ParsleyEngine<>(
                limit, CausalFrontier.empty(), f -> {},
                new RocksBufferStore<>(bufferKV, serializer), new RocksPositionIndex(waitKV),
                ParsleyMetrics.NOOP);
        for (int i = 0; i < bufferSize; i++) {
            CausalDependencies deps = CausalDependencies.builder()
                    .require(new CausalPosition(CausalPosition.deriveUuid("never-" + i), 0, 0L)).build();
            engine.onRecord(record("bench-" + i, 0, (long) i, deps));
        }
        // Discard the engine; benchmark methods will reconstruct it from RocksDB.
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        driver.close();
    }

    /**
     * Simulates frontier restoration on startup: a single RocksDB read of key {@code "f"} followed
     * by {@link CausalFrontier#fromBytes}. Expected to be effectively constant regardless of
     * buffer size.
     */
    @Benchmark
    public CausalFrontier frontierRestore() {
        return CausalFrontier.fromBytes(frontierKV.get(ParsleyAttributes.FRONTIER_KEY));
    }

    /**
     * Simulates buffer restoration on startup: the O(n) scan in {@link RocksBufferStore}'s
     * constructor (to seed nextSequence and size) plus the O(n) position-index rebuild in
     * {@link ParsleyEngine}'s constructor (one {@link RocksPositionIndex#index} write per buffered
     * record). Together these reproduce the full cost of {@link ParsleyProcessor#init} when
     * non-empty state is found in RocksDB.
     */
    @Benchmark
    public ParsleyEngine<String, String> bufferRestore() {
        RocksBufferStore<String, String> buf = new RocksBufferStore<>(bufferKV, serializer);
        RocksPositionIndex idx = new RocksPositionIndex(waitKV);
        return new ParsleyEngine<>(limit, CausalFrontier.empty(), f -> {},
                buf, idx, ParsleyMetrics.NOOP);
    }

    private static ParsleyRecord<String, String> record(String srcTopic, int partition, long offset,
                                                         CausalDependencies deps) {
        List<ParsleyHeader> headers = new ArrayList<>();
        headers.add(new ParsleyHeader(ParsleyAttributes.CAUSAL_DEPENDENCIES, deps.toBytes()));
        headers.add(new ParsleyHeader(ParsleyAttributes.SRC_TOPIC, srcTopic.getBytes(UTF_8)));
        headers.add(new ParsleyHeader(ParsleyAttributes.SRC_PARTITION, ParsleyRecord.intToBytes(partition)));
        headers.add(new ParsleyHeader(ParsleyAttributes.SRC_OFFSET, ParsleyRecord.longToBytes(offset)));
        return new ParsleyRecord<>("k", "v", 0L, headers);
    }
}
