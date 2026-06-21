package io.parsley;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
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
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Measures the cost of scanning and releasing buffered records when the causal frontier advances.
 *
 * <p>Three operations are benchmarked independently, each with its own @Param dimension and its
 * own pre-populated {@link RocksBufferStore} / {@link RocksPositionIndex} backed by a
 * {@link TopologyTestDriver}-managed RocksDB instance:
 * <ul>
 *   <li>{@link #bufferSize} — O(log n) position-index lookup; varies buffer size {@code n}, fixes k=1, r=1
 *   <li>{@link #positionalOccupancy} — O(k) positional scan; varies occupancy {@code k}, fixes n=128, r=1
 *   <li>{@link #cascadeDepth} — O(r) cascade propagation; varies depth {@code r}, fixes n=128, k=1
 * </ul>
 *
 * <p>JMH runs all combinations of the three @Param dimensions. To isolate a single curve, restrict
 * parameters at run time, e.g. {@code -p n=1,8,32,128,512,1024 -p k=1 -p r=1} for the n-curve.
 *
 * <p>This benchmark corresponds to the <em>causal buffering latency</em> latency category in the
 * Parsley documentation.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@Fork(3)
@State(Scope.Benchmark)
public class BufferReleaseBenchmark {

    /** Buffer size (n); isolates O(log n) position-index lookup when k=1 and r=1. */
    @Param({"1", "8", "32", "128", "512", "1024"})
    public int n;

    /** Positional occupancy (k); isolates O(k) positional scan when n=128 and r=1. */
    @Param({"1", "2", "4", "8", "16", "32"})
    public int k;

    /** Cascade depth (r); isolates O(r) cascade propagation when n=128 and k=1. */
    @Param({"1", "2", "4", "8", "16", "32"})
    public int r;

    // -------------------------------------------------------------------------
    // Shared helpers — static so nested @State classes can call them
    // -------------------------------------------------------------------------

    static final int FIXED_N = 128; // n held constant in the k-curve and r-curve benchmarks
    static final CausalBufferLimit BENCH_LIMIT = CausalBufferLimit.ofSize(Integer.MAX_VALUE / 2);

    // A stable Uuid per synthetic topic name, so a record's own SRC_TOPIC_ID header matches the
    // CausalPosition other records declare as a dependency on that same name (e.g. "trigger").
    private static final Map<String, Uuid> TOPIC_IDS = new ConcurrentHashMap<>();

    static Uuid topicId(String topic) {
        return TOPIC_IDS.computeIfAbsent(topic, t -> Uuid.randomUuid());
    }

    static ParsleyRecord<String, String> syntheticRecord(String srcTopic, int partition, long offset,
                                                          ParsleyClock deps) {
        List<ParsleyHeader> headers = new ArrayList<>();
        headers.add(new ParsleyHeader(ParsleyAttributes.CAUSAL_DEPENDENCIES, deps.toBytes()));
        headers.add(new ParsleyHeader(ParsleyAttributes.SRC_TOPIC, srcTopic.getBytes(UTF_8)));
        headers.add(new ParsleyHeader(ParsleyAttributes.SRC_TOPIC_ID, ParsleyRecord.uuidToBytes(topicId(srcTopic))));
        headers.add(new ParsleyHeader(ParsleyAttributes.SRC_PARTITION, ParsleyRecord.intToBytes(partition)));
        headers.add(new ParsleyHeader(ParsleyAttributes.SRC_OFFSET, ParsleyRecord.longToBytes(offset)));
        return new ParsleyRecord<>("k", "v", 0L, headers);
    }

    static ParsleyEngine<String, String> freshEngine(KeyValueStore<Long, byte[]> bufferKV,
                                                       KeyValueStore<byte[], byte[]> waitKV,
                                                       ParsleySerializer<String, String> serializer) {
        return new ParsleyEngine<>(
                BENCH_LIMIT,
                ParsleyClock.empty(),
                f -> {},
                new RocksBufferStore<>(bufferKV, serializer),
                new RocksPositionIndex(waitKV),
                ParsleyMetrics.NOOP);
    }

    static void clearBufferStore(KeyValueStore<Long, byte[]> store) {
        List<Long> keys = new ArrayList<>();
        try (KeyValueIterator<Long, byte[]> it = store.all()) {
            while (it.hasNext()) keys.add(it.next().key);
        }
        keys.forEach(store::delete);
    }

    static void clearWaitStore(KeyValueStore<byte[], byte[]> store) {
        List<byte[]> keys = new ArrayList<>();
        try (KeyValueIterator<byte[], byte[]> it = store.all()) {
            while (it.hasNext()) keys.add(it.next().key);
        }
        keys.forEach(store::delete);
    }

    static TopologyTestDriver buildDriver(BenchmarkInfra infra, String label) throws IOException {
        ProcessorSupplier<String, String, String, String> noOp = () -> new Processor<>() {
            @Override public void init(ProcessorContext<String, String> ctx) {}
            @Override public void process(Record<String, String> record) {}
        };
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("bench-in", Consumed.with(Serdes.String(), Serdes.String()))
               .process(CausalProcessors.builder(noOp, BENCH_LIMIT)
                       .serdes(Serdes.String(), Serdes.String())
                       .addCausalTopic(new CausalTopic("bench-in", topicId("bench-in")))
                       .build())
               .to("bench-out", Produced.with(Serdes.String(), Serdes.String()));
        // Unique state dir per trial so each trial starts with an empty RocksDB
        Path trialDir = Files.createTempDirectory(infra.stateDir(), label + "-");
        Properties config = infra.streamsConfig(label);
        config.put(org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG,
                   trialDir.toAbsolutePath().toString());
        return new TopologyTestDriver(builder.build(), config);
    }

    static ParsleySerializer<String, String> buildSerializer() {
        return new ParsleySerializer<>(new ParsleyResolver<>(t -> Serdes.String(), t -> Serdes.String()));
    }

    // -------------------------------------------------------------------------
    // bufferSize benchmark: varies n, k=1, r=1
    // O(log n) — one record at the trigger coordinate among n total buffer entries
    // -------------------------------------------------------------------------

    @State(Scope.Thread)
    public static class SizeSetup {
        TopologyTestDriver driver;
        KeyValueStore<Long, byte[]> bufferKV;
        KeyValueStore<byte[], byte[]> waitKV;
        ParsleySerializer<String, String> serializer;
        ParsleyEngine<String, String> engine;
        ParsleyRecord<String, String> trigger;

        @Setup(Level.Trial)
        public void setUpTrial(BenchmarkInfra infra) throws IOException {
            driver     = buildDriver(infra, "bench-size");
            bufferKV   = driver.getKeyValueStore("parsley-buffer");
            waitKV     = driver.getKeyValueStore("parsley-position-index");
            serializer = buildSerializer();
        }

        @TearDown(Level.Trial)
        public void tearDownTrial() { driver.close(); }

        @Setup(Level.Invocation)
        public void setUpInvocation(BufferReleaseBenchmark bench) {
            clearBufferStore(bufferKV);
            clearWaitStore(waitKV);
            engine = freshEngine(bufferKV, waitKV, serializer);

            // Record 0 waits on the trigger coordinate; only it is released when the trigger fires.
            ParsleyClock triggerDeps = ParsleyClock.empty().observe(topicId("trigger"), 0, 0L);
            engine.onRecord(syntheticRecord("bench-0", 0, 0L, triggerDeps));

            // Records 1..n-1 each wait on a unique, never-satisfied coordinate.
            for (int i = 1; i < bench.n; i++) {
                ParsleyClock deps = ParsleyClock.empty().observe(topicId("unique-" + i), 0, 0L);
                engine.onRecord(syntheticRecord("bench-" + i, 0, (long) i, deps));
            }

            // Trigger: source=(trigger-topic, 0, 0), empty deps — forwarded immediately, advances
            // frontier on the trigger coordinate, causing record 0 to drain.
            trigger = syntheticRecord("trigger", 0, 0L, ParsleyClock.empty());
        }
    }

    // -------------------------------------------------------------------------
    // positionalOccupancy benchmark: varies k, n=128, r=1
    // O(k) — k records all waiting on the same trigger coordinate
    // -------------------------------------------------------------------------

    @State(Scope.Thread)
    public static class OccupancySetup {
        TopologyTestDriver driver;
        KeyValueStore<Long, byte[]> bufferKV;
        KeyValueStore<byte[], byte[]> waitKV;
        ParsleySerializer<String, String> serializer;
        ParsleyEngine<String, String> engine;
        ParsleyRecord<String, String> trigger;

        @Setup(Level.Trial)
        public void setUpTrial(BenchmarkInfra infra) throws IOException {
            driver     = buildDriver(infra, "bench-occupancy");
            bufferKV   = driver.getKeyValueStore("parsley-buffer");
            waitKV     = driver.getKeyValueStore("parsley-position-index");
            serializer = buildSerializer();
        }

        @TearDown(Level.Trial)
        public void tearDownTrial() { driver.close(); }

        @Setup(Level.Invocation)
        public void setUpInvocation(BufferReleaseBenchmark bench) {
            clearBufferStore(bufferKV);
            clearWaitStore(waitKV);
            engine = freshEngine(bufferKV, waitKV, serializer);

            ParsleyClock triggerDeps = ParsleyClock.empty().observe(topicId("trigger"), 0, 0L);

            // k records all waiting on the same trigger coordinate.
            for (int i = 0; i < bench.k; i++) {
                engine.onRecord(syntheticRecord("bench-" + i, 0, (long) i, triggerDeps));
            }

            // FIXED_N - k filler records each waiting on a unique, never-satisfied coordinate.
            for (int i = bench.k; i < FIXED_N; i++) {
                ParsleyClock deps = ParsleyClock.empty().observe(topicId("unique-" + i), 0, 0L);
                engine.onRecord(syntheticRecord("filler-" + i, 0, (long) i, deps));
            }

            trigger = syntheticRecord("trigger", 0, 0L, ParsleyClock.empty());
        }
    }

    // -------------------------------------------------------------------------
    // cascadeDepth benchmark: varies r, n=128, k=1
    // O(r) — each released record enables the next in a chain
    // -------------------------------------------------------------------------

    @State(Scope.Thread)
    public static class CascadeSetup {
        TopologyTestDriver driver;
        KeyValueStore<Long, byte[]> bufferKV;
        KeyValueStore<byte[], byte[]> waitKV;
        ParsleySerializer<String, String> serializer;
        ParsleyEngine<String, String> engine;
        ParsleyRecord<String, String> trigger;

        @Setup(Level.Trial)
        public void setUpTrial(BenchmarkInfra infra) throws IOException {
            driver     = buildDriver(infra, "bench-cascade");
            bufferKV   = driver.getKeyValueStore("parsley-buffer");
            waitKV     = driver.getKeyValueStore("parsley-position-index");
            serializer = buildSerializer();
        }

        @TearDown(Level.Trial)
        public void tearDownTrial() { driver.close(); }

        @Setup(Level.Invocation)
        public void setUpInvocation(BufferReleaseBenchmark bench) {
            clearBufferStore(bufferKV);
            clearWaitStore(waitKV);
            engine = freshEngine(bufferKV, waitKV, serializer);

            // Record 0 depends on the trigger; record i depends on record (i-1)'s source coordinate.
            // This forms an r-hop chain: advancing the trigger releases record 0, which in turn
            // releases record 1, ..., which releases record r-1.
            ParsleyClock dep0 = ParsleyClock.empty().observe(topicId("trigger"), 0, 0L);
            engine.onRecord(syntheticRecord("chain-0", 0, 0L, dep0));

            for (int i = 1; i < bench.r; i++) {
                ParsleyClock dep = ParsleyClock.empty().observe(topicId("chain-" + (i - 1)), 0, (long) (i - 1));
                engine.onRecord(syntheticRecord("chain-" + i, 0, (long) i, dep));
            }

            // Filler records: FIXED_N - r records each waiting on a unique, never-satisfied coordinate.
            for (int i = bench.r; i < FIXED_N; i++) {
                ParsleyClock deps = ParsleyClock.empty().observe(topicId("unique-" + i), 0, 0L);
                engine.onRecord(syntheticRecord("filler-" + i, 0, (long) i, deps));
            }

            trigger = syntheticRecord("trigger", 0, 0L, ParsleyClock.empty());
        }
    }

    // -------------------------------------------------------------------------
    // Benchmark methods
    // -------------------------------------------------------------------------

    /**
     * Isolates the O(log n) position-index lookup cost as buffer size n grows.
     * One record is released per invocation; records 1..n-1 remain buffered.
     */
    @Benchmark
    public List<ParsleyRecord<String, String>> bufferSize(SizeSetup s) {
        return s.engine.onRecord(s.trigger);
    }

    /**
     * Isolates the O(k) positional scan cost as per-coordinate occupancy k grows.
     * k records are released per invocation from a fixed-size (n=128) buffer.
     */
    @Benchmark
    public List<ParsleyRecord<String, String>> positionalOccupancy(OccupancySetup s) {
        return s.engine.onRecord(s.trigger);
    }

    /**
     * Isolates the O(r) cascade propagation cost as cascade depth r grows.
     * r records are released in a chain per invocation from a fixed-size (n=128) buffer.
     */
    @Benchmark
    public List<ParsleyRecord<String, String>> cascadeDepth(CascadeSetup s) {
        return s.engine.onRecord(s.trigger);
    }
}
