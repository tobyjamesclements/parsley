package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
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
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

/**
 * Measures the per-record constant overhead of Parsley's causal metadata handling.
 *
 * <p>Three operations are benchmarked independently across a single {@code clockWidth} parameter,
 * producing three O(w) curves on a shared x-axis:
 * <ul>
 *   <li>{@link #deserialize} — parse the causal clock from incoming record headers
 *   <li>{@link #dominanceCheck} — compare the parsed clock against the local frontier
 *   <li>{@link #serialize} — write the updated clock to outgoing record headers
 * </ul>
 *
 * <p>This benchmark corresponds to the <em>constant per-record overhead</em> latency category in
 * the Parsley documentation.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@Fork(3)
@State(Scope.Benchmark)
public class HeaderEvaluationBenchmark {

    /** Number of (topicId, partition) entries in the clock vector. */
    @Param({"1", "2", "4", "8", "16", "32", "64"})
    public int clockWidth;

    private ParsleyVectorClock frontier;
    private ParsleyVectorClock dependencies;
    private byte[] headerBytes;

    @Setup(Level.Trial)
    public void setUp() {
        // Build a frontier and matching dependencies with exactly clockWidth entries.
        // All entries have offset=1 so dominates() always traverses every entry (worst-case path).
        ParsleyVectorClock f = ParsleyVectorClock.empty();
        ParsleyVectorClock deps = ParsleyVectorClock.empty();
        for (int i = 0; i < clockWidth; i++) {
            Uuid topicId = Uuid.randomUuid();
            f = f.observe(topicId, 0, 1L);
            deps = deps.observe(topicId, 0, 1L);
        }
        frontier     = f;
        dependencies = deps;
        headerBytes  = dependencies.toBytes();
    }

    /**
     * Reads and parses the causal clock from the serialised header bytes, as happens when a record
     * arrives at a Parsley consumer.
     */
    @Benchmark
    public ParsleyVectorClock deserialize() {
        return ParsleyVectorClock.fromBytes(headerBytes);
    }

    /**
     * Compares the parsed dependencies against the local frontier to determine admissibility, as
     * happens in {@link ParsleyEngine#receive}.
     */
    @Benchmark
    public boolean dominanceCheck() {
        return frontier.dominates(dependencies);
    }

    /**
     * Serialises the current frontier to the outgoing header bytes, as happens when
     * {@link ParsleyProcessorContext} stamps a forwarded record.
     */
    @Benchmark
    public byte[] serialize() {
        return frontier.toBytes();
    }
}
