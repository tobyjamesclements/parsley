package io.parsley.core;

import io.parsley.BufferLimit;
import io.parsley.BufferingPolicy;
import io.parsley.CausalRecord;
import io.parsley.CausalViolationHandler;
import io.parsley.CausalViolationReason;
import io.parsley.ParsleyMetrics;
import io.parsley.VectorClock;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CausalEngineTest {

    private static final TestClockSerialiser SERIALISER = new TestClockSerialiser();
    private static final BufferingPolicy IGNORE_LARGE =
            BufferingPolicy.ignore(BufferLimit.ofSize(100));

    private final List<TestClock> persistedFrontiers = new ArrayList<>();
    private final List<CausalViolationReason> violations = new ArrayList<>();

    private CausalEngine<String, String, TestClock> engine(BufferingPolicy policy) {
        return new CausalEngine<>(policy,
                (record, reason) -> violations.add(reason),
                SERIALISER, TestClock.empty(), null, persistedFrontiers::add);
    }

    private static CausalRecord<String, String, TestClock> record(
            String key, TestClock position, TestClock dependencies) {
        byte[] encoded = dependencies == null ? null : SERIALISER.serialise(dependencies);
        return new CausalRecord<>(key, "v", 0L, List.of(), encoded, position, key);
    }

    @Test
    void satisfiedRecordIsForwardedAndAdvancesFrontier() {
        var engine = engine(IGNORE_LARGE);

        var out = engine.onRecord(record("r1", TestClock.at("A", 0), TestClock.empty()));

        assertEquals(List.of("r1"), out.stream().map(CausalRecord::key).toList());
        assertEquals(TestClock.at("A", 0), engine.frontier());
        assertTrue(violations.isEmpty());
    }

    @Test
    void missingDependenciesForwardsWithViolation() {
        var engine = engine(IGNORE_LARGE);

        var out = engine.onRecord(record("r1", TestClock.at("A", 0), null));

        assertEquals(1, out.size());
        assertEquals(List.of(CausalViolationReason.MISSING_HEADER), violations);
        assertEquals(TestClock.at("A", 0), engine.frontier());
    }

    @Test
    void unresolvableDependenciesForwardsWithViolation() {
        var engine = engine(IGNORE_LARGE);
        var garbage = new CausalRecord<String, String, TestClock>(
                "r1", "v", 0L, List.of(), "not-a-clock".getBytes(StandardCharsets.UTF_8),
                TestClock.at("A", 0), "r1");

        var out = engine.onRecord(garbage);

        assertEquals(1, out.size());
        assertEquals(List.of(CausalViolationReason.UNRESOLVABLE_CLOCK), violations);
        assertEquals(TestClock.at("A", 0), engine.frontier());
    }

    @Test
    void missingDependenciesRecordCascadesBufferedReleases() {
        var engine = engine(IGNORE_LARGE);
        engine.onRecord(record("dependent", TestClock.at("B", 0), TestClock.at("A", 0)));

        var out = engine.onRecord(record("orphan", TestClock.at("A", 0), null));

        assertEquals(List.of("orphan", "dependent"),
                out.stream().map(CausalRecord::key).toList());
    }

    @Test
    void unresolvableDependenciesRecordCascadesBufferedReleases() {
        var engine = engine(IGNORE_LARGE);
        engine.onRecord(record("dependent", TestClock.at("B", 0), TestClock.at("A", 0)));
        var garbage = new CausalRecord<String, String, TestClock>(
                "garbled", "v", 0L, List.of(), "not-a-clock".getBytes(StandardCharsets.UTF_8),
                TestClock.at("A", 0), "garbled");

        var out = engine.onRecord(garbage);

        assertEquals(List.of("garbled", "dependent"),
                out.stream().map(CausalRecord::key).toList());
    }

    @Test
    void unsatisfiedRecordIsBuffered() {
        var engine = engine(IGNORE_LARGE);

        var out = engine.onRecord(record("r1", TestClock.at("B", 0), TestClock.at("A", 0)));

        assertTrue(out.isEmpty());
        assertEquals(TestClock.empty(), engine.frontier());
        assertTrue(violations.isEmpty());
    }

    @Test
    void bufferedRecordIsReleasedWhenDependencyArrives() {
        var engine = engine(IGNORE_LARGE);
        engine.onRecord(record("dependent", TestClock.at("B", 0), TestClock.at("A", 0)));

        var out = engine.onRecord(record("dependency", TestClock.at("A", 0), TestClock.empty()));

        assertEquals(List.of("dependency", "dependent"),
                out.stream().map(CausalRecord::key).toList());
        assertEquals(TestClock.empty().advance("A", 0).advance("B", 0), engine.frontier());
    }

    @Test
    void cascadingReleasesPreserveCausalOrder() {
        var engine = engine(IGNORE_LARGE);
        engine.onRecord(record("c", TestClock.at("C", 0), TestClock.at("B", 0)));
        engine.onRecord(record("b", TestClock.at("B", 0), TestClock.at("A", 0)));

        var out = engine.onRecord(record("a", TestClock.at("A", 0), TestClock.empty()));

        assertEquals(List.of("a", "b", "c"), out.stream().map(CausalRecord::key).toList());
    }

    @Test
    void frontierListenerFiresOncePerForwardedRecordBeforeReturn() {
        var engine = engine(IGNORE_LARGE);
        engine.onRecord(record("dependent", TestClock.at("B", 0), TestClock.at("A", 0)));
        persistedFrontiers.clear();

        engine.onRecord(record("dependency", TestClock.at("A", 0), TestClock.empty()));

        assertEquals(List.of(
                        TestClock.at("A", 0),
                        TestClock.empty().advance("A", 0).advance("B", 0)),
                persistedFrontiers);
    }

    @Test
    void sizeLimitEvictionWithIgnorePolicyForwardsOutOfOrder() {
        var engine = engine(BufferingPolicy.ignore(BufferLimit.ofSize(2)));
        engine.onRecord(record("r1", TestClock.at("B", 0), TestClock.at("A", 10)));

        var out = engine.onRecord(record("r2", TestClock.at("B", 1), TestClock.at("A", 11)));

        assertEquals(List.of("r1", "r2"), out.stream().map(CausalRecord::key).toList());
        assertEquals(List.of(CausalViolationReason.LIMIT_REACHED, CausalViolationReason.LIMIT_REACHED),
                violations);
        assertEquals(TestClock.empty().advance("B", 1), engine.frontier());
    }

    @Test
    void sizeLimitEvictionWithDropPolicyForwardsNothing() {
        var engine = engine(BufferingPolicy.drop(BufferLimit.ofSize(2)));
        engine.onRecord(record("r1", TestClock.at("B", 0), TestClock.at("A", 10)));

        var out = engine.onRecord(record("r2", TestClock.at("B", 1), TestClock.at("A", 11)));

        assertTrue(out.isEmpty());
        assertEquals(2, violations.size());
        assertEquals(TestClock.empty(), engine.frontier());
    }

    @Test
    void deadLetterEvictionRoutesToSink() {
        List<CausalRecord<String, String, TestClock>> sink = new ArrayList<>();
        var engine = new CausalEngine<String, String, TestClock>(
                BufferingPolicy.deadLetter(BufferLimit.ofSize(2), "dead"),
                (record, reason) -> violations.add(reason),
                SERIALISER, TestClock.empty(), sink::add, persistedFrontiers::add);
        engine.onRecord(record("r1", TestClock.at("B", 0), TestClock.at("A", 10)));

        var out = engine.onRecord(record("r2", TestClock.at("B", 1), TestClock.at("A", 11)));

        assertTrue(out.isEmpty());
        assertEquals(List.of("r1", "r2"), sink.stream().map(CausalRecord::key).toList());
        assertEquals(2, violations.size());
    }

    @Test
    void deadLetterPolicyWithoutSinkIsRejected() {
        assertThrows(IllegalArgumentException.class, () ->
                engine(BufferingPolicy.deadLetter(BufferLimit.ofSize(2), "dead")));
    }

    @Test
    void unsupportedLimitsAreRejectedAtConstruction() {
        assertThrows(UnsupportedOperationException.class, () ->
                engine(BufferingPolicy.ignore(BufferLimit.ofBytes(1024))));
        assertThrows(UnsupportedOperationException.class, () ->
                engine(BufferingPolicy.ignore(BufferLimit.ofFrontierAdvancement(10))));
    }

    @Test
    void evictionIntervalReflectsDurationLimit() {
        assertEquals(Duration.ofSeconds(30),
                engine(BufferingPolicy.ignore(BufferLimit.ofDuration(Duration.ofSeconds(30))))
                        .evictionInterval().orElseThrow());
        assertEquals(Duration.ofSeconds(5),
                engine(BufferingPolicy.ignore(BufferLimit.first(
                        BufferLimit.ofDuration(Duration.ofSeconds(5)), BufferLimit.ofSize(10))))
                        .evictionInterval().orElseThrow());
        assertTrue(engine(IGNORE_LARGE).evictionInterval().isEmpty());
    }

    @Test
    void evictNowOnEmptyBufferReturnsNothing() {
        assertTrue(engine(IGNORE_LARGE).evictNow().isEmpty());
        assertTrue(violations.isEmpty());
    }

    @Test
    void directEvictNowForwardsIgnoreEvictionsAndAdvancesFrontier() {
        var engine = engine(BufferingPolicy.ignore(BufferLimit.ofDuration(Duration.ofSeconds(30))));
        engine.onRecord(record("r1", TestClock.at("B", 0), TestClock.at("A", 10)));

        var out = engine.evictNow();

        assertEquals(List.of("r1"), out.stream().map(CausalRecord::key).toList());
        assertEquals(List.of(CausalViolationReason.LIMIT_REACHED), violations);
        assertEquals(TestClock.at("B", 0), engine.frontier());
    }

    @Test
    void evictionAdvancedFrontierSatisfiesSubsequentRecords() {
        var engine = engine(BufferingPolicy.ignore(BufferLimit.ofSize(1)));
        engine.onRecord(record("evicted", TestClock.at("B", 0), TestClock.at("A", 10)));
        violations.clear();

        var out = engine.onRecord(record("dependent", TestClock.at("B", 1), TestClock.at("B", 0)));

        assertEquals(List.of("dependent"), out.stream().map(CausalRecord::key).toList());
        assertTrue(violations.isEmpty());
    }

    @Test
    void sizeEvictionFiresUnderFirstLimit() {
        var engine = engine(BufferingPolicy.ignore(BufferLimit.first(
                BufferLimit.ofDuration(Duration.ofSeconds(30)), BufferLimit.ofSize(2))));
        assertEquals(Duration.ofSeconds(30), engine.evictionInterval().orElseThrow());
        engine.onRecord(record("r1", TestClock.at("B", 0), TestClock.at("A", 10)));

        var out = engine.onRecord(record("r2", TestClock.at("B", 1), TestClock.at("A", 11)));

        assertEquals(List.of("r1", "r2"), out.stream().map(CausalRecord::key).toList());
        assertEquals(2, violations.size());
    }

    @Test
    void equalButDistinctRecordsAreTrackedIndependently() {
        var released = new ArrayList<Duration>();
        ParsleyMetrics metrics = new ParsleyMetrics() {
            @Override public void onMessageBuffered() {}
            @Override public void onMessageReleased(Duration bufferDuration) { released.add(bufferDuration); }
            @Override public void onViolation(CausalViolationReason reason) {}
            @Override public void onFrontierAdvanced(VectorClock<?> frontier) {}
        };
        var engine = new CausalEngine<String, String, TestClock>(
                IGNORE_LARGE, CausalViolationHandler.noop(),
                SERIALISER, TestClock.empty(), null, persistedFrontiers::add, metrics);

        // Two value-equal records (sharing the same dependency array — CausalRecord's
        // byte[] component compares by reference): the engine's identity-based buffer
        // bookkeeping must track them separately.
        byte[] deps = SERIALISER.serialise(TestClock.at("A", 0));
        var first = new CausalRecord<String, String, TestClock>(
                "twin", "v", 0L, List.of(), deps, TestClock.at("B", 0), "twin");
        var second = new CausalRecord<String, String, TestClock>(
                "twin", "v", 0L, List.of(), deps, TestClock.at("B", 0), "twin");
        assertEquals(first, second);
        engine.onRecord(first);
        engine.onRecord(second);

        var out = engine.onRecord(record("dependency", TestClock.at("A", 0), TestClock.empty()));

        assertEquals(List.of("dependency", "twin", "twin"),
                out.stream().map(CausalRecord::key).toList());
        assertEquals(2, released.size());
        assertTrue(released.stream().noneMatch(Duration::isNegative));
    }

    @Test
    void partiallySatisfiedMultiPartitionDependenciesStayBuffered() {
        var engine = engine(IGNORE_LARGE);
        engine.onRecord(record("free", TestClock.at("A", 0), TestClock.empty()));
        violations.clear();

        var out = engine.onRecord(record("needsBoth",
                TestClock.at("C", 0),
                TestClock.empty().advance("A", 0).advance("B", 0)));

        assertTrue(out.isEmpty());
        assertEquals(TestClock.at("A", 0), engine.frontier());
    }

    @Test
    void metricsAreReported() {
        var buffered = new int[1];
        var released = new int[1];
        var advanced = new int[1];
        List<CausalViolationReason> metricViolations = new ArrayList<>();
        ParsleyMetrics metrics = new ParsleyMetrics() {
            @Override public void onMessageBuffered() { buffered[0]++; }
            @Override public void onMessageReleased(Duration bufferDuration) { released[0]++; }
            @Override public void onViolation(CausalViolationReason reason) { metricViolations.add(reason); }
            @Override public void onFrontierAdvanced(VectorClock<?> frontier) { advanced[0]++; }
        };
        var engine = new CausalEngine<String, String, TestClock>(
                IGNORE_LARGE, CausalViolationHandler.noop(),
                SERIALISER, TestClock.empty(), null, persistedFrontiers::add, metrics);

        engine.onRecord(record("dependent", TestClock.at("B", 0), TestClock.at("A", 0)));
        engine.onRecord(record("dependency", TestClock.at("A", 0), TestClock.empty()));
        engine.onRecord(record("orphan", TestClock.at("A", 1), null));

        assertEquals(1, buffered[0]);
        assertEquals(1, released[0]);
        assertEquals(List.of(CausalViolationReason.MISSING_HEADER), metricViolations);
        assertEquals(3, advanced[0]);
    }

    @Test
    void evictionReportsLimitReachedToMetrics() {
        List<CausalViolationReason> metricViolations = new ArrayList<>();
        ParsleyMetrics metrics = new ParsleyMetrics() {
            @Override public void onMessageBuffered() {}
            @Override public void onMessageReleased(Duration bufferDuration) {}
            @Override public void onViolation(CausalViolationReason reason) { metricViolations.add(reason); }
            @Override public void onFrontierAdvanced(VectorClock<?> frontier) {}
        };
        var engine = new CausalEngine<String, String, TestClock>(
                BufferingPolicy.drop(BufferLimit.ofSize(2)), CausalViolationHandler.noop(),
                SERIALISER, TestClock.empty(), null, persistedFrontiers::add, metrics);

        engine.onRecord(record("r1", TestClock.at("B", 0), TestClock.at("A", 10)));
        engine.onRecord(record("r2", TestClock.at("B", 1), TestClock.at("A", 11)));

        assertEquals(List.of(CausalViolationReason.LIMIT_REACHED, CausalViolationReason.LIMIT_REACHED),
                metricViolations);
    }
}
