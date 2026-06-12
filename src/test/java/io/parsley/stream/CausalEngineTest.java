package io.parsley.stream;

import io.parsley.BufferLimit;
import io.parsley.BufferingPolicy;
import io.parsley.CausalViolationHandler;
import io.parsley.CausalViolationReason;
import io.parsley.VectorClock;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CausalEngineTest {

    private static final TopicPartition PRICES = new TopicPartition("prices", 0);
    private static final TopicPartition ORDERS = new TopicPartition("orders", 0);

    private final List<CausalRecord<String, String>> forwarded = new ArrayList<>();
    private final List<CausalViolationReason> violations = new ArrayList<>();
    private final List<VectorClock> frontiers = new ArrayList<>();

    private CausalEngine<String, String> engine(BufferingPolicy policy) {
        return engine(policy, null);
    }

    private CausalEngine<String, String> engine(BufferingPolicy policy,
                                                java.util.function.Consumer<CausalRecord<String, String>> sink) {
        CausalViolationHandler handler = (record, reason) -> violations.add(reason);
        return new CausalEngine<>(policy, handler, VectorClock.empty(), sink, frontiers::add);
    }

    private static CausalRecord<String, String> rec(TopicPartition tp, long offset, VectorClock deps) {
        return new CausalRecord<>("k", "v", 0L, List.of(),
                deps == null ? null : deps.toBytes(), tp, offset);
    }

    private void onRecord(CausalEngine<String, String> engine, CausalRecord<String, String> record) {
        forwarded.addAll(engine.onRecord(record));
    }

    @Test
    void satisfiedRecordForwardsImmediatelyAndAdvancesFrontier() {
        CausalEngine<String, String> engine = engine(BufferingPolicy.ignore(BufferLimit.ofSize(100)));

        onRecord(engine, rec(PRICES, 3, VectorClock.empty()));

        assertEquals(1, forwarded.size());
        assertEquals(VectorClock.empty().advance(PRICES, 3), engine.frontier());
    }

    @Test
    void unsatisfiedRecordIsBufferedUntilFrontierCatchesUp() {
        CausalEngine<String, String> engine = engine(BufferingPolicy.ignore(BufferLimit.ofSize(100)));

        // An order causally depending on prices-0 having reached offset 3, arriving first.
        VectorClock orderDeps = VectorClock.empty().advance(PRICES, 3);
        onRecord(engine, rec(ORDERS, 0, orderDeps));
        assertTrue(forwarded.isEmpty(), "order must be buffered until its price premise is observed");

        // The price it depends on arrives, releasing the buffered order in causal order.
        onRecord(engine, rec(PRICES, 3, VectorClock.empty()));

        assertEquals(2, forwarded.size());
        assertEquals(PRICES, forwarded.get(0).sourcePartition());
        assertEquals(ORDERS, forwarded.get(1).sourcePartition());
    }

    @Test
    void missingHeaderForwardsWithViolation() {
        CausalEngine<String, String> engine = engine(BufferingPolicy.ignore(BufferLimit.ofSize(100)));

        onRecord(engine, rec(PRICES, 0, null));

        assertEquals(1, forwarded.size());
        assertEquals(List.of(CausalViolationReason.MISSING_HEADER), violations);
    }

    @Test
    void unresolvableClockForwardsWithViolation() {
        CausalEngine<String, String> engine = engine(BufferingPolicy.ignore(BufferLimit.ofSize(100)));

        CausalRecord<String, String> garbage =
                new CausalRecord<>("k", "v", 0L, List.of(), new byte[]{9, 9, 9}, PRICES, 0);
        onRecord(engine, garbage);

        assertEquals(1, forwarded.size());
        assertEquals(List.of(CausalViolationReason.UNRESOLVABLE_CLOCK), violations);
    }

    @Test
    void sizeLimitEvictsAndForwardsUnderIgnorePolicy() {
        CausalEngine<String, String> engine = engine(BufferingPolicy.ignore(BufferLimit.ofSize(2)));
        VectorClock unmet = VectorClock.empty().advance(PRICES, 99);

        onRecord(engine, rec(ORDERS, 0, unmet));
        assertTrue(forwarded.isEmpty());
        onRecord(engine, rec(ORDERS, 1, unmet)); // hits size 2 → evict both

        assertEquals(2, forwarded.size());
        assertEquals(List.of(CausalViolationReason.LIMIT_REACHED, CausalViolationReason.LIMIT_REACHED), violations);
    }

    @Test
    void dropPolicyDiscardsOnEviction() {
        CausalEngine<String, String> engine = engine(BufferingPolicy.drop(BufferLimit.ofSize(1)));

        onRecord(engine, rec(ORDERS, 0, VectorClock.empty().advance(PRICES, 99)));

        assertTrue(forwarded.isEmpty(), "dropped records are never forwarded");
        assertEquals(List.of(CausalViolationReason.LIMIT_REACHED), violations);
    }

    @Test
    void deadLetterPolicyRoutesEvictedRecordsToSink() {
        List<CausalRecord<String, String>> deadLettered = new ArrayList<>();
        CausalEngine<String, String> engine = engine(
                BufferingPolicy.deadLetter(BufferLimit.ofSize(1), "dlq"), deadLettered::add);

        onRecord(engine, rec(ORDERS, 0, VectorClock.empty().advance(PRICES, 99)));

        assertTrue(forwarded.isEmpty());
        assertEquals(1, deadLettered.size());
        assertEquals(List.of(CausalViolationReason.LIMIT_REACHED), violations);
    }

    @Test
    void durationPolicyExposesEvictionInterval() {
        CausalEngine<String, String> engine =
                engine(BufferingPolicy.ignore(BufferLimit.ofDuration(Duration.ofSeconds(5))));
        assertEquals(Duration.ofSeconds(5), engine.evictionInterval().orElseThrow());
    }

    @Test
    void frontierListenerFiresBeforeEachForward() {
        CausalEngine<String, String> engine = engine(BufferingPolicy.ignore(BufferLimit.ofSize(100)));

        onRecord(engine, rec(PRICES, 3, VectorClock.empty()));

        // Listener observed the advanced frontier; persistence happens before the record is returned.
        assertEquals(engine.frontier(), frontiers.get(frontiers.size() - 1));
    }
}
