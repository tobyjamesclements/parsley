package io.parsley;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ParsleyEngineTest {

    private static final TopicPartition PRICES = new TopicPartition("prices", 0);
    private static final TopicPartition ORDERS = new TopicPartition("orders", 0);

    private final List<ParsleyRecord<String, String>> forwarded = new ArrayList<>();
    private final List<CausalViolation> violations = new ArrayList<>();
    private final List<CausalDependencies> frontiers = new ArrayList<>();
    // The engine's buffer; held directly so tests can inspect what is currently buffered.
    private final InMemoryBufferStore<String, String> buffer = new InMemoryBufferStore<>();

    private ParsleyEngine<String, String> engine(CausalBufferPolicy policy) {
        return engine(policy, null);
    }

    private ParsleyEngine<String, String> engine(CausalBufferPolicy policy,
                                                java.util.function.Consumer<ParsleyRecord<String, String>> sink) {
        return new ParsleyEngine<>(policy, violations::add, CausalDependencies.empty(), sink, frontiers::add,
                buffer);
    }

    private List<CausalViolationReason> reasons() {
        return violations.stream().map(CausalViolation::reason).toList();
    }

    private static ParsleyRecord<String, String> rec(TopicPartition tp, long offset, CausalDependencies deps) {
        return new ParsleyRecord<>("k", "v", 0L, List.of(),
                deps == null ? null : deps.toBytes(), tp, offset);
    }

    private void onRecord(ParsleyEngine<String, String> engine, ParsleyRecord<String, String> record) {
        forwarded.addAll(engine.onRecord(record));
    }

    @Test
    void satisfiedRecordForwardsImmediatelyAndAdvancesFrontier() {
        ParsleyEngine<String, String> engine = engine(CausalBufferPolicy.forwardUnsafe(CausalBufferLimit.ofSize(100)));

        onRecord(engine, rec(PRICES, 3, CausalDependencies.empty()));

        assertEquals(1, forwarded.size());
        assertEquals(CausalDependencies.empty().advance(PRICES, 3), engine.frontier());
    }

    @Test
    void unsatisfiedRecordIsBufferedUntilFrontierCatchesUp() {
        ParsleyEngine<String, String> engine = engine(CausalBufferPolicy.forwardUnsafe(CausalBufferLimit.ofSize(100)));

        // An order causally depending on prices-0 having reached offset 3, arriving first.
        CausalDependencies orderDeps = CausalDependencies.empty().advance(PRICES, 3);
        onRecord(engine, rec(ORDERS, 0, orderDeps));
        assertTrue(forwarded.isEmpty(), "order must be buffered until its price premise is observed");

        // The price it depends on arrives, releasing the buffered order in causal order.
        onRecord(engine, rec(PRICES, 3, CausalDependencies.empty()));

        assertEquals(2, forwarded.size());
        assertEquals(PRICES, forwarded.get(0).sourcePartition());
        assertEquals(ORDERS, forwarded.get(1).sourcePartition());
        assertEquals(CausalDependencies.empty().advance(PRICES, 3).advance(ORDERS, 0), engine.frontier(),
                "draining a buffered record must advance the frontier through it");
    }

    @Test
    void bufferHoldsAnUnsatisfiedRecordAndReleasesItOnDrain() {
        ParsleyEngine<String, String> engine = engine(CausalBufferPolicy.forwardUnsafe(CausalBufferLimit.ofSize(100)));

        ParsleyRecord<String, String> order = rec(ORDERS, 0, CausalDependencies.empty().advance(PRICES, 3));
        engine.onRecord(order);
        assertEquals(1, buffer.size(), "an unsatisfied record must be held in the buffer");

        // The premise arrives: the order drains and leaves the buffer.
        engine.onRecord(rec(PRICES, 3, CausalDependencies.empty()));
        assertEquals(0, buffer.size(), "draining a record must remove it from the buffer");
    }

    @Test
    void strictEvictionRemovesTheRecordFromTheBuffer() {
        ParsleyEngine<String, String> engine = engine(CausalBufferPolicy.drop(CausalBufferLimit.ofSize(1)));

        // Buffered then immediately evicted (dropped) by the size-1 limit.
        engine.onRecord(rec(ORDERS, 0, CausalDependencies.empty().advance(PRICES, 99)));

        assertEquals(0, buffer.size(), "a dropped record must be removed from the buffer");
    }

    @Test
    void recordsAlreadyInTheBufferDrainWhenTheFrontierCatchesUp() {
        // Records that survived a restart are simply already in the buffer store — there is no
        // separate restore step. They must not advance the frontier until their premise is observed.
        buffer.add(rec(ORDERS, 0, CausalDependencies.empty().advance(PRICES, 3)));
        ParsleyEngine<String, String> engine = engine(CausalBufferPolicy.forwardUnsafe(CausalBufferLimit.ofSize(100)));
        assertEquals(CausalDependencies.empty(), engine.frontier(), "a pre-buffered record must not advance the frontier");
        assertTrue(frontiers.isEmpty(), "a pre-buffered record must not fire the frontier listener");

        // Its premise arrives; the buffered record drains in causal order behind it.
        onRecord(engine, rec(PRICES, 3, CausalDependencies.empty()));

        assertEquals(2, forwarded.size());
        assertEquals(PRICES, forwarded.get(0).sourcePartition());
        assertEquals(ORDERS, forwarded.get(1).sourcePartition());
    }

    @Test
    void missingHeaderForwardsWithViolation() {
        ParsleyEngine<String, String> engine = engine(CausalBufferPolicy.forwardUnsafe(CausalBufferLimit.ofSize(100)));

        onRecord(engine, rec(PRICES, 0, null));

        assertEquals(1, forwarded.size());
        assertEquals(List.of(CausalViolationReason.MISSING_HEADER), reasons());
        assertEquals(CausalDependencies.empty().advance(PRICES, 0), engine.frontier(),
                "a forwarded missing-header record still advances the frontier");
    }

    @Test
    void unresolvableClockForwardsWithViolation() {
        ParsleyEngine<String, String> engine = engine(CausalBufferPolicy.forwardUnsafe(CausalBufferLimit.ofSize(100)));

        ParsleyRecord<String, String> garbage =
                new ParsleyRecord<>("k", "v", 0L, List.of(), new byte[]{9, 9, 9}, PRICES, 0);
        onRecord(engine, garbage);

        assertEquals(1, forwarded.size());
        assertEquals(List.of(CausalViolationReason.UNRESOLVABLE_CLOCK), reasons());
    }

    @Test
    void sizeLimitEvictsAndForwardsUnderForwardUnsafePolicy() {
        ParsleyEngine<String, String> engine = engine(CausalBufferPolicy.forwardUnsafe(CausalBufferLimit.ofSize(2)));
        CausalDependencies unmet = CausalDependencies.empty().advance(PRICES, 99);

        onRecord(engine, rec(ORDERS, 0, unmet));
        assertTrue(forwarded.isEmpty());
        onRecord(engine, rec(ORDERS, 1, unmet)); // hits size 2 → evict both

        assertEquals(2, forwarded.size());
        assertEquals(List.of(CausalViolationReason.LIMIT_REACHED, CausalViolationReason.LIMIT_REACHED), reasons());
        assertEquals(CausalDependencies.empty().advance(ORDERS, 1), engine.frontier(),
                "forwardUnsafe eviction advances the frontier through each evicted record");
    }

    @Test
    void dropPolicyDiscardsOnEvictionAndReportsTheGap() {
        ParsleyEngine<String, String> engine = engine(CausalBufferPolicy.drop(CausalBufferLimit.ofSize(1)));

        onRecord(engine, rec(ORDERS, 0, CausalDependencies.empty().advance(PRICES, 99)));

        assertTrue(forwarded.isEmpty(), "dropped records are never forwarded");
        assertEquals(1, violations.size());
        CausalViolation violation = violations.get(0);
        assertEquals(CausalViolationReason.LIMIT_REACHED, violation.reason());
        assertEquals(CausalDependencies.empty(), violation.frontier());
        assertEquals(CausalDependencies.empty().advance(PRICES, 99), violation.required());
        assertEquals(Map.of(PRICES, 100L), violation.gap(), "required 99 vs observed -1 → gap 100");
    }

    @Test
    void deadLetterPolicyRoutesEvictedRecordsToSink() {
        List<ParsleyRecord<String, String>> deadLettered = new ArrayList<>();
        ParsleyEngine<String, String> engine = engine(
                CausalBufferPolicy.deadLetter(CausalBufferLimit.ofSize(1), "dlq"), deadLettered::add);

        onRecord(engine, rec(ORDERS, 0, CausalDependencies.empty().advance(PRICES, 99)));

        assertTrue(forwarded.isEmpty());
        assertEquals(1, deadLettered.size());
        assertEquals(List.of(CausalViolationReason.LIMIT_REACHED), reasons());
    }

    @Test
    void durationPolicyExposesEvictionInterval() {
        ParsleyEngine<String, String> engine =
                engine(CausalBufferPolicy.forwardUnsafe(CausalBufferLimit.ofDuration(Duration.ofSeconds(5))));
        assertEquals(Duration.ofSeconds(5), engine.evictionInterval().orElseThrow());
    }

    @Test
    void frontierListenerFiresBeforeEachForward() {
        ParsleyEngine<String, String> engine = engine(CausalBufferPolicy.forwardUnsafe(CausalBufferLimit.ofSize(100)));

        onRecord(engine, rec(PRICES, 3, CausalDependencies.empty()));

        // Listener observed the advanced frontier; persistence happens before the record is returned.
        assertEquals(engine.frontier(), frontiers.get(frontiers.size() - 1));
    }
}
