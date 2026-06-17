package io.parsley;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.header.Headers;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ParsleyEngineTest {

    private static final TopicPartition PRICES = new TopicPartition("prices", 0);
    private static final TopicPartition ORDERS = new TopicPartition("orders", 0);
    private static final Uuid PRICES_ID = CausalPosition.deriveUuid(PRICES.topic());
    private static final Uuid ORDERS_ID = CausalPosition.deriveUuid(ORDERS.topic());

    private final List<ParsleyRecord<String, String>> forwarded = new ArrayList<>();
    private final List<CausalViolation> violations = new ArrayList<>();
    private final List<CausalFrontier> frontiers = new ArrayList<>();
    private final MockBufferStore<String, String> buffer = new MockBufferStore<>();

    private ParsleyEngine<String, String> engine(CausalBufferPolicy policy) {
        return engine(policy, null);
    }

    private ParsleyEngine<String, String> engine(CausalBufferPolicy policy,
                                                java.util.function.Consumer<ParsleyRecord<String, String>> sink) {
        return new ParsleyEngine<>(policy, violations::add, CausalFrontier.empty(), sink, frontiers::add,
                buffer, new MockWaitIndex(), ParsleyMetrics.NOOP);
    }

    private List<CausalViolationReason> reasons() {
        return violations.stream().map(CausalViolation::reason).toList();
    }

    private static ParsleyRecord<String, String> rec(TopicPartition tp, long offset, CausalDependencies deps) {
        List<ParsleyHeader> headers = new ArrayList<>();
        if (deps != null) {
            headers.add(new ParsleyHeader(ParsleyAttributes.VECTOR_CLOCK, deps.toBytes()));
        }
        headers.add(new ParsleyHeader(ParsleyAttributes.SRC_TOPIC, tp.topic().getBytes(UTF_8)));
        headers.add(new ParsleyHeader(ParsleyAttributes.SRC_PARTITION, ParsleyRecord.intToBytes(tp.partition())));
        headers.add(new ParsleyHeader(ParsleyAttributes.SRC_OFFSET, ParsleyRecord.longToBytes(offset)));
        return new ParsleyRecord<>("k", "v", 0L, headers);
    }

    private void onRecord(ParsleyEngine<String, String> engine, ParsleyRecord<String, String> record) {
        forwarded.addAll(engine.onRecord(record));
    }

    @Test
    void satisfiedRecordForwardsImmediatelyAndAdvancesFrontier() {
        ParsleyEngine<String, String> engine = engine(CausalBufferPolicy.drop(CausalBufferLimit.ofSize(100)));

        onRecord(engine, rec(PRICES, 3, CausalDependencies.empty()));

        assertEquals(1, forwarded.size());
        assertEquals(CausalFrontier.empty().advance(PRICES_ID, 0, 3), engine.frontier());
    }

    @Test
    void unsatisfiedRecordIsBufferedUntilFrontierCatchesUp() {
        ParsleyEngine<String, String> engine = engine(CausalBufferPolicy.drop(CausalBufferLimit.ofSize(100)));

        CausalDependencies orderDeps = CausalDependencies.empty().advance(PRICES_ID, 0, 3);
        onRecord(engine, rec(ORDERS, 0, orderDeps));
        assertTrue(forwarded.isEmpty(), "order must be buffered until its price premise is offsetFor");

        onRecord(engine, rec(PRICES, 3, CausalDependencies.empty()));

        assertEquals(2, forwarded.size());
        assertEquals(PRICES, forwarded.get(0).sourcePartition());
        assertEquals(ORDERS, forwarded.get(1).sourcePartition());
        assertEquals(CausalFrontier.empty().advance(PRICES_ID, 0, 3).advance(ORDERS_ID, 0, 0), engine.frontier(),
                "draining a buffered record must advance the frontier through it");
    }

    @Test
    void bufferHoldsAnUnsatisfiedRecordAndReleasesItOnDrain() {
        ParsleyEngine<String, String> engine = engine(CausalBufferPolicy.drop(CausalBufferLimit.ofSize(100)));

        ParsleyRecord<String, String> order = rec(ORDERS, 0, CausalDependencies.empty().advance(PRICES_ID, 0, 3));
        engine.onRecord(order);
        assertEquals(1, buffer.size(), "an unsatisfied record must be held in the buffer");

        engine.onRecord(rec(PRICES, 3, CausalDependencies.empty()));
        assertEquals(0, buffer.size(), "draining a record must remove it from the buffer");
    }

    @Test
    void strictEvictionRemovesTheRecordFromTheBuffer() {
        ParsleyEngine<String, String> engine = engine(CausalBufferPolicy.drop(CausalBufferLimit.ofSize(1)));

        engine.onRecord(rec(ORDERS, 0, CausalDependencies.empty().advance(PRICES_ID, 0, 99)));

        assertEquals(0, buffer.size(), "a dropped record must be removed from the buffer");
    }

    @Test
    void recordsAlreadyInTheBufferDrainWhenTheFrontierCatchesUp() {
        buffer.add(rec(ORDERS, 0, CausalDependencies.empty().advance(PRICES_ID, 0, 3)));
        ParsleyEngine<String, String> engine = engine(CausalBufferPolicy.drop(CausalBufferLimit.ofSize(100)));
        assertEquals(CausalFrontier.empty(), engine.frontier(), "a pre-buffered record must not advance the frontier");
        assertTrue(frontiers.isEmpty(), "a pre-buffered record must not fire the frontier listener");

        onRecord(engine, rec(PRICES, 3, CausalDependencies.empty()));

        assertEquals(2, forwarded.size());
        assertEquals(PRICES, forwarded.get(0).sourcePartition());
        assertEquals(ORDERS, forwarded.get(1).sourcePartition());
    }

    @Test
    void inboundClockIsNeverFoldedIntoTheFrontier() {
        ParsleyEngine<String, String> engine = engine(CausalBufferPolicy.forwardUnsafe(CausalBufferLimit.ofSize(1)));

        CausalDependencies big = CausalDependencies.empty();
        for (int p = 0; p < 200; p++) {
            big = big.advance(CausalPosition.deriveUuid("ghost"), p, 1_000 + p);
        }
        onRecord(engine, rec(ORDERS, 0, big));

        assertEquals(CausalFrontier.empty().advance(ORDERS_ID, 0, 0), engine.frontier(),
                "the inbound dependency clock must never be merged into the frontier");
        assertEquals(1, engine.frontier().positions().size());
    }

    @Test
    void missingHeaderForwardsWithViolation() {
        ParsleyEngine<String, String> engine = engine(CausalBufferPolicy.drop(CausalBufferLimit.ofSize(100)));

        onRecord(engine, rec(PRICES, 0, null));

        assertEquals(1, forwarded.size());
        assertEquals(List.of(CausalViolationReason.MISSING_HEADER), reasons());
        assertEquals(CausalFrontier.empty().advance(PRICES_ID, 0, 0), engine.frontier(),
                "a forwarded missing-header record still advances the frontier");
    }

    @Test
    void unresolvableClockForwardsWithViolation() {
        ParsleyEngine<String, String> engine = engine(CausalBufferPolicy.drop(CausalBufferLimit.ofSize(100)));

        ParsleyRecord<String, String> garbage = new ParsleyRecord<>("k", "v", 0L, List.of(
                new ParsleyHeader(ParsleyAttributes.VECTOR_CLOCK, new byte[]{9, 9, 9}),
                new ParsleyHeader(ParsleyAttributes.SRC_TOPIC, PRICES.topic().getBytes(UTF_8)),
                new ParsleyHeader(ParsleyAttributes.SRC_PARTITION, ParsleyRecord.intToBytes(PRICES.partition())),
                new ParsleyHeader(ParsleyAttributes.SRC_OFFSET, ParsleyRecord.longToBytes(0L))));
        onRecord(engine, garbage);

        assertEquals(1, forwarded.size());
        assertEquals(List.of(CausalViolationReason.UNRESOLVABLE_CLOCK), reasons());
    }

    @Test
    void sizeLimitEvictsAndForwardsUnderForwardUnsafePolicy() {
        ParsleyEngine<String, String> engine = engine(CausalBufferPolicy.forwardUnsafe(CausalBufferLimit.ofSize(2)));
        CausalDependencies unmet = CausalDependencies.empty().advance(PRICES_ID, 0, 99);

        onRecord(engine, rec(ORDERS, 0, unmet));
        assertTrue(forwarded.isEmpty());
        onRecord(engine, rec(ORDERS, 1, unmet)); // hits size 2 → evict both

        assertEquals(2, forwarded.size());
        assertEquals(List.of(CausalViolationReason.LIMIT_REACHED, CausalViolationReason.LIMIT_REACHED), reasons());
        assertEquals(CausalFrontier.empty().advance(ORDERS_ID, 0, 1), engine.frontier(),
                "forwardUnsafe eviction advances the frontier through each evicted record");
    }

    @Test
    void dropPolicyDiscardsOnEvictionAndReportsTheGap() {
        ParsleyEngine<String, String> engine = engine(CausalBufferPolicy.drop(CausalBufferLimit.ofSize(1)));

        onRecord(engine, rec(ORDERS, 0, CausalDependencies.empty().advance(PRICES_ID, 0, 99)));

        assertTrue(forwarded.isEmpty(), "dropped records are never forwarded");
        assertEquals(1, violations.size());
        CausalViolation violation = violations.get(0);
        assertEquals(CausalViolationReason.LIMIT_REACHED, violation.reason());
        assertEquals(CausalFrontier.empty(), violation.frontier());
        assertEquals(CausalDependencies.empty().advance(PRICES_ID, 0, 99), violation.required());
        assertEquals(List.of(new CausalPosition(CausalPosition.deriveUuid("prices"), 0, 100L)),
                violation.gap(), "required 99 vs offsetFor -1 → gap 100");
    }

    @Test
    void deadLetterPolicyRoutesEvictedRecordsToSink() {
        List<ParsleyRecord<String, String>> deadLettered = new ArrayList<>();
        ParsleyEngine<String, String> engine = engine(
                CausalBufferPolicy.deadLetter(CausalBufferLimit.ofSize(1), "dlq"), deadLettered::add);

        onRecord(engine, rec(ORDERS, 0, CausalDependencies.empty().advance(PRICES_ID, 0, 99)));

        assertTrue(forwarded.isEmpty());
        assertEquals(1, deadLettered.size());
        assertEquals(List.of(CausalViolationReason.LIMIT_REACHED), reasons());
    }

    @Test
    void deadLetteredRecordCarriesCausalContextHeaders() {
        List<ParsleyRecord<String, String>> deadLettered = new ArrayList<>();
        CausalDependencies required = CausalDependencies.empty().advance(PRICES_ID, 0, 5L);

        ParsleyEngine<String, String> engine = engine(
                CausalBufferPolicy.deadLetter(CausalBufferLimit.ofSize(1), "dlq"), deadLettered::add);
        onRecord(engine, rec(ORDERS, 0, required));

        assertEquals(1, deadLettered.size());
        Headers headers = deadLettered.get(0).toConsumerRecord().headers();

        assertNotNull(headers.lastHeader(CausalViolation.DLQ_REASON_HEADER));
        assertEquals("LIMIT_REACHED",
                new String(headers.lastHeader(CausalViolation.DLQ_REASON_HEADER).value(), UTF_8));

        assertNotNull(headers.lastHeader(CausalViolation.DLQ_REQUIRED_CLOCK_HEADER));
        assertEquals(required,
                CausalDependencies.fromBytes(headers.lastHeader(CausalViolation.DLQ_REQUIRED_CLOCK_HEADER).value()));

        assertNotNull(headers.lastHeader(CausalViolation.DLQ_GAP_HEADER));
        CausalDependencies decodedGap =
                CausalDependencies.fromBytes(headers.lastHeader(CausalViolation.DLQ_GAP_HEADER).value());
        assertTrue(decodedGap.dependencies().stream()
                .anyMatch(p -> p.topicId().equals(CausalPosition.deriveUuid("prices")) && p.partition() == 0),
                "gap must cover the unsatisfied partition");
    }

    @Test
    void durationPolicyExposesEvictionInterval() {
        ParsleyEngine<String, String> engine =
                engine(CausalBufferPolicy.drop(CausalBufferLimit.ofDuration(Duration.ofSeconds(5))));
        assertEquals(Duration.ofSeconds(5), engine.evictionInterval().orElseThrow());
    }

    @Test
    void frontierListenerFiresBeforeEachForward() {
        ParsleyEngine<String, String> engine = engine(CausalBufferPolicy.drop(CausalBufferLimit.ofSize(100)));

        onRecord(engine, rec(PRICES, 3, CausalDependencies.empty()));

        assertEquals(engine.frontier(), frontiers.get(frontiers.size() - 1));
    }

    @Test
    void metricsCallbacksFireOnBufferAndRelease() {
        List<Integer> bufferedDepths = new ArrayList<>();
        List<Integer> releasedCounts = new ArrayList<>();
        List<Integer> releasedDepths = new ArrayList<>();
        ParsleyMetrics capturing = new ParsleyMetrics() {
            @Override public void recordBuffered(int depth)        { bufferedDepths.add(depth); }
            @Override public void recordReleased(int c, int depth) { releasedCounts.add(c); releasedDepths.add(depth); }
            @Override public void recordEvicted(int c)             {}
            @Override public void recordViolation()                {}
        };
        ParsleyEngine<String, String> engine = new ParsleyEngine<>(
                CausalBufferPolicy.drop(CausalBufferLimit.ofSize(100)),
                violations::add, CausalFrontier.empty(), null, frontiers::add, buffer,
                new MockWaitIndex(), capturing);

        engine.onRecord(rec(ORDERS, 0, CausalDependencies.empty().advance(PRICES_ID, 0, 3)));
        assertEquals(List.of(1), bufferedDepths, "recordBuffered fires with the new depth");
        assertTrue(releasedCounts.isEmpty());

        engine.onRecord(rec(PRICES, 3, CausalDependencies.empty()));
        assertEquals(List.of(1), releasedCounts, "recordReleased fires with the count of drained records");
        assertEquals(List.of(0), releasedDepths, "buffer is empty after the drain");
    }

    @Test
    void metricsCallbackFiresOnEviction() {
        List<Integer> evictedCounts = new ArrayList<>();
        ParsleyMetrics capturing = new ParsleyMetrics() {
            @Override public void recordBuffered(int d)        {}
            @Override public void recordReleased(int c, int d) {}
            @Override public void recordEvicted(int c)         { evictedCounts.add(c); }
            @Override public void recordViolation()            {}
        };
        ParsleyEngine<String, String> engine = new ParsleyEngine<>(
                CausalBufferPolicy.drop(CausalBufferLimit.ofSize(1)),
                violations::add, CausalFrontier.empty(), null, frontiers::add, buffer,
                new MockWaitIndex(), capturing);

        engine.onRecord(rec(ORDERS, 0, CausalDependencies.empty().advance(PRICES_ID, 0, 99)));

        assertEquals(List.of(1), evictedCounts, "recordEvicted fires with the count of evicted records");
    }

    @Test
    void selfDependencyOnly_forwardsImmediately() {
        ParsleyEngine<String, String> engine = engine(CausalBufferPolicy.drop(CausalBufferLimit.ofSize(100)));

        // Record at PRICES/0@3 whose dep clock requires PRICES/0@3 — exactly itself.
        onRecord(engine, rec(PRICES, 3, CausalDependencies.empty().advance(PRICES_ID, 0, 3)));

        assertEquals(1, forwarded.size(), "self-dep is stripped → clock empty → forwarded immediately");
        assertTrue(violations.isEmpty(), "no violation: the circular entry is silently removed");
        assertEquals(0, buffer.size(), "record must never enter the buffer");
        assertEquals(CausalFrontier.empty().advance(PRICES_ID, 0, 3), engine.frontier());
    }

    @Test
    void selfDependencyPlusReal_buffersOnRealDepOnly() {
        ParsleyEngine<String, String> engine = engine(CausalBufferPolicy.drop(CausalBufferLimit.ofSize(100)));

        // ORDERS/0@0 has self-dep on ORDERS_ID/0@0 AND a real dep on PRICES_ID/0@5.
        // After stripping the self-ref, the effective clock is {PRICES_ID/0@5}: still unsatisfied.
        CausalDependencies mixed = CausalDependencies.empty()
                .advance(ORDERS_ID, 0, 0)
                .advance(PRICES_ID, 0, 5);
        onRecord(engine, rec(ORDERS, 0, mixed));
        assertTrue(forwarded.isEmpty(), "real dep on PRICES@5 still holds the record");
        assertEquals(1, buffer.size());

        // PRICES@5 arrives — satisfies the real dep; ORDERS drains.
        onRecord(engine, rec(PRICES, 5, CausalDependencies.empty()));
        assertEquals(2, forwarded.size());
        assertEquals(PRICES, forwarded.get(0).sourcePartition());
        assertEquals(ORDERS, forwarded.get(1).sourcePartition());
    }

    private static ParsleyRecord<String, String> rec(TopicPartition tp, long offset,
                                                      Uuid topicId, CausalDependencies deps) {
        List<ParsleyHeader> headers = new ArrayList<>();
        if (deps != null) {
            headers.add(new ParsleyHeader(ParsleyAttributes.VECTOR_CLOCK, deps.toBytes()));
        }
        headers.add(new ParsleyHeader(ParsleyAttributes.SRC_TOPIC, tp.topic().getBytes(UTF_8)));
        headers.add(new ParsleyHeader(ParsleyAttributes.SRC_PARTITION, ParsleyRecord.intToBytes(tp.partition())));
        headers.add(new ParsleyHeader(ParsleyAttributes.SRC_OFFSET, ParsleyRecord.longToBytes(offset)));
        headers.add(new ParsleyHeader(ParsleyAttributes.SRC_TOPIC_ID, ParsleyRecord.uuidToBytes(topicId)));
        return new ParsleyRecord<>("k", "v", 0L, headers);
    }

    @Test
    void recreatedTopicDoesNotSatisfyDependencyOnOldIncarnation() {
        ParsleyEngine<String, String> engine =
                engine(CausalBufferPolicy.drop(CausalBufferLimit.ofSize(100)));

        Uuid oldPrices = new Uuid(0L, 1L);
        Uuid newPrices = new Uuid(0L, 2L);   // recreated — different UUID, same name + partition

        // ORDERS depends on prices-0@5 under the OLD incarnation.
        onRecord(engine, rec(ORDERS, 0, CausalPosition.deriveUuid(ORDERS.topic()),
                CausalDependencies.empty().advance(oldPrices, 0, 5L)));
        assertTrue(forwarded.isEmpty(), "order must be buffered: old-prices dependency unsatisfied");

        // A record from the NEW prices incarnation at the same offset must NOT unblock ORDERS.
        onRecord(engine, rec(PRICES, 5, newPrices, CausalDependencies.empty()));
        assertEquals(1, forwarded.size(), "only new-prices record forwarded; order still buffered");
        assertEquals(PRICES, forwarded.get(0).sourcePartition());

        // A record from the OLD prices incarnation arrives — dependency now satisfied.
        onRecord(engine, rec(PRICES, 5, oldPrices, CausalDependencies.empty()));
        assertEquals(3, forwarded.size(), "old-prices record forwarded, then buffered order released");
        assertEquals(ORDERS, forwarded.get(2).sourcePartition());
    }
}
