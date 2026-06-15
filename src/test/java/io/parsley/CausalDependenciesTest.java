package io.parsley;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CausalDependenciesTest {

    private static final TopicPartition P0 = new TopicPartition("prices", 0);
    private static final TopicPartition O0 = new TopicPartition("orders", 0);

    @Test
    void emptyClockIsSatisfiedByAnything() {
        assertTrue(CausalDependencies.empty().satisfiedBy(CausalDependencies.empty()));
        assertTrue(CausalDependencies.empty().satisfiedBy(CausalDependencies.empty().advance(P0, 7)));
    }

    @Test
    void advanceTakesTheMaximum() {
        CausalDependencies clock = CausalDependencies.empty().advance(P0, 5).advance(P0, 2);
        assertEquals(Map.of(P0, 5L), clock.positions());
    }

    @Test
    void satisfiedByRequiresEveryPartitionToBeCaughtUp() {
        CausalDependencies required = CausalDependencies.empty().advance(P0, 3);
        assertFalse(required.satisfiedBy(CausalDependencies.empty()));
        assertFalse(required.satisfiedBy(CausalDependencies.empty().advance(P0, 2)));
        assertTrue(required.satisfiedBy(CausalDependencies.empty().advance(P0, 3)));
        assertTrue(required.satisfiedBy(CausalDependencies.empty().advance(P0, 4)));
    }

    @Test
    void mergeTakesPerPartitionMaximum() {
        CausalDependencies a = CausalDependencies.empty().advance(P0, 3).advance(O0, 1);
        CausalDependencies b = CausalDependencies.empty().advance(P0, 1).advance(O0, 9);
        assertEquals(Map.of(P0, 3L, O0, 9L), a.merge(b).positions());
    }

    @Test
    void serialisationRoundTrips() {
        CausalDependencies clock = CausalDependencies.empty().advance(P0, 42).advance(O0, 7);
        assertEquals(clock, CausalDependencies.fromBytes(clock.toBytes()));
    }

    @Test
    void emptyClockSerialisationRoundTrips() {
        assertEquals(CausalDependencies.empty(), CausalDependencies.fromBytes(CausalDependencies.empty().toBytes()));
    }

    @Test
    void fromBytesRejectsGarbage() {
        assertThrows(IllegalStateException.class, () -> CausalDependencies.fromBytes(new byte[]{1, 2, 3}));
    }

    @Test
    void fromHeadersReadsTheStampedClock() {
        CausalDependencies clock = CausalDependencies.empty().advance(P0, 12).advance(O0, 4);
        Headers headers = new RecordHeaders();
        headers.add(new RecordHeader(ParsleyAttributes.VECTOR_CLOCK, clock.toBytes()));

        assertEquals(Optional.of(clock), CausalDependencies.fromHeaders(headers));
    }

    @Test
    void fromHeadersIsEmptyWhenNoClockHeaderPresent() {
        Headers headers = new RecordHeaders();
        headers.add(new RecordHeader("trace-id", "abc".getBytes()));

        assertEquals(Optional.empty(), CausalDependencies.fromHeaders(headers));
    }

    @Test
    void fromRecordReadsTheStampedClock() {
        CausalDependencies clock = CausalDependencies.empty().advance(P0, 27);
        ConsumerRecord<String, String> record = new ConsumerRecord<>("orders", 0, 5L, "k", "v");
        record.headers().add(new RecordHeader(ParsleyAttributes.VECTOR_CLOCK, clock.toBytes()));

        assertEquals(Optional.of(clock), CausalDependencies.fromRecord(record));
    }

    @Test
    void fromRecordIsEmptyWhenRecordCarriesNoClock() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("orders", 0, 5L, "k", "v");
        assertEquals(Optional.empty(), CausalDependencies.fromRecord(record));
    }

    @Test
    void missingAgainstIsEmptyWhenTheFrontierSatisfiesTheClock() {
        CausalDependencies required = CausalDependencies.empty().advance(P0, 3);
        assertTrue(required.missingAgainst(CausalDependencies.empty().advance(P0, 3)).isEmpty());
        assertTrue(required.missingAgainst(CausalDependencies.empty().advance(P0, 9)).isEmpty());
        assertTrue(CausalDependencies.empty().missingAgainst(CausalDependencies.empty()).isEmpty());
    }

    @Test
    void missingAgainstReportsThePerPartitionShortfall() {
        CausalDependencies required = CausalDependencies.empty().advance(P0, 5).advance(O0, 2);
        // P0: required 5, observed 1 → gap 4. O0: required 2, observed absent(-1) → gap 3.
        CausalDependencies frontier = CausalDependencies.empty().advance(P0, 1);
        assertEquals(Map.of(P0, 4L, O0, 3L), required.missingAgainst(frontier));
    }

    @Test
    void missingAgainstCountsAnAbsentPositionAsMinusOne() {
        CausalDependencies required = CausalDependencies.empty().advance(P0, 0);
        assertEquals(Map.of(P0, 1L), required.missingAgainst(CausalDependencies.empty()),
                "requiring offset 0 against an unseen partition is a gap of 1");
    }

    @Test
    void positionsAreDefensivelyCopied() {
        java.util.Map<TopicPartition, Long> mutable = new java.util.HashMap<>();
        mutable.put(P0, 1L);
        CausalDependencies clock = new CausalDependencies(mutable);
        mutable.put(P0, 99L);
        assertEquals(1L, clock.positions().get(P0));
    }
}
