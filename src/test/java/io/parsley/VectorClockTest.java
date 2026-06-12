package io.parsley;

import io.parsley.internal.Attributes;
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

class VectorClockTest {

    private static final TopicPartition P0 = new TopicPartition("prices", 0);
    private static final TopicPartition O0 = new TopicPartition("orders", 0);

    @Test
    void emptyClockIsSatisfiedByAnything() {
        assertTrue(VectorClock.empty().satisfiedBy(VectorClock.empty()));
        assertTrue(VectorClock.empty().satisfiedBy(VectorClock.empty().advance(P0, 7)));
    }

    @Test
    void advanceTakesTheMaximum() {
        VectorClock clock = VectorClock.empty().advance(P0, 5).advance(P0, 2);
        assertEquals(Map.of(P0, 5L), clock.positions());
    }

    @Test
    void satisfiedByRequiresEveryPartitionToBeCaughtUp() {
        VectorClock required = VectorClock.empty().advance(P0, 3);
        assertFalse(required.satisfiedBy(VectorClock.empty()));
        assertFalse(required.satisfiedBy(VectorClock.empty().advance(P0, 2)));
        assertTrue(required.satisfiedBy(VectorClock.empty().advance(P0, 3)));
        assertTrue(required.satisfiedBy(VectorClock.empty().advance(P0, 4)));
    }

    @Test
    void mergeTakesPerPartitionMaximum() {
        VectorClock a = VectorClock.empty().advance(P0, 3).advance(O0, 1);
        VectorClock b = VectorClock.empty().advance(P0, 1).advance(O0, 9);
        assertEquals(Map.of(P0, 3L, O0, 9L), a.merge(b).positions());
    }

    @Test
    void serialisationRoundTrips() {
        VectorClock clock = VectorClock.empty().advance(P0, 42).advance(O0, 7);
        assertEquals(clock, VectorClock.fromBytes(clock.toBytes()));
    }

    @Test
    void emptyClockSerialisationRoundTrips() {
        assertEquals(VectorClock.empty(), VectorClock.fromBytes(VectorClock.empty().toBytes()));
    }

    @Test
    void fromBytesRejectsGarbage() {
        assertThrows(IllegalStateException.class, () -> VectorClock.fromBytes(new byte[]{1, 2, 3}));
    }

    @Test
    void fromHeadersReadsTheStampedClock() {
        VectorClock clock = VectorClock.empty().advance(P0, 12).advance(O0, 4);
        Headers headers = new RecordHeaders();
        headers.add(new RecordHeader(Attributes.VECTOR_CLOCK, clock.toBytes()));

        assertEquals(Optional.of(clock), VectorClock.fromHeaders(headers));
    }

    @Test
    void fromHeadersIsEmptyWhenNoClockHeaderPresent() {
        Headers headers = new RecordHeaders();
        headers.add(new RecordHeader("trace-id", "abc".getBytes()));

        assertEquals(Optional.empty(), VectorClock.fromHeaders(headers));
    }

    @Test
    void fromRecordReadsTheStampedClock() {
        VectorClock clock = VectorClock.empty().advance(P0, 27);
        ConsumerRecord<String, String> record = new ConsumerRecord<>("orders", 0, 5L, "k", "v");
        record.headers().add(new RecordHeader(Attributes.VECTOR_CLOCK, clock.toBytes()));

        assertEquals(Optional.of(clock), VectorClock.fromRecord(record));
    }

    @Test
    void fromRecordIsEmptyWhenRecordCarriesNoClock() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("orders", 0, 5L, "k", "v");
        assertEquals(Optional.empty(), VectorClock.fromRecord(record));
    }

    @Test
    void positionsAreDefensivelyCopied() {
        java.util.Map<TopicPartition, Long> mutable = new java.util.HashMap<>();
        mutable.put(P0, 1L);
        VectorClock clock = new VectorClock(mutable);
        mutable.put(P0, 99L);
        assertEquals(1L, clock.positions().get(P0));
    }
}
