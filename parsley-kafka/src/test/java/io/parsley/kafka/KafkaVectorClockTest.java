package io.parsley.kafka;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class KafkaVectorClockTest {

    private static final TopicPartition P0 = new TopicPartition("orders", 0);
    private static final TopicPartition P1 = new TopicPartition("payments", 1);

    @Test
    void emptyClockHasNoPositions() {
        assertTrue(KafkaVectorClock.empty().positions().isEmpty());
    }

    @Test
    void advanceAddsNewPartition() {
        KafkaVectorClock clock = KafkaVectorClock.empty().advance(P0, 5L);
        assertEquals(Map.of(P0, 5L), clock.positions());
    }

    @Test
    void advanceMovesPartitionForward() {
        KafkaVectorClock clock = KafkaVectorClock.empty().advance(P0, 5L).advance(P0, 9L);
        assertEquals(Map.of(P0, 9L), clock.positions());
    }

    @Test
    void advanceNeverMovesBackwards() {
        KafkaVectorClock clock = KafkaVectorClock.empty().advance(P0, 9L).advance(P0, 5L);
        assertEquals(Map.of(P0, 9L), clock.positions());
    }

    @Test
    void satisfiedByRequiresAllPartitionsAtOrBeyondPosition() {
        KafkaVectorClock required = new KafkaVectorClock(Map.of(P0, 5L, P1, 3L));

        assertTrue(required.satisfiedBy(new KafkaVectorClock(Map.of(P0, 5L, P1, 3L))));
        assertTrue(required.satisfiedBy(new KafkaVectorClock(Map.of(P0, 6L, P1, 9L))));
        assertFalse(required.satisfiedBy(new KafkaVectorClock(Map.of(P0, 6L, P1, 2L))));
    }

    @Test
    void partitionAbsentFromFrontierIsUnsatisfied() {
        KafkaVectorClock required = new KafkaVectorClock(Map.of(P0, 0L));
        assertFalse(required.satisfiedBy(KafkaVectorClock.empty()));
        assertFalse(required.satisfiedBy(new KafkaVectorClock(Map.of(P1, 99L))));
    }

    @Test
    void emptyClockIsAlwaysSatisfied() {
        assertTrue(KafkaVectorClock.empty().satisfiedBy(KafkaVectorClock.empty()));
        assertTrue(KafkaVectorClock.empty().satisfiedBy(new KafkaVectorClock(Map.of(P0, 1L))));
    }

    @Test
    void mergeTakesPerPartitionMaximum() {
        KafkaVectorClock merged = new KafkaVectorClock(Map.of(P0, 5L, P1, 1L))
                .merge(new KafkaVectorClock(Map.of(P0, 3L, P1, 9L)));
        assertEquals(Map.of(P0, 5L, P1, 9L), merged.positions());
    }

    @Test
    void mergeUnionsDisjointPartitions() {
        KafkaVectorClock merged = new KafkaVectorClock(Map.of(P0, 1L))
                .merge(new KafkaVectorClock(Map.of(P1, 2L)));
        assertEquals(Map.of(P0, 1L, P1, 2L), merged.positions());
    }

    @Test
    void constructorDefensivelyCopiesPositions() {
        Map<TopicPartition, Long> input = new HashMap<>(Map.of(P0, 1L));
        KafkaVectorClock clock = new KafkaVectorClock(input);

        input.put(P0, 99L);

        assertEquals(Map.of(P0, 1L), clock.positions());
        assertThrows(UnsupportedOperationException.class, () -> clock.positions().put(P1, 2L));
    }

    @Test
    void clocksWithEqualPositionsAreEqual() {
        assertEquals(new KafkaVectorClock(Map.of(P0, 1L)), new KafkaVectorClock(Map.of(P0, 1L)));
        assertNotEquals(new KafkaVectorClock(Map.of(P0, 1L)), new KafkaVectorClock(Map.of(P0, 2L)));
    }

    @Test
    void toStringContainsPositions() {
        String text = KafkaVectorClock.empty().advance(P0, 5L).toString();
        assertTrue(text.contains("orders"));
        assertTrue(text.contains("5"));
    }
}
