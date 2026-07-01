package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link ParsleyFrontier}'s single-value persistence: the frontier clock and the per-channel
 * clocks both round-trip through the one {@code "f"} key of the frontier store.
 */
class ParsleyFrontierTest {

    private static final Uuid T1_ID = Uuid.randomUuid();
    private static final Uuid T2_ID = Uuid.randomUuid();
    private static final Uuid ANC_ID = Uuid.randomUuid();

    /**
     * The frontier clock and the channel clocks survive a reload from the same store — a fresh
     * {@link ParsleyFrontier} over the store reproduces both the delivered frontier and completeness,
     * without replaying any records.
     */
    @Test
    void frontierClockAndChannelsRoundTripThroughTheSingleFBlob() {
        TestKeyValueStore<String, byte[]> store =
                new TestKeyValueStore<String, byte[]>(Comparator.naturalOrder(), "frontier");

        ParsleyFrontier original = new ParsleyFrontier(store, new MockForwardedIndex());
        // Advance the contiguous frontier on T1 and record channel clocks for two inputs.
        original.deliver(T1_ID, 0, 0);
        original.deliver(T1_ID, 0, 1);
        original.channelUpdate(T1_ID, 0, ParsleyClock.empty().observe(ANC_ID, 0, 4));
        original.channelUpdate(T2_ID, 0, ParsleyClock.empty().observe(ANC_ID, 0, 7));

        ParsleyClock frontierBefore = original.snapshot();
        ParsleyClock completenessBefore = original.completeness();

        // Reload: a fresh frontier over the same store restores from the "f" blob alone.
        ParsleyFrontier restored = new ParsleyFrontier(store, new MockForwardedIndex());

        assertEquals(frontierBefore, restored.snapshot(),
                "the contiguous frontier clock must round-trip through the \"f\" blob");
        assertEquals(1L, restored.snapshot().offsetFor(T1_ID, 0),
                "T1 must restore at its delivered offset 1");
        assertEquals(completenessBefore, restored.completeness(),
                "completeness must be identical after reload — both channel clocks restored");
        assertEquals(4L, restored.completeness().offsetFor(ANC_ID, 0),
                "the shared ancestor must restore at the min across channels: min(4, 7) = 4");
    }
}
