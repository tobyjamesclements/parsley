package io.github.tobyjamesclements.parsley.core;

import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The clock's algebra and its wire form — the byte layout every stamp travels as. */
class ClockTest {

    private static final Channel C1 = new Channel(UUID.nameUUIDFromBytes("c1".getBytes()), 0);
    private static final Channel C2 = new Channel(UUID.nameUUIDFromBytes("c2".getBytes()), 3);

    /** Serialization round-trips exactly, and equal clocks are byte-identical (sorted form). */
    @Test
    void serializationRoundTrips() {
        Clock k = new Clock();
        k.advanceTo(C1, 42);
        k.advanceTo(C2, 7);
        Clock back = Clock.deserialize(k.serialize());
        assertEquals(k, back);
        assertEquals(0, java.util.Arrays.compare(k.serialize(), back.serialize()));
    }

    /** Watermarks never regress: advanceTo and mergeMax are max-folds. */
    @Test
    void advanceNeverLowers() {
        Clock k = Clock.of(C1, 10);
        k.advanceTo(C1, 5);
        assertEquals(10, k.get(C1));
        Clock other = Clock.of(C1, 3);
        k.mergeMax(other);
        assertEquals(10, k.get(C1));
    }

    /** Dominance is pointwise, with absent channels claiming nothing. */
    @Test
    void dominanceIsPointwise() {
        Clock big = new Clock();
        big.advanceTo(C1, 10);
        big.advanceTo(C2, 5);
        Clock small = Clock.of(C1, 10);
        assertTrue(big.dominates(small));
        assertFalse(small.dominates(big));
        assertTrue(big.dominates(new Clock()), "everything dominates the empty clock");
    }

    /** Truncation drops entries at or below the stability bound, and only those. */
    @Test
    void truncationDropsOnlyStableEntries() {
        Clock k = new Clock();
        k.advanceTo(C1, 10);
        k.advanceTo(C2, 5);
        Clock stability = Clock.of(C1, 10);
        k.truncateAtOrBelow(stability);
        assertEquals(Clock.NOTHING, k.get(C1), "entry at the bound is dropped");
        assertEquals(5, k.get(C2), "entry above/outside the bound survives");
    }

    /** Restriction keeps only the given channels. */
    @Test
    void restrictionFilters() {
        Clock k = new Clock();
        k.advanceTo(C1, 1);
        k.advanceTo(C2, 2);
        Clock r = k.restrictedTo(Set.of(C2));
        assertEquals(Clock.NOTHING, r.get(C1));
        assertEquals(2, r.get(C2));
    }

    /** Sequence claims round-trip, merge as max per (channel, sender), and normalise away. */
    @Test
    void sequenceClaimsRoundTripAndNormalize() {
        UUID sender = UUID.nameUUIDFromBytes("s1".getBytes());
        Clock k = new Clock();
        k.advanceTo(C1, 10);
        k.advanceSeq(C2, sender, 4);
        Clock back = Clock.deserialize(k.serialize());
        assertEquals(k, back);
        assertEquals(4, back.getSeq(new Clock.SeqKey(C2, sender)));

        Clock other = new Clock();
        other.advanceSeq(C2, sender, 2);
        other.advanceSeq(C1, sender, 7);
        k.mergeMax(other);
        assertEquals(4, k.getSeq(new Clock.SeqKey(C2, sender)), "max-merge keeps the higher seq");
        assertEquals(7, k.getSeq(new Clock.SeqKey(C1, sender)));

        k.normalizeSeq(new Clock.SeqKey(C2, sender), 42);
        assertEquals(Clock.NOTHING, k.getSeq(new Clock.SeqKey(C2, sender)));
        assertEquals(42, k.get(C2), "normalisation upgrades the claim to offset space");
    }

    /** A present but undecodable clock throws — it must never read as empty. */
    @Test
    void corruptBytesFailClosed() {
        assertThrows(CorruptClockException.class, () -> Clock.deserialize(new byte[] {1, 2, 3}));
        assertThrows(CorruptClockException.class, () -> Clock.deserialize(new byte[] {99}));
        byte[] valid = Clock.of(C1, 1).serialize();
        byte[] shortened = java.util.Arrays.copyOf(valid, valid.length - 4);
        assertThrows(CorruptClockException.class, () -> Clock.deserialize(shortened));
    }
}
