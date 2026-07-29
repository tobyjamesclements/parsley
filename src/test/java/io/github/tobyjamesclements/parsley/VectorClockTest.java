package io.github.tobyjamesclements.parsley;

import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The clock's algebra and its wire form — the byte layout every stamp travels as. */
class VectorClockTest {

    private static final Channel C1 = new Channel(UUID.nameUUIDFromBytes("c1".getBytes()), 0);
    private static final Channel C2 = new Channel(UUID.nameUUIDFromBytes("c2".getBytes()), 3);

    /** Serialization round-trips exactly, and equal clocks are byte-identical (sorted form). */
    @Test
    void serializationRoundTrips() {
        VectorClock k = new VectorClock();
        k.advanceTo(C1, 42);
        k.advanceTo(C2, 7);
        VectorClock back = VectorClock.deserialize(k.serialize());
        assertEquals(k, back);
        assertEquals(0, java.util.Arrays.compare(k.serialize(), back.serialize()));
    }

    /** The entry counts track the two claim maps independently. */
    @Test
    void entryCountsTrackBothClaimKinds() {
        VectorClock k = new VectorClock();
        assertEquals(0, k.offsetEntryCount(), "an empty clock has no offset entries");
        assertEquals(0, k.sequenceEntryCount(), "an empty clock has no sequence entries");
        k.advanceTo(C1, 42);
        k.advanceTo(C2, 7);
        k.advanceSeq(C1, UUID.nameUUIDFromBytes("p1".getBytes()), 3);
        assertEquals(2, k.offsetEntryCount(), "each channel's watermark counts once");
        assertEquals(1, k.sequenceEntryCount(), "each (channel, sender) claim counts once");
    }

    /** Watermarks never regress: advanceTo and mergeMax are max-folds. */
    @Test
    void advanceNeverLowers() {
        VectorClock k = VectorClock.of(C1, 10);
        k.advanceTo(C1, 5);
        assertEquals(10, k.get(C1));
        VectorClock other = VectorClock.of(C1, 3);
        k.mergeMax(other);
        assertEquals(10, k.get(C1));
    }

    /** Dominance is pointwise, with absent channels claiming nothing. */
    @Test
    void dominanceIsPointwise() {
        VectorClock big = new VectorClock();
        big.advanceTo(C1, 10);
        big.advanceTo(C2, 5);
        VectorClock small = VectorClock.of(C1, 10);
        assertTrue(big.dominates(small));
        assertFalse(small.dominates(big));
        assertTrue(big.dominates(new VectorClock()), "everything dominates the empty clock");
    }

    /** Truncation drops entries at or below the stability bound, and only those. */
    @Test
    void truncationDropsOnlyStableEntries() {
        VectorClock k = new VectorClock();
        k.advanceTo(C1, 10);
        k.advanceTo(C2, 5);
        VectorClock stability = VectorClock.of(C1, 10);
        k.truncateAtOrBelow(stability);
        assertEquals(VectorClock.NOTHING, k.get(C1), "entry at the bound is dropped");
        assertEquals(5, k.get(C2), "entry above/outside the bound survives");
    }

    /** Sequence claims round-trip, merge as max per (channel, sender), and normalise away. */
    @Test
    void sequenceClaimsRoundTripAndNormalize() {
        UUID sender = UUID.nameUUIDFromBytes("s1".getBytes());
        VectorClock k = new VectorClock();
        k.advanceTo(C1, 10);
        k.advanceSeq(C2, sender, 4);
        VectorClock back = VectorClock.deserialize(k.serialize());
        assertEquals(k, back);
        assertEquals(4, back.getSeq(new VectorClock.SeqKey(C2, sender)));

        VectorClock other = new VectorClock();
        other.advanceSeq(C2, sender, 2);
        other.advanceSeq(C1, sender, 7);
        k.mergeMax(other);
        assertEquals(4, k.getSeq(new VectorClock.SeqKey(C2, sender)), "max-merge keeps the higher seq");
        assertEquals(7, k.getSeq(new VectorClock.SeqKey(C1, sender)));

        k.normalizeSeq(new VectorClock.SeqKey(C2, sender), 42);
        assertEquals(VectorClock.NOTHING, k.getSeq(new VectorClock.SeqKey(C2, sender)));
        assertEquals(42, k.get(C2), "normalisation upgrades the claim to offset space");
    }

    /** A clock with no offset entries but live sequence claims is a valid wire form. */
    @Test
    void seqOnlyClockRoundTrips() {
        UUID sender = UUID.nameUUIDFromBytes("s1".getBytes());
        VectorClock k = new VectorClock();
        k.advanceSeq(C1, sender, 3);
        VectorClock back = VectorClock.deserialize(k.serialize());
        assertEquals(k, back, "zero offset entries must deserialize, not read as malformed");
        assertEquals(3, back.getSeq(new VectorClock.SeqKey(C1, sender)));
    }

    /** Clocks differing in either claim kind are unequal; equal clocks share a hash. */
    @Test
    void equalityDistinguishesClaims() {
        UUID sender = UUID.nameUUIDFromBytes("s1".getBytes());
        VectorClock offsets = VectorClock.of(C1, 10);
        VectorClock higher = VectorClock.of(C1, 11);
        VectorClock withSeq = VectorClock.of(C1, 10);
        withSeq.advanceSeq(C2, sender, 4);

        assertEquals(VectorClock.of(C1, 10), offsets, "same claims must be equal");
        assertEquals(VectorClock.of(C1, 10).hashCode(), offsets.hashCode(),
                "equal clocks must share a hash");
        assertFalse(offsets.equals(higher), "a different watermark must break equality");
        assertFalse(offsets.equals(withSeq), "an extra sequence claim must break equality");
        assertFalse(offsets.equals("not a clock"), "a non-clock must never be equal");
    }

    /** The hash combines both claim maps; recomputed here from their public map forms. */
    @Test
    void hashCombinesBothClaimMaps() {
        UUID sender = UUID.nameUUIDFromBytes("s1".getBytes());
        VectorClock k = new VectorClock();
        k.advanceTo(C1, 42);
        k.advanceTo(C2, 7);
        k.advanceSeq(C2, sender, 4);
        int expected = java.util.Map.of(C1, 42L, C2, 7L).hashCode() * 31
                + java.util.Map.of(new VectorClock.SeqKey(C2, sender), 4L).hashCode();
        assertEquals(expected, k.hashCode(),
                "hash must be the offset map's hash scaled, plus the sequence map's hash");
    }

    /** The text form always shows offset entries and shows sequence claims only when live. */
    @Test
    void textFormMarksSequenceClaimsOnlyWhenPresent() {
        VectorClock offsets = VectorClock.of(C1, 10);
        assertFalse(offsets.toString().isEmpty(), "the text form must show the entries");
        assertFalse(offsets.toString().contains("seq"),
                "no sequence section without sequence claims");

        offsets.advanceSeq(C2, UUID.nameUUIDFromBytes("s1".getBytes()), 4);
        assertTrue(offsets.toString().contains("seq"),
                "live sequence claims must be visible in the text form");
    }

    /** A present but undecodable clock throws — it must never read as empty. */
    @Test
    void corruptBytesFailClosed() {
        assertThrows(CorruptClockException.class, () -> VectorClock.deserialize(new byte[] {1, 2, 3}));
        assertThrows(CorruptClockException.class, () -> VectorClock.deserialize(new byte[] {99}));
        byte[] valid = VectorClock.of(C1, 1).serialize();
        byte[] shortened = java.util.Arrays.copyOf(valid, valid.length - 4);
        assertThrows(CorruptClockException.class, () -> VectorClock.deserialize(shortened));
    }
}
