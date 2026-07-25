package io.github.tobyjamesclements.parsley;

import io.github.tobyjamesclements.parsley.ParsleyChannels.CoordKey;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Round-trip and wire-version tests for {@link ParsleyFrontierState}, the durable frontier-store
 * value extracted from {@link ParsleyChannels}. The extraction traded the former trailing-optional
 * section layout for a single leading wire-version byte with a hard fail on mismatch, matching
 * {@link ParsleyVectorClock} and {@link ParsleySerializer}; these tests pin both halves.
 */
final class ParsleyFrontierStateTest {

    private static final Uuid C1_ID = Uuid.randomUuid();
    private static final Uuid C2_ID = Uuid.randomUuid();
    private static final Uuid C4_ID = Uuid.randomUuid();

    /**
     * A fully populated frontier state survives a {@code toBytes()} then {@code fromBytes()} round
     * trip unchanged: every one of the seven sections (frontier, channel clocks, highest-received
     * offsets, carried ancestry, declared inputs, own outputs, declared sinks) restores equal, so
     * the within-version byte layout is deliberate rather than incidental.
     */
    @Test
    void aFullyPopulatedStateRoundTripsThroughToBytesAndFromBytes() {
        ParsleyFrontierState original = new ParsleyFrontierState(
                ParsleyVectorClock.empty().observe(C1_ID, 0, 7),
                Map.of(new CoordKey(C1_ID, 0), ParsleyVectorClock.empty().observe(C2_ID, 0, 3)),
                Map.of(new CoordKey(C1_ID, 0), 9L),
                ParsleyVectorClock.empty().observe(C2_ID, 1, 4),
                Map.of("c1", C1_ID, "c2", C2_ID),
                ParsleyVectorClock.empty().observe(C4_ID, 0, 5),
                Map.of("c4", C4_ID));

        ParsleyFrontierState restored = ParsleyFrontierState.fromBytes(original.toBytes());

        assertEquals(original, restored,
                "every persisted section must restore equal across a toBytes/fromBytes round trip");
    }

    /**
     * An empty frontier state round-trips too: every section serialises as a zero count or an empty
     * clock and reads back empty, the shape the test fixtures and a never-yet-rescoped node persist.
     */
    @Test
    void anEmptyStateRoundTripsAsEmpty() {
        ParsleyFrontierState empty = new ParsleyFrontierState(
                ParsleyVectorClock.empty(), Map.of(), Map.of(), ParsleyVectorClock.empty(),
                Map.of(), ParsleyVectorClock.empty(), Map.of());

        assertEquals(empty, ParsleyFrontierState.fromBytes(empty.toBytes()),
                "an all-empty frontier state must round-trip to an equal all-empty state");
    }

    /**
     * {@code fromBytes} hard-fails on a value whose leading wire-version byte is not
     * {@link ParsleyFrontierState#WIRE_VERSION}, rather than mis-parsing it. This is the O6 stance
     * that there is no cross-version upgrade path before 1.0: an unreadable value faults at init.
     */
    @Test
    void fromBytesRejectsAnUnknownWireVersion() {
        byte[] blob = new ParsleyFrontierState(
                ParsleyVectorClock.empty().observe(C1_ID, 0, 1),
                Map.of(), Map.of(), ParsleyVectorClock.empty(),
                Map.of(), ParsleyVectorClock.empty(), Map.of()).toBytes();
        blob[0] = (byte) (ParsleyFrontierState.WIRE_VERSION + 1);

        IllegalStateException thrown = assertThrows(IllegalStateException.class,
                () -> ParsleyFrontierState.fromBytes(blob),
                "an unrecognised wire version must fault rather than load partially");
        assertTrue(ParsleyTestFixtures.message(thrown).contains("wire version"),
                "the failure must name the wire-version mismatch: " + thrown.getMessage());
    }
}
