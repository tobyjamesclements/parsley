package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Round-trip tests for {@link ParsleyEpochEvent}'s wire codec — each coordination event survives
 * {@link ParsleyEpochEvent#toBytes()} / {@link ParsleyEpochEvent#fromBytes(byte[])}.
 */
class ParsleyEpochEventTest {

    private static final Uuid T1_ID = Uuid.randomUuid();
    private static final Uuid T2_ID = Uuid.randomUuid();

    /** A {@link ParsleyEpochEvent.JoinRequested} round-trips with its member id, app id, input/sink topics,
     * roster view, and task total. */
    @Test
    void joinRequestedRoundTrips() {
        ParsleyEpochEvent event = new ParsleyEpochEvent.JoinRequested(
                "orders/0_1", "orders", Set.of("orders", "prices"), Set.of("enriched"),
                Set.of("orders", "payments"), 3);
        assertEquals(event, ParsleyEpochEvent.fromBytes(event.toBytes()),
                "JoinRequested must round-trip with its app id, topics, roster view, and task total");
    }

    /** A {@link ParsleyEpochEvent.JoinRequested} with no declared topics round-trips (empty sets survive). */
    @Test
    void joinRequestedWithNoTopicsRoundTrips() {
        ParsleyEpochEvent event = new ParsleyEpochEvent.JoinRequested(
                "0_5", "", Set.of(), Set.of(), Set.of("solo"), 1);
        assertEquals(event, ParsleyEpochEvent.fromBytes(event.toBytes()),
                "JoinRequested with empty topic sets must round-trip");
    }

    /** A {@link ParsleyEpochEvent.SnapshotRequested} round-trips with its member id. */
    @Test
    void snapshotRequestedRoundTrips() {
        ParsleyEpochEvent event = new ParsleyEpochEvent.SnapshotRequested("task-0_2");
        assertEquals(event, ParsleyEpochEvent.fromBytes(event.toBytes()), "SnapshotRequested must round-trip");
    }

    /** A {@link ParsleyEpochEvent.FrontierPublished} round-trips with its member id and completeness clock. */
    @Test
    void frontierPublishedRoundTrips() {
        ParsleyEpochEvent event = new ParsleyEpochEvent.FrontierPublished(
                "task-0_3", ParsleyVectorClock.empty().observe(T1_ID, 0, 7).observe(T2_ID, 1, 3));
        assertEquals(event, ParsleyEpochEvent.fromBytes(event.toBytes()), "FrontierPublished must round-trip");
    }

    /** An {@link ParsleyEpochEvent.EpochCommitted} round-trips with its epoch id, lower bounds, and roster. */
    @Test
    void epochCommittedRoundTrips() {
        ParsleyEpochEvent event = new ParsleyEpochEvent.EpochCommitted(
                42L, ParsleyVectorClock.empty().observe(T1_ID, 0, 100), Set.of("orders", "payments"));
        assertEquals(event, ParsleyEpochEvent.fromBytes(event.toBytes()),
                "EpochCommitted must round-trip with its committed roster");
    }

    /** A {@link ParsleyEpochEvent.Leave} round-trips with its member id. */
    @Test
    void leaveRoundTrips() {
        ParsleyEpochEvent event = new ParsleyEpochEvent.Leave("task-0_4");
        assertEquals(event, ParsleyEpochEvent.fromBytes(event.toBytes()), "Leave must round-trip");
    }

    /** A record on a pre-genesis-cohort tag (the old JoinRequested/EpochCommitted wire format) is rejected
     * fatally on read, rather than silently mis-parsed — a mixed-version domain must fail loud. */
    @Test
    void preCohortTagsAreRejectedFatally() {
        assertThrows(ParsleyIncompatibleEpochLogException.class,
                () -> ParsleyEpochEvent.fromBytes(taggedBody(ParsleyEpochEvent.TAG_JOIN)),
                "the pre-cohort JoinRequested tag must be rejected, not silently parsed");
        assertThrows(ParsleyIncompatibleEpochLogException.class,
                () -> ParsleyEpochEvent.fromBytes(taggedBody(ParsleyEpochEvent.TAG_COMMIT)),
                "the pre-cohort EpochCommitted tag must be rejected, not silently parsed");
    }

    /** An unrecognised tag (an incompatible binary's event) is rejected fatally rather than ignored. */
    @Test
    void unrecognisedTagIsRejectedFatally() {
        assertThrows(ParsleyIncompatibleEpochLogException.class,
                () -> ParsleyEpochEvent.fromBytes(taggedBody((byte) 99)),
                "an unrecognised event tag must be rejected fatally");
    }

    /** Trailing bytes after an otherwise-valid body (a newer, longer wire format) are rejected fatally,
     * not silently ignored — the one-level-down form of the pre-cohort compatibility hazard. */
    @Test
    void trailingBytesAfterAValidBodyAreRejectedFatally() {
        byte[] valid = new ParsleyEpochEvent.Leave("task-0_1").toBytes();
        byte[] withTrailer = java.util.Arrays.copyOf(valid, valid.length + 1);   // one extra trailing byte
        assertThrows(ParsleyIncompatibleEpochLogException.class,
                () -> ParsleyEpochEvent.fromBytes(withTrailer),
                "trailing bytes after a recognised body must be rejected fatally, not silently ignored");
    }

    /** A byte[] whose first byte is {@code tag}, followed by nothing — enough for the tag switch to trip. */
    private static byte[] taggedBody(byte tag) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeByte(tag);
            dos.flush();
            return baos.toByteArray();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
