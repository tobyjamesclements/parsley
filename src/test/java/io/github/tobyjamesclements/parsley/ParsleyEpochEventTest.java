package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Round-trip tests for {@link ParsleyEpochEvent}'s wire codec — each coordination event survives
 * {@link ParsleyEpochEvent#toBytes()} / {@link ParsleyEpochEvent#fromBytes(byte[])}.
 */
class ParsleyEpochEventTest {

    private static final Uuid T1_ID = Uuid.randomUuid();
    private static final Uuid T2_ID = Uuid.randomUuid();

    /** A {@link ParsleyEpochEvent.JoinRequested} round-trips with its member id and declared input/sink topics. */
    @Test
    void joinRequestedRoundTrips() {
        ParsleyEpochEvent event = new ParsleyEpochEvent.JoinRequested(
                "task-0_1", Set.of("orders", "prices"), Set.of("enriched"));
        assertEquals(event, ParsleyEpochEvent.fromBytes(event.toBytes()),
                "JoinRequested must round-trip with its declared input and sink topics");
    }

    /** A {@link ParsleyEpochEvent.JoinRequested} with no declared topics round-trips (empty sets survive). */
    @Test
    void joinRequestedWithNoTopicsRoundTrips() {
        ParsleyEpochEvent event = new ParsleyEpochEvent.JoinRequested("task-0_5", Set.of(), Set.of());
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
                "task-0_3", ParsleyClock.empty().observe(T1_ID, 0, 7).observe(T2_ID, 1, 3));
        assertEquals(event, ParsleyEpochEvent.fromBytes(event.toBytes()), "FrontierPublished must round-trip");
    }

    /** An {@link ParsleyEpochEvent.EpochCommitted} round-trips with its epoch id and lower bounds. */
    @Test
    void epochCommittedRoundTrips() {
        ParsleyEpochEvent event = new ParsleyEpochEvent.EpochCommitted(
                42L, ParsleyClock.empty().observe(T1_ID, 0, 100));
        assertEquals(event, ParsleyEpochEvent.fromBytes(event.toBytes()), "EpochCommitted must round-trip");
    }

    /** A {@link ParsleyEpochEvent.Leave} round-trips with its member id. */
    @Test
    void leaveRoundTrips() {
        ParsleyEpochEvent event = new ParsleyEpochEvent.Leave("task-0_4");
        assertEquals(event, ParsleyEpochEvent.fromBytes(event.toBytes()), "Leave must round-trip");
    }
}
