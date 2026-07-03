package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Round-trip tests for {@link EpochEvent}'s wire codec — each of the four coordination events survives
 * {@link EpochEvent#toBytes()} / {@link EpochEvent#fromBytes(byte[])}.
 */
class EpochEventTest {

    private static final Uuid T1_ID = Uuid.randomUuid();
    private static final Uuid T2_ID = Uuid.randomUuid();

    /** A {@link EpochEvent.JoinRequested} round-trips with its member id. */
    @Test
    void joinRequestedRoundTrips() {
        EpochEvent event = new EpochEvent.JoinRequested("task-0_1");
        assertEquals(event, EpochEvent.fromBytes(event.toBytes()), "JoinRequested must round-trip");
    }

    /** A {@link EpochEvent.SnapshotRequested} round-trips with its member id. */
    @Test
    void snapshotRequestedRoundTrips() {
        EpochEvent event = new EpochEvent.SnapshotRequested("task-0_2");
        assertEquals(event, EpochEvent.fromBytes(event.toBytes()), "SnapshotRequested must round-trip");
    }

    /** A {@link EpochEvent.FrontierPublished} round-trips with its member id and completeness clock. */
    @Test
    void frontierPublishedRoundTrips() {
        EpochEvent event = new EpochEvent.FrontierPublished(
                "task-0_3", ParsleyClock.empty().observe(T1_ID, 0, 7).observe(T2_ID, 1, 3));
        assertEquals(event, EpochEvent.fromBytes(event.toBytes()), "FrontierPublished must round-trip");
    }

    /** An {@link EpochEvent.EpochCommitted} round-trips with its epoch id and lower bounds. */
    @Test
    void epochCommittedRoundTrips() {
        EpochEvent event = new EpochEvent.EpochCommitted(
                42L, ParsleyClock.empty().observe(T1_ID, 0, 100));
        assertEquals(event, EpochEvent.fromBytes(event.toBytes()), "EpochCommitted must round-trip");
    }
}
