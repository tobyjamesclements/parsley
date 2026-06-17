package io.parsley;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CausalDependenciesTest {

    private static final Uuid PRICES_ID = CausalPosition.nameUuid("prices");
    private static final Uuid ORDERS_ID = CausalPosition.nameUuid("orders");

    @Test
    void emptyClockIsSatisfiedByAnything() {
        assertTrue(CausalDependencies.empty().satisfiedBy(CausalFrontier.empty()));
        assertTrue(CausalDependencies.empty().satisfiedBy(CausalFrontier.empty().advance(PRICES_ID, 0, 7)));
    }

    @Test
    void advanceTakesTheMaximum() {
        CausalDependencies clock = CausalDependencies.empty().advance(PRICES_ID, 0, 5).advance(PRICES_ID, 0, 2);
        assertEquals(CausalDependencies.empty().advance(PRICES_ID, 0, 5), clock);
    }

    @Test
    void satisfiedByRequiresEveryPartitionToBeCaughtUp() {
        CausalDependencies required = CausalDependencies.empty().advance(PRICES_ID, 0, 3);
        assertFalse(required.satisfiedBy(CausalFrontier.empty()));
        assertFalse(required.satisfiedBy(CausalFrontier.empty().advance(PRICES_ID, 0, 2)));
        assertTrue(required.satisfiedBy(CausalFrontier.empty().advance(PRICES_ID, 0, 3)));
        assertTrue(required.satisfiedBy(CausalFrontier.empty().advance(PRICES_ID, 0, 4)));
    }

    @Test
    void frontierMergeTakesPerPartitionMaximum() {
        CausalFrontier a = CausalFrontier.empty().advance(PRICES_ID, 0, 3).advance(ORDERS_ID, 0, 1);
        CausalFrontier b = CausalFrontier.empty().advance(PRICES_ID, 0, 1).advance(ORDERS_ID, 0, 9);
        CausalFrontier merged = a.merge(b);
        assertEquals(CausalFrontier.empty().advance(PRICES_ID, 0, 3).advance(ORDERS_ID, 0, 9), merged);
    }

    @Test
    void serialisationRoundTrips() {
        CausalDependencies clock = CausalDependencies.empty().advance(PRICES_ID, 0, 42).advance(ORDERS_ID, 0, 7);
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
    void fromBytesRejectsAnUnknownWireVersion() {
        // version byte 99 (not 1), then a well-formed empty body — must be rejected on the version.
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> CausalDependencies.fromBytes(new byte[]{99, 0, 0, 0, 0}));
        assertTrue(ex.getMessage().contains("99"), "error names the unsupported version; got: " + ex.getMessage());
    }

    @Test
    void fromHeadersReadsTheStampedClock() {
        CausalDependencies clock = CausalDependencies.empty().advance(PRICES_ID, 0, 12).advance(ORDERS_ID, 0, 4);
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
        CausalDependencies clock = CausalDependencies.empty().advance(PRICES_ID, 0, 27);
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
        CausalDependencies required = CausalDependencies.empty().advance(PRICES_ID, 0, 3);
        assertTrue(required.missingAgainst(CausalFrontier.empty().advance(PRICES_ID, 0, 3)).isEmpty());
        assertTrue(required.missingAgainst(CausalFrontier.empty().advance(PRICES_ID, 0, 9)).isEmpty());
        assertTrue(CausalDependencies.empty().missingAgainst(CausalFrontier.empty()).isEmpty());
    }

    @Test
    void missingAgainstReportsThePerPartitionShortfall() {
        CausalDependencies required = CausalDependencies.empty().advance(PRICES_ID, 0, 5).advance(ORDERS_ID, 0, 2);
        // P0: required 5, observed 1 → gap 4. O0: required 2, observed absent(-1) → gap 3.
        List<CausalPosition> gap = required.missingAgainst(CausalFrontier.empty().advance(PRICES_ID, 0, 1));
        assertEquals(Set.of(
                new CausalPosition(CausalPosition.nameUuid("prices"), 0, 4L),
                new CausalPosition(CausalPosition.nameUuid("orders"), 0, 3L)),
                Set.copyOf(gap));
    }

    @Test
    void missingAgainstCountsAnAbsentPositionAsMinusOne() {
        CausalDependencies required = CausalDependencies.empty().advance(PRICES_ID, 0, 0);
        List<CausalPosition> gap = required.missingAgainst(CausalFrontier.empty());
        assertEquals(List.of(new CausalPosition(CausalPosition.nameUuid("prices"), 0, 1L)), gap,
                "requiring offset 0 against an unseen partition is a gap of 1");
    }

    @Test
    void frontierAndDependenciesShareWireFormat() {
        // A frontier serialised with toBytes() must be decodable as CausalDependencies and vice versa.
        CausalFrontier frontier = CausalFrontier.empty().advance(PRICES_ID, 0, 5).advance(ORDERS_ID, 0, 2);
        CausalDependencies decoded = CausalDependencies.fromBytes(frontier.toBytes());
        assertEquals(frontier.asDependencies(), decoded);

        CausalDependencies deps = CausalDependencies.empty().advance(PRICES_ID, 0, 5).advance(ORDERS_ID, 0, 2);
        CausalFrontier frontierDecoded = CausalFrontier.fromBytes(deps.toBytes());
        assertEquals(frontier, frontierDecoded);
    }

    @Test
    void recreatedTopicUuidDistinguishesFromOldIncarnation() {
        Uuid oldUuid = new Uuid(0L, 1L);
        Uuid newUuid = new Uuid(0L, 2L);   // same name, different incarnation

        CausalDependencies required = CausalDependencies.empty().advance(oldUuid, 0, 5L);

        assertFalse(required.satisfiedBy(CausalFrontier.empty().advance(newUuid, 0, 5L)),
                "new-UUID frontier must not satisfy old-UUID dependency");
        assertTrue(required.satisfiedBy(CausalFrontier.empty().advance(oldUuid, 0, 5L)),
                "old-UUID frontier at matching offset must satisfy the dependency");
    }
}
