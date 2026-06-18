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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CausalDependenciesTest {

    private static final Uuid PRICES_ID = CausalPosition.deriveUuid("prices");
    private static final Uuid ORDERS_ID = CausalPosition.deriveUuid("orders");

    @Test
    void emptyClockIsSatisfiedByAnything() {
        assertTrue(CausalDependencies.empty().isSatisfiedBy(CausalFrontier.empty()));
        assertTrue(CausalDependencies.empty().isSatisfiedBy(CausalFrontier.empty().advance(PRICES_ID, 0, 7)));
    }

    @Test
    void advanceTakesTheMaximum() {
        CausalDependencies deps = CausalDependencies.empty().advance(PRICES_ID, 0, 5).advance(PRICES_ID, 0, 2);
        assertEquals(CausalDependencies.empty().advance(PRICES_ID, 0, 5), deps);
    }

    @Test
    void satisfiedByRequiresEveryPartitionToBeCaughtUp() {
        CausalDependencies required = CausalDependencies.empty().advance(PRICES_ID, 0, 3);
        assertFalse(required.isSatisfiedBy(CausalFrontier.empty()));
        assertFalse(required.isSatisfiedBy(CausalFrontier.empty().advance(PRICES_ID, 0, 2)));
        assertTrue(required.isSatisfiedBy(CausalFrontier.empty().advance(PRICES_ID, 0, 3)));
        assertTrue(required.isSatisfiedBy(CausalFrontier.empty().advance(PRICES_ID, 0, 4)));
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
        CausalDependencies deps = CausalDependencies.empty().advance(PRICES_ID, 0, 42).advance(ORDERS_ID, 0, 7);
        assertEquals(deps, CausalDependencies.fromBytes(deps.toBytes()));
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
        CausalDependencies deps = CausalDependencies.empty().advance(PRICES_ID, 0, 12).advance(ORDERS_ID, 0, 4);
        Headers headers = new RecordHeaders();
        headers.add(new RecordHeader(ParsleyAttributes.CAUSAL_DEPENDENCIES, deps.toBytes()));

        assertEquals(Optional.of(deps), CausalDependencies.fromHeaders(headers));
    }

    @Test
    void fromHeadersIsEmptyWhenNoClockHeaderPresent() {
        Headers headers = new RecordHeaders();
        headers.add(new RecordHeader("trace-id", "abc".getBytes()));

        assertEquals(Optional.empty(), CausalDependencies.fromHeaders(headers));
    }

    @Test
    void fromRecordReadsTheStampedClock() {
        CausalDependencies deps = CausalDependencies.empty().advance(PRICES_ID, 0, 27);
        ConsumerRecord<String, String> record = new ConsumerRecord<>("orders", 0, 5L, "k", "v");
        record.headers().add(new RecordHeader(ParsleyAttributes.CAUSAL_DEPENDENCIES, deps.toBytes()));

        assertEquals(Optional.of(deps), CausalDependencies.fromRecord(record));
    }

    @Test
    void fromRecordIsEmptyWhenRecordCarriesNoClock() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("orders", 0, 5L, "k", "v");
        assertEquals(Optional.empty(), CausalDependencies.fromRecord(record));
    }

    @Test
    void missingAgainstIsEmptyWhenTheFrontierSatisfiesTheClock() {
        CausalDependencies required = CausalDependencies.empty().advance(PRICES_ID, 0, 3);
        assertTrue(required.findMissing(CausalFrontier.empty().advance(PRICES_ID, 0, 3)).isEmpty());
        assertTrue(required.findMissing(CausalFrontier.empty().advance(PRICES_ID, 0, 9)).isEmpty());
        assertTrue(CausalDependencies.empty().findMissing(CausalFrontier.empty()).isEmpty());
    }

    @Test
    void missingAgainstReportsThePerPartitionShortfall() {
        CausalDependencies required = CausalDependencies.empty().advance(PRICES_ID, 0, 5).advance(ORDERS_ID, 0, 2);
        // P0: required 5, offsetFor 1 → gap 4. O0: required 2, offsetFor absent(-1) → gap 3.
        List<CausalPosition> gap = required.findMissing(CausalFrontier.empty().advance(PRICES_ID, 0, 1));
        assertEquals(Set.of(
                new CausalPosition(CausalPosition.deriveUuid("prices"), 0, 4L),
                new CausalPosition(CausalPosition.deriveUuid("orders"), 0, 3L)),
                Set.copyOf(gap));
    }

    @Test
    void missingAgainstCountsAnAbsentPositionAsMinusOne() {
        CausalDependencies required = CausalDependencies.empty().advance(PRICES_ID, 0, 0);
        List<CausalPosition> gap = required.findMissing(CausalFrontier.empty());
        assertEquals(List.of(new CausalPosition(CausalPosition.deriveUuid("prices"), 0, 1L)), gap,
                "requiring offset 0 against an unseen partition is a gap of 1");
    }

    @Test
    void frontierAndDependenciesShareWireFormat() {
        // A frontier serialised with toBytes() must be decodable as CausalDependencies and vice versa.
        CausalFrontier frontier = CausalFrontier.empty().advance(PRICES_ID, 0, 5).advance(ORDERS_ID, 0, 2);
        CausalDependencies decoded = CausalDependencies.fromBytes(frontier.toBytes());
        assertEquals(frontier.toDependencies(), decoded);

        CausalDependencies deps = CausalDependencies.empty().advance(PRICES_ID, 0, 5).advance(ORDERS_ID, 0, 2);
        CausalFrontier frontierDecoded = CausalFrontier.fromBytes(deps.toBytes());
        assertEquals(frontier, frontierDecoded);
    }

    @Test
    void withoutSelfReference_stripsEntryAtExactSelfOffset() {
        CausalDependencies deps = CausalDependencies.empty().advance(PRICES_ID, 0, 3);
        CausalDependencies stripped = deps.withoutSelfReference(PRICES_ID, 0, 3);
        assertEquals(CausalDependencies.empty(), stripped,
                "entry at req==selfOffset is a self-reference and must be removed");
    }

    @Test
    void withoutSelfReference_stripsEntryAtFutureOffset() {
        CausalDependencies deps = CausalDependencies.empty().advance(PRICES_ID, 0, 10);
        CausalDependencies stripped = deps.withoutSelfReference(PRICES_ID, 0, 3);
        assertEquals(CausalDependencies.empty(), stripped,
                "entry at req>selfOffset is also circular (requires a future record) and must be removed");
    }

    @Test
    void withoutSelfReference_preservesEntryAtPriorOffset() {
        CausalDependencies deps = CausalDependencies.empty().advance(PRICES_ID, 0, 2);
        assertSame(deps, deps.withoutSelfReference(PRICES_ID, 0, 3),
                "dep on a prior record is satisfiable and must not be stripped");
    }

    @Test
    void withoutSelfReference_returnsUnchangedWhenCoordinateAbsent() {
        CausalDependencies deps = CausalDependencies.empty().advance(ORDERS_ID, 0, 5);
        assertSame(deps, deps.withoutSelfReference(PRICES_ID, 0, 3),
                "no entry for (topicId, partition) — nothing to strip");
    }

    @Test
    void withoutSelfReference_preservesUnrelatedEntries() {
        CausalDependencies deps = CausalDependencies.empty()
                .advance(PRICES_ID, 0, 3)
                .advance(ORDERS_ID, 0, 7);
        CausalDependencies stripped = deps.withoutSelfReference(PRICES_ID, 0, 3);
        assertEquals(CausalDependencies.empty().advance(ORDERS_ID, 0, 7), stripped,
                "only the self-referential entry is removed; other entries survive");
    }

    @Test
    void recreatedTopicUuidDistinguishesFromOldIncarnation() {
        Uuid oldUuid = new Uuid(0L, 1L);
        Uuid newUuid = new Uuid(0L, 2L);   // same name, different incarnation

        CausalDependencies required = CausalDependencies.empty().advance(oldUuid, 0, 5L);

        assertFalse(required.isSatisfiedBy(CausalFrontier.empty().advance(newUuid, 0, 5L)),
                "new-UUID frontier must not satisfy old-UUID dependency");
        assertTrue(required.isSatisfiedBy(CausalFrontier.empty().advance(oldUuid, 0, 5L)),
                "old-UUID frontier at matching offset must satisfy the dependency");
    }
}
