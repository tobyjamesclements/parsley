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

    private static final Uuid T1_ID = Uuid.randomUuid();
    private static final Uuid T2_ID = Uuid.randomUuid();

    /**
     * An empty dependency clock carries no requirements and is therefore satisfied by any
     * frontier, including an empty one.
     *
     * Asserts that {@code CausalDependencies.empty().isSatisfiedBy()} returns {@code true}
     * for both an empty frontier and a non-empty frontier.
     */
    @Test
    void emptyClockIsSatisfiedByAnything() {
        assertTrue(CausalDependencies.empty().isSatisfiedBy(CausalFrontier.empty()),
                "empty deps must be satisfied by an empty frontier");
        assertTrue(CausalDependencies.empty().isSatisfiedBy(CausalFrontier.empty().observe(new CausalPosition(T1_ID, 0, 7))),
                "empty deps must be satisfied by a non-empty frontier");
    }

    /**
     * When two entries for the same (topicId, partition) are added via the builder,
     * the builder retains only the higher offset, because a record at the higher offset
     * implies the lower offset has already been produced.
     *
     * Asserts that the resulting dependencies contain only the maximum required offset.
     */
    @Test
    void advanceTakesTheMaximum() {
        CausalDependencies deps = CausalDependencies.builder()
                .require(new CausalPosition(T1_ID, 0, 5))
                .require(new CausalPosition(T1_ID, 0, 2))
                .build();
        assertEquals(CausalDependencies.builder().require(new CausalPosition(T1_ID, 0, 5)).build(), deps,
                "builder must retain only the maximum required offset per (topicId, partition)");
    }

    /**
     * A non-empty dependency clock is satisfied only when the frontier has reached or
     * passed the required offset for every tracked (topicId, partition) pair.
     *
     * Asserts that {@code isSatisfiedBy} returns {@code false} when the frontier is behind
     * the required offset and {@code true} when the frontier is at or ahead of it.
     */
    @Test
    void satisfiedByRequiresEveryPartitionToBeCaughtUp() {
        CausalDependencies required = CausalDependencies.builder().require(new CausalPosition(T1_ID, 0, 3)).build();
        assertFalse(required.isSatisfiedBy(CausalFrontier.empty()),
                "empty frontier cannot satisfy a non-empty dependency");
        assertFalse(required.isSatisfiedBy(CausalFrontier.empty().observe(new CausalPosition(T1_ID, 0, 2))),
                "frontier at offset 2 does not satisfy a requirement of offset 3");
        assertTrue(required.isSatisfiedBy(CausalFrontier.empty().observe(new CausalPosition(T1_ID, 0, 3))),
                "frontier at exactly the required offset must be satisfied");
        assertTrue(required.isSatisfiedBy(CausalFrontier.empty().observe(new CausalPosition(T1_ID, 0, 4))),
                "frontier ahead of the required offset must be satisfied");
    }

    /**
     * Merging two frontiers produces a new frontier that takes the per-partition maximum
     * offset from each.
     *
     * Asserts that the merged frontier holds the higher offset for each (topicId, partition).
     */
    @Test
    void frontierMergeTakesPerPartitionMaximum() {
        CausalFrontier a = CausalFrontier.empty()
                .observe(new CausalPosition(T1_ID, 0, 3))
                .observe(new CausalPosition(T2_ID, 0, 1));
        CausalFrontier b = CausalFrontier.empty()
                .observe(new CausalPosition(T1_ID, 0, 1))
                .observe(new CausalPosition(T2_ID, 0, 9));
        CausalFrontier merged = a.merge(b);
        assertEquals(
                CausalFrontier.empty()
                        .observe(new CausalPosition(T1_ID, 0, 3))
                        .observe(new CausalPosition(T2_ID, 0, 9)),
                merged,
                "merged frontier must take the per-partition maximum from both frontiers");
    }

    /**
     * {@code CausalDependencies} round-trips through its binary wire format.
     *
     * Asserts that deserialising the serialised form produces an equal object.
     */
    @Test
    void serialisationRoundTrips() {
        CausalDependencies deps = CausalDependencies.builder()
                .require(new CausalPosition(T1_ID, 0, 42))
                .require(new CausalPosition(T2_ID, 0, 7))
                .build();
        assertEquals(deps, CausalDependencies.fromBytes(deps.toBytes()),
                "dependencies must round-trip through binary serialisation");
    }

    /**
     * An empty {@code CausalDependencies} round-trips through its binary wire format.
     *
     * Asserts that the empty instance survives serialisation unchanged.
     */
    @Test
    void emptyClockSerialisationRoundTrips() {
        assertEquals(CausalDependencies.empty(), CausalDependencies.fromBytes(CausalDependencies.empty().toBytes()),
                "empty dependencies must round-trip through binary serialisation");
    }

    /**
     * {@code CausalDependencies.fromBytes()} rejects byte arrays that do not contain a
     * valid wire-encoded dependency clock.
     *
     * Asserts that an {@code IllegalStateException} is thrown for garbage input.
     */
    @Test
    void fromBytesRejectsGarbage() {
        assertThrows(IllegalStateException.class, () -> CausalDependencies.fromBytes(new byte[]{1, 2, 3}),
                "fromBytes must throw for input that is not a valid dependency clock");
    }

    /**
     * {@code CausalDependencies.fromBytes()} rejects byte arrays that carry an unrecognised
     * wire-format version byte.
     *
     * Asserts that an {@code IllegalStateException} is thrown and its message identifies the
     * unrecognised version number.
     */
    @Test
    void fromBytesRejectsAnUnknownWireVersion() {
        // version byte 99 (not 1), then a well-formed empty body — must be rejected on the version.
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> CausalDependencies.fromBytes(new byte[]{99, 0, 0, 0, 0}));
        assertTrue(ex.getMessage().contains("99"),
                "error message must identify the unsupported version; got: " + ex.getMessage());
    }

    /**
     * {@code CausalDependencies.fromHeaders()} reads the causal-dependencies header and
     * deserialises it into a {@code CausalDependencies} instance.
     *
     * Asserts that the parsed result equals the original dependencies.
     */
    @Test
    void fromHeadersReadsTheStampedClock() {
        CausalDependencies deps = CausalDependencies.builder()
                .require(new CausalPosition(T1_ID, 0, 12))
                .require(new CausalPosition(T2_ID, 0, 4))
                .build();
        Headers headers = new RecordHeaders();
        headers.add(new RecordHeader(ParsleyAttributes.CAUSAL_DEPENDENCIES, deps.toBytes()));

        assertEquals(Optional.of(deps), CausalDependencies.fromHeaders(headers),
                "fromHeaders must decode the dependency header");
    }

    /**
     * {@code CausalDependencies.fromHeaders()} returns {@code Optional.empty()} when no
     * causal-dependencies header is present, without throwing.
     *
     * Asserts that the result is {@code Optional.empty()} for a headers set that contains
     * only unrelated headers.
     */
    @Test
    void fromHeadersIsEmptyWhenNoClockHeaderPresent() {
        Headers headers = new RecordHeaders();
        headers.add(new RecordHeader("trace-id", "abc".getBytes()));

        assertEquals(Optional.empty(), CausalDependencies.fromHeaders(headers),
                "fromHeaders must return empty when no dependency header is present");
    }

    /**
     * {@code CausalDependencies.fromRecord()} reads the causal-dependencies header from a
     * {@code ConsumerRecord} and deserialises it.
     *
     * Asserts that the parsed result equals the original dependencies.
     */
    @Test
    void fromRecordReadsTheStampedClock() {
        CausalDependencies deps = CausalDependencies.builder().require(new CausalPosition(T1_ID, 0, 27)).build();
        ConsumerRecord<String, String> record = new ConsumerRecord<>("t2", 0, 5L, "k", "v");
        record.headers().add(new RecordHeader(ParsleyAttributes.CAUSAL_DEPENDENCIES, deps.toBytes()));

        assertEquals(Optional.of(deps), CausalDependencies.fromRecord(record),
                "fromRecord must decode the dependency header");
    }

    /**
     * {@code CausalDependencies.fromRecord()} returns {@code Optional.empty()} when the
     * record carries no causal-dependencies header.
     *
     * Asserts that the result is {@code Optional.empty()} for a plain record.
     */
    @Test
    void fromRecordIsEmptyWhenRecordCarriesNoClock() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("t2", 0, 5L, "k", "v");
        assertEquals(Optional.empty(), CausalDependencies.fromRecord(record),
                "fromRecord must return empty when the record carries no dependency header");
    }

    /**
     * {@code findMissing()} returns an empty list when the given frontier satisfies all
     * required dependencies, including the case where the dependencies are themselves empty.
     *
     * Asserts that the gap list is empty for a satisfied frontier and for empty dependencies.
     */
    @Test
    void findMissingReturnsEmptyWhenFrontierSatisfiesDependencies() {
        CausalDependencies required = CausalDependencies.builder().require(new CausalPosition(T1_ID, 0, 3)).build();
        assertTrue(required.findMissing(CausalFrontier.empty().observe(new CausalPosition(T1_ID, 0, 3))).isEmpty(),
                "gap must be empty when frontier is at exactly the required offset");
        assertTrue(required.findMissing(CausalFrontier.empty().observe(new CausalPosition(T1_ID, 0, 9))).isEmpty(),
                "gap must be empty when frontier is ahead of the required offset");
        assertTrue(CausalDependencies.empty().findMissing(CausalFrontier.empty()).isEmpty(),
                "gap must be empty when dependencies are empty");
    }

    /**
     * {@code findMissing()} reports the per-partition shortfall when the frontier is behind
     * one or more required offsets. Each gap entry carries the difference between the
     * required offset and the current frontier position.
     *
     * Asserts that the gap set contains the correct per-partition shortfall entries.
     */
    @Test
    void findMissingReportsPerPartitionShortfall() {
        CausalDependencies required = CausalDependencies.builder()
                .require(new CausalPosition(T1_ID, 0, 5))
                .require(new CausalPosition(T2_ID, 0, 2))
                .build();
        // T1: required 5, frontier at 1 → gap 4. T2: required 2, frontier absent (-1) → gap 3.
        List<CausalPosition> gap = required.findMissing(CausalFrontier.empty().observe(new CausalPosition(T1_ID, 0, 1)));
        assertEquals(Set.of(
                new CausalPosition(T1_ID, 0, 4L),
                new CausalPosition(T2_ID, 0, 3L)),
                Set.copyOf(gap),
                "gap must reflect the per-partition shortfall for each unsatisfied dependency");
    }

    /**
     * When a dependency requires offset 0 on a partition that has never been seen by the
     * frontier, {@code findMissing()} treats the frontier position as -1 and reports a
     * gap of 1.
     *
     * Asserts that the gap list contains a single entry with the expected shortfall.
     */
    @Test
    void findMissingCountsAbsentPartitionAsMinusOne() {
        CausalDependencies required = CausalDependencies.builder().require(new CausalPosition(T1_ID, 0, 0)).build();
        List<CausalPosition> gap = required.findMissing(CausalFrontier.empty());
        assertEquals(List.of(new CausalPosition(T1_ID, 0, 1L)), gap,
                "requiring offset 0 against an unseen partition is a gap of 1");
    }

    /**
     * {@code CausalFrontier} and {@code CausalDependencies} share the same binary wire
     * format: a frontier serialised with {@code toBytes()} is decodable as dependencies
     * and vice versa.
     *
     * Asserts bidirectional round-trip compatibility between the two types.
     */
    @Test
    void frontierAndDependenciesShareWireFormat() {
        CausalFrontier frontier = CausalFrontier.empty()
                .observe(new CausalPosition(T1_ID, 0, 5))
                .observe(new CausalPosition(T2_ID, 0, 2));
        CausalDependencies decoded = CausalDependencies.fromBytes(frontier.toBytes());
        assertEquals(frontier.toDependencies(), decoded,
                "frontier serialised as bytes must be decodable as dependencies");

        CausalDependencies deps = CausalDependencies.builder()
                .require(new CausalPosition(T1_ID, 0, 5))
                .require(new CausalPosition(T2_ID, 0, 2))
                .build();
        CausalFrontier frontierDecoded = CausalFrontier.fromBytes(deps.toBytes());
        assertEquals(frontier, frontierDecoded,
                "dependencies serialised as bytes must be decodable as a frontier");
    }

    /**
     * A dependency on a coordinate under an old topic UUID is not satisfied by a frontier
     * that has advanced under a new UUID for the same topic name and partition.
     *
     * <p>Topic UUIDs uniquely identify incarnations: recreating a topic produces a new UUID
     * that is distinct from the old one even at the same partition and offset.
     *
     * Asserts that only a frontier using the old UUID satisfies the dependency.
     */
    @Test
    void recreatedTopicUuidDistinguishesFromOldIncarnation() {
        Uuid oldUuid = new Uuid(0L, 1L);
        Uuid newUuid = new Uuid(0L, 2L);   // same name, different incarnation

        CausalDependencies required = CausalDependencies.builder().require(new CausalPosition(oldUuid, 0, 5L)).build();

        assertFalse(required.isSatisfiedBy(CausalFrontier.empty().observe(new CausalPosition(newUuid, 0, 5L))),
                "new-UUID frontier must not satisfy an old-UUID dependency");
        assertTrue(required.isSatisfiedBy(CausalFrontier.empty().observe(new CausalPosition(oldUuid, 0, 5L))),
                "old-UUID frontier at matching offset must satisfy the dependency");
    }
}
