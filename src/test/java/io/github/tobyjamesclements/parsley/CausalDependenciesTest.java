package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.header.Headers;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the public {@link CausalDependencies} facade: building, wire serialisation, and reading the
 * stamped header off records. The vector-clock semantics it delegates to are covered by
 * {@link ParsleyVectorClockTest}.
 */
class CausalDependenciesTest {

    private static final ParsleyTopics TOPICS =
            ParsleyTopics.of(Map.of("t1", Uuid.randomUuid(), "t2", Uuid.randomUuid()));

    /**
     * When two entries for the same (topic, partition) are added via the builder, the builder
     * retains only the higher offset, because a record at the higher offset implies the lower offset
     * has already been produced.
     *
     * Asserts that the resulting dependencies contain only the maximum required offset.
     */
    @Test
    void requireTakesTheMaximum() {
        CausalDependencies deps = CausalDependencies.builder(TOPICS)
                .require("t1", 0, 5)
                .require("t1", 0, 2)
                .build();
        assertEquals(CausalDependencies.builder(TOPICS).require("t1", 0, 5).build(), deps,
                "builder must retain only the maximum required offset per (topic, partition)");
    }

    /**
     * {@code CausalDependencies} round-trips through its binary wire format.
     *
     * Asserts that deserialising the serialised form produces an equal object.
     */
    @Test
    void serialisationRoundTrips() {
        CausalDependencies deps = CausalDependencies.builder(TOPICS)
                .require("t1", 0, 42)
                .require("t2", 0, 7)
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
     * {@code CausalDependencies.fromBytes()} rejects byte arrays that do not contain a valid
     * wire-encoded dependency clock.
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
     * {@code CausalDependencies.fromHeaders()} reads the causal-dependencies header and deserialises
     * it into a {@code CausalDependencies} instance.
     *
     * Asserts that the parsed result equals the original dependencies.
     */
    @Test
    void fromHeadersReadsTheStampedClock() {
        CausalDependencies deps = CausalDependencies.builder(TOPICS)
                .require("t1", 0, 12)
                .require("t2", 0, 4)
                .build();
        Headers headers = ParsleyHeader.mutableHeaders();
        headers.add(ParsleyHeader.CAUSAL_DEPENDENCIES, deps.toBytes());

        assertEquals(Optional.of(deps), CausalDependencies.fromHeaders(headers),
                "fromHeaders must decode the dependency header");
    }

    /**
     * {@code CausalDependencies.fromHeaders()} returns {@code Optional.empty()} when no
     * causal-dependencies header is present, without throwing.
     *
     * Asserts that the result is {@code Optional.empty()} for a headers set that contains only
     * unrelated headers.
     */
    @Test
    void fromHeadersIsEmptyWhenNoClockHeaderPresent() {
        Headers headers = ParsleyHeader.mutableHeaders();
        headers.add("trace-id", "abc".getBytes());

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
        CausalDependencies deps = CausalDependencies.builder(TOPICS).require("t1", 0, 27).build();
        ConsumerRecord<String, String> record = new ConsumerRecord<>("t2", 0, 5L, "k", "v");
        record.headers().add(ParsleyHeader.CAUSAL_DEPENDENCIES, deps.toBytes());

        assertEquals(Optional.of(deps), CausalDependencies.fromRecord(record),
                "fromRecord must decode the dependency header");
    }

    /**
     * {@code CausalDependencies.fromRecord()} returns {@code Optional.empty()} when the record
     * carries no causal-dependencies header.
     *
     * Asserts that the result is {@code Optional.empty()} for a plain record.
     */
    @Test
    void fromRecordIsEmptyWhenRecordCarriesNoClock() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("t2", 0, 5L, "k", "v");
        assertEquals(Optional.empty(), CausalDependencies.fromRecord(record),
                "fromRecord must return empty when the record carries no dependency header");
    }
}
