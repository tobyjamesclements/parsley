package io.parsley;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the topology-edge operations on {@link CausalDependencies}: stamping a {@link ProducerRecord},
 * merging dependency sets for a fan-in, and deriving a record's outbound dependencies from a consumed
 * {@link ConsumerRecord} via a {@link CausalTopics} resolver — the edge-of-topology operations for
 * talking to a Parsley topology from plain Kafka clients.
 */
class CausalDependenciesEdgeOpsTest {

    private static final Uuid ORDERS_ID = Uuid.randomUuid();
    private static final Uuid PRICES_ID = Uuid.randomUuid();
    private static final CausalTopics TOPICS =
            CausalTopics.of(Map.of("orders", ORDERS_ID, "prices", PRICES_ID));

    /**
     * {@code stamp} writes the dependencies into the record's header in a form {@code fromRecord} can
     * read back.
     *
     * Asserts that reading the stamped record reconstructs the original dependencies.
     */
    @Test
    void stampRoundTripsThroughFromRecord() {
        CausalDependencies deps = CausalDependencies.builder(TOPICS).require("prices", 0, 7).build();
        ProducerRecord<String, String> stamped = deps.stamp(new ProducerRecord<>("orders", "k", "v"));

        ConsumerRecord<String, String> consumed = asConsumerRecord(stamped);
        assertEquals(Optional.of(deps), CausalDependencies.fromRecord(consumed),
                "a stamped record must read back as the dependencies that stamped it");
    }

    /**
     * Re-stamping a record that already carries a dependencies header replaces it rather than adding a
     * second one — stamping is idempotent on the header key.
     *
     * Asserts that the result carries exactly one dependencies header, holding the latest stamp.
     */
    @Test
    void stampReplacesAnExistingDependenciesHeaderWithoutDuplicating() {
        CausalDependencies first = CausalDependencies.builder(TOPICS).require("prices", 0, 1).build();
        CausalDependencies second = CausalDependencies.builder(TOPICS).require("prices", 0, 9).build();

        ProducerRecord<String, String> once = first.stamp(new ProducerRecord<>("orders", "k", "v"));
        ProducerRecord<String, String> twice = second.stamp(once);

        List<Header> depHeaders = new ArrayList<>();
        twice.headers().headers(ParsleyHeader.CAUSAL_DEPENDENCIES).forEach(depHeaders::add);
        assertEquals(1, depHeaders.size(), "re-stamping must not duplicate the dependencies header");
        assertEquals(Optional.of(second), CausalDependencies.fromRecord(asConsumerRecord(twice)),
                "re-stamping must replace the header with the latest dependencies");
    }

    /**
     * {@code stamp} never mutates the record it is given: it returns a fresh record and leaves the
     * input's headers untouched.
     *
     * Asserts that the input record carries no dependencies header after stamping, while the returned
     * record does.
     */
    @Test
    void stampDoesNotMutateTheInputRecord() {
        ProducerRecord<String, String> input = new ProducerRecord<>("orders", "k", "v");
        CausalDependencies deps = CausalDependencies.builder(TOPICS).require("prices", 0, 3).build();

        ProducerRecord<String, String> stamped = deps.stamp(input);

        assertTrue(input != stamped, "stamp must return a new record, not the input");
        assertEquals(null, input.headers().lastHeader(ParsleyHeader.CAUSAL_DEPENDENCIES),
                "stamp must not write a header onto the input record");
        assertTrue(stamped.headers().lastHeader(ParsleyHeader.CAUSAL_DEPENDENCIES) != null,
                "the returned record must carry the dependencies header");
    }

    /**
     * {@code merge} unions two dependency sets, keeping the higher offset where they overlap.
     *
     * Asserts that merging two single-coordinate dependencies yields both coordinates, and that an
     * overlapping coordinate keeps the maximum offset.
     */
    @Test
    void mergeTakesTheMaximumOffsetPerCoordinate() {
        CausalDependencies a = CausalDependencies.builder(TOPICS)
                .require("prices", 0, 5).require("orders", 0, 2).build();
        CausalDependencies b = CausalDependencies.builder(TOPICS)
                .require("prices", 0, 8).build();

        CausalDependencies expected = CausalDependencies.builder(TOPICS)
                .require("prices", 0, 8).require("orders", 0, 2).build();
        assertEquals(expected, a.merge(b),
                "merge must keep the per-coordinate maximum offset across both inputs");
    }

    /**
     * {@code from} derives a record's outbound dependencies as the dependencies the consumed record
     * carried, unioned with the consumed record's own position.
     *
     * Asserts that the result equals the carried dependencies plus the {@code (topic, partition,
     * offset)} of the consumed record.
     */
    @Test
    void fromUnionsCarriedDependenciesWithTheConsumedRecordsOwnPosition() {
        CausalDependencies carried = CausalDependencies.builder(TOPICS).require("prices", 0, 4).build();
        ConsumerRecord<String, String> consumed = new ConsumerRecord<>("orders", 0, 11L, "k", "v");
        consumed.headers().add(ParsleyHeader.CAUSAL_DEPENDENCIES, carried.toBytes());

        CausalDependencies expected = CausalDependencies.builder(TOPICS)
                .require("prices", 0, 4).require("orders", 0, 11).build();
        assertEquals(expected, CausalDependencies.from(TOPICS, consumed),
                "from must union the carried dependencies with the consumed record's own position");
    }

    /**
     * {@code from} includes the consumed record's own position even when the record carries no
     * dependencies header of its own.
     *
     * Asserts that the result is the single own-position coordinate.
     */
    @Test
    void fromIncludesOwnPositionForARecordWithNoCarriedDependencies() {
        ConsumerRecord<String, String> consumed = new ConsumerRecord<>("orders", 0, 6L, "k", "v");

        CausalDependencies expected = CausalDependencies.builder(TOPICS).require("orders", 0, 6).build();
        assertEquals(expected, CausalDependencies.from(TOPICS, consumed),
                "from must record the consumed record's own position even with no carried dependencies");
    }

    /**
     * The resolver rejects a topic name it does not know, rather than inventing a UUID.
     *
     * Asserts that resolving an unregistered topic throws {@code IllegalArgumentException}.
     */
    @Test
    void resolverRejectsAnUnknownTopic() {
        ConsumerRecord<String, String> consumed = new ConsumerRecord<>("unknown", 0, 0L, "k", "v");
        assertThrows(IllegalArgumentException.class, () -> CausalDependencies.from(TOPICS, consumed),
                "from must throw when the consumed record's topic cannot be resolved to a UUID");
    }

    private static ConsumerRecord<String, String> asConsumerRecord(ProducerRecord<String, String> record) {
        ConsumerRecord<String, String> consumed =
                new ConsumerRecord<>(record.topic(), 0, 0L, record.key(), record.value());
        record.headers().forEach(h -> consumed.headers().add(h.key(), h.value()));
        return consumed;
    }
}
