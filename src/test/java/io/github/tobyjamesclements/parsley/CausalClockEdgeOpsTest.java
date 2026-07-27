package io.github.tobyjamesclements.parsley;

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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the topology-edge operations on {@link CausalClock}: stamping a {@link ProducerRecord},
 * merging dependency sets for a fan-in, and deriving a record's outbound dependencies from a consumed
 * {@link ConsumerRecord} via a {@link ParsleyTopics} resolver — the edge-of-topology operations for
 * talking to a Parsley topology from plain Kafka clients.
 */
class CausalClockEdgeOpsTest {

    private static final Uuid C2_ID = Uuid.randomUuid();
    private static final Uuid C1_ID = Uuid.randomUuid();
    private static final ParsleyTopics TOPICS =
            ParsleyTopics.of(Map.of("c2", C2_ID, "c1", C1_ID));

    /**
     * {@code stamp} writes the dependencies into the record's header in a form {@code fromRecord} can
     * read back.
     *
     * Asserts that reading the stamped record reconstructs the original dependencies.
     */
    @Test
    void stampRoundTripsThroughFromRecord() {
        CausalClock deps = CausalClock.builder(TOPICS).require("c1", 0, 7).build();
        ProducerRecord<String, String> stamped = deps.stamp(new ProducerRecord<>("c2", "k", "v"));

        ConsumerRecord<String, String> consumed = asConsumerRecord(stamped);
        assertEquals(Optional.of(deps), CausalClock.fromRecord(consumed),
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
        CausalClock first = CausalClock.builder(TOPICS).require("c1", 0, 1).build();
        CausalClock second = CausalClock.builder(TOPICS).require("c1", 0, 9).build();

        ProducerRecord<String, String> once = first.stamp(new ProducerRecord<>("c2", "k", "v"));
        ProducerRecord<String, String> twice = second.stamp(once);

        List<Header> depHeaders = new ArrayList<>();
        twice.headers().headers(ParsleyHeader.CAUSAL_CLOCK).forEach(depHeaders::add);
        assertEquals(1, depHeaders.size(), "re-stamping must not duplicate the dependencies header");
        assertEquals(Optional.of(second), CausalClock.fromRecord(asConsumerRecord(twice)),
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
        ProducerRecord<String, String> m1 = new ProducerRecord<>("c2", "k", "v");
        CausalClock deps = CausalClock.builder(TOPICS).require("c1", 0, 3).build();

        ProducerRecord<String, String> stamped = deps.stamp(m1);

        assertTrue(m1 != stamped, "stamp must return a new record, not the input");
        assertEquals(null, m1.headers().lastHeader(ParsleyHeader.CAUSAL_CLOCK),
                "stamp must not write a header onto the input record");
        assertTrue(stamped.headers().lastHeader(ParsleyHeader.CAUSAL_CLOCK) != null,
                "the returned record must carry the dependencies header");
    }

    /**
     * {@code stamp} preserves a pre-existing header unrelated to the dependencies header — only the
     * dependencies header itself is replaced; every other header on the input record is carried
     * through untouched.
     *
     * Asserts that the stamped record carries both the original unrelated header and the new
     * dependencies header.
     */
    @Test
    void stampPreservesUnrelatedExistingHeaders() {
        ProducerRecord<String, String> m1 = new ProducerRecord<>("c2", "k", "v");
        m1.headers().add("trace-id", "abc".getBytes());
        CausalClock deps = CausalClock.builder(TOPICS).require("c1", 0, 3).build();

        ProducerRecord<String, String> stamped = deps.stamp(m1);

        assertEquals("abc", new String(stamped.headers().lastHeader("trace-id").value()),
                "an unrelated header on the input record must be preserved by stamp");
        assertEquals(Optional.of(deps), CausalClock.fromRecord(asConsumerRecord(stamped)),
                "the dependencies header must still be set alongside the preserved unrelated header");
    }

    /**
     * {@code merge} unions two dependency sets, keeping the higher offset where they overlap.
     *
     * Asserts that merging two single-coordinate dependencies yields both coordinates, and that an
     * overlapping coordinate keeps the maximum offset.
     */
    @Test
    void mergeTakesTheMaximumOffsetPerCoordinate() {
        CausalClock a = CausalClock.builder(TOPICS)
                .require("c1", 0, 5).require("c2", 0, 2).build();
        CausalClock b = CausalClock.builder(TOPICS)
                .require("c1", 0, 8).build();

        CausalClock expected = CausalClock.builder(TOPICS)
                .require("c1", 0, 8).require("c2", 0, 2).build();
        assertEquals(expected, a.merge(b),
                "merge must keep the per-coordinate maximum offset across both inputs");
    }

    /**
     * {@code observe} folds a consumed record into an empty accumulator as the dependencies the
     * consumed record carried, unioned with the consumed record's own position.
     *
     * Asserts that the result equals the carried dependencies plus the {@code (topic, partition,
     * offset)} of the consumed record.
     */
    @Test
    void observeUnionsCarriedDependenciesWithTheConsumedRecordsOwnPosition() {
        CausalClock carried = CausalClock.builder(TOPICS).require("c1", 0, 4).build();
        ConsumerRecord<String, String> consumed = new ConsumerRecord<>("c2", 0, 11L, "k", "v");
        consumed.headers().add(ParsleyHeader.CAUSAL_CLOCK, carried.toBytes());

        CausalClock expected = CausalClock.builder(TOPICS)
                .require("c1", 0, 4).require("c2", 0, 11).build();
        assertEquals(expected, CausalClock.using(TOPICS).observe(consumed),
                "observe must union the carried dependencies with the consumed record's own position");
    }

    /**
     * {@code observe} includes the consumed record's own position even when the record carries no
     * dependencies header of its own.
     *
     * Asserts that the result is the single own-position coordinate.
     */
    @Test
    void observeIncludesOwnPositionForARecordWithNoCarriedDependencies() {
        ConsumerRecord<String, String> consumed = new ConsumerRecord<>("c2", 0, 6L, "k", "v");

        CausalClock expected = CausalClock.builder(TOPICS).require("c2", 0, 6).build();
        assertEquals(expected, CausalClock.using(TOPICS).observe(consumed),
                "observe must record the consumed record's own position even with no carried dependencies");
    }

    /**
     * {@code observe} accumulates across successive records, so a running instance acts as the
     * consumer's own causal frontier: folding in several consumed records yields the union of every
     * carried dependency and every record's own position, keeping the maximum offset per coordinate.
     *
     * Asserts that observing three records — two on one coordinate, one (carrying its own past) on
     * another — yields the per-coordinate maximum across all of them plus the carried past.
     */
    @Test
    void observeAccumulatesAcrossRecordsAsARunningFrontier() {
        ConsumerRecord<String, String> m1 = new ConsumerRecord<>("c1", 0, 3L, "k", "v");
        ConsumerRecord<String, String> m2 = new ConsumerRecord<>("c1", 0, 7L, "k", "v");
        CausalClock carried = CausalClock.builder(TOPICS).require("c1", 0, 2).build();
        ConsumerRecord<String, String> m3 = new ConsumerRecord<>("c2", 0, 5L, "k", "v");
        m3.headers().add(ParsleyHeader.CAUSAL_CLOCK, carried.toBytes());

        CausalClock frontier = CausalClock.using(TOPICS)
                .observe(m1)
                .observe(m2)
                .observe(m3);

        CausalClock expected = CausalClock.builder(TOPICS)
                .require("c1", 0, 7).require("c2", 0, 5).build();
        assertEquals(expected, frontier,
                "a running observe(...) accumulator must keep the per-coordinate maximum across all observed records");
    }

    /**
     * {@code observe} requires a bound resolver, so an instance created without one (here via
     * {@code empty()}) fails fast rather than silently dropping the consumed record's position.
     *
     * Asserts that calling {@code observe} on an unbound instance throws {@code IllegalStateException}.
     */
    @Test
    void observeOnAnUnboundInstanceThrows() {
        ConsumerRecord<String, String> consumed = new ConsumerRecord<>("c2", 0, 0L, "k", "v");
        assertThrows(IllegalStateException.class, () -> CausalClock.empty().observe(consumed),
                "observe must throw when no ParsleyTopics resolver is bound");
    }

    /**
     * The resolver rejects a topic name it does not know, rather than inventing a UUID.
     *
     * Asserts that resolving an unregistered topic throws {@code IllegalArgumentException}.
     */
    @Test
    void resolverRejectsAnUnknownTopic() {
        ConsumerRecord<String, String> consumed = new ConsumerRecord<>("unknown", 0, 0L, "k", "v");
        assertThrows(IllegalArgumentException.class, () -> CausalClock.using(TOPICS).observe(consumed),
                "observe must throw when the consumed record's topic cannot be resolved to a UUID");
    }

    /**
     * {@code isNullMessage} distinguishes a Parsley protocol null message — identified by its marker header
     * — from an ordinary business record, so a plain Kafka client can decide which records to surface
     * to application code.
     *
     * Asserts that a record carrying the null message marker header is reported as a null message, and a
     * plain business record is not.
     */
    @Test
    void isNullMessageDistinguishesNullMessagesFromBusinessRecords() {
        ConsumerRecord<String, String> business = new ConsumerRecord<>("c2", 0, 1L, "k", "v");
        ConsumerRecord<String, String> nullMessage =
                nullMessageRecord("c2", 0, 2L, CausalClock.empty());

        assertFalse(CausalClock.isNullMessage(business),
                "a business record must not be reported as a null message");
        assertTrue(CausalClock.isNullMessage(nullMessage),
                "a record carrying the null message marker header must be reported as a null message");
    }

    /**
     * {@code observe} folds a null message's carried vector time into the accumulator but never
     * its own position: a null message is protocol metadata occupying an offset with no business payload,
     * so counting that offset would force downstream to wait on a record that delivers nothing. This
     * mirrors how a Parsley causal-broadcast core folds a received null message.
     *
     * Asserts that observing a null message on {@code c2@99} carrying {@code c1@4} yields exactly
     * {@code c1@4} — the carried frontier, with no {@code c2} coordinate from the null message's
     * own position.
     */
    @Test
    void observeFoldsANullMessagesCarriedFrontierButNotItsOwnPosition() {
        CausalClock carriedFrontier = CausalClock.builder(TOPICS).require("c1", 0, 4).build();
        ConsumerRecord<String, String> nullMessage = nullMessageRecord("c2", 0, 99L, carriedFrontier);

        CausalClock expected = CausalClock.builder(TOPICS).require("c1", 0, 4).build();
        assertEquals(expected, CausalClock.using(TOPICS).observe(nullMessage),
                "observe must fold a null message's carried frontier but not its own (topic, partition, offset)");
    }

    /**
     * A running {@code observe} accumulator advances across a service that emitted only a null message on
     * this path: the null message's carried clock lifts the client's frontier to the upstream progress
     * the client never saw a business record for, keeping the per-coordinate maximum.
     *
     * Asserts that after a business record on {@code c1@2} and then a null message carrying
     * {@code c1@6}, the frontier is {@code c1@6} — advanced by the null message — with no
     * coordinate for the null message's own topic.
     */
    @Test
    void observeAdvancesARunningFrontierAcrossANullMessageOnlyService() {
        ConsumerRecord<String, String> m1 = new ConsumerRecord<>("c1", 0, 2L, "k", "v");
        CausalClock upstreamComplete = CausalClock.builder(TOPICS).require("c1", 0, 6).build();
        ConsumerRecord<String, String> nullMessage = nullMessageRecord("c2", 0, 50L, upstreamComplete);

        CausalClock frontier = CausalClock.using(TOPICS).observe(m1).observe(nullMessage);

        CausalClock expected = CausalClock.builder(TOPICS).require("c1", 0, 6).build();
        assertEquals(expected, frontier,
                "a null message must advance the running frontier to the upstream progress it carries");
    }

    private static ConsumerRecord<String, String> nullMessageRecord(
            String topic, int partition, long offset, CausalClock carriedFrontier) {
        ConsumerRecord<String, String> nullMessage = new ConsumerRecord<>(topic, partition, offset, null, null);
        nullMessage.headers().add(ParsleyHeader.NULL_MESSAGE, new byte[0]);
        nullMessage.headers().add(ParsleyHeader.CAUSAL_CLOCK, carriedFrontier.toBytes());
        return nullMessage;
    }

    private static ConsumerRecord<String, String> asConsumerRecord(ProducerRecord<String, String> record) {
        ConsumerRecord<String, String> consumed =
                new ConsumerRecord<>(record.topic(), 0, 0L, record.key(), record.value());
        record.headers().forEach(h -> consumed.headers().add(h.key(), h.value()));
        return consumed;
    }
}
