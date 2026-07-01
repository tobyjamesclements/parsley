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
     * {@code stamp} preserves a pre-existing header unrelated to the dependencies header — only the
     * dependencies header itself is replaced; every other header on the input record is carried
     * through untouched.
     *
     * Asserts that the stamped record carries both the original unrelated header and the new
     * dependencies header.
     */
    @Test
    void stampPreservesUnrelatedExistingHeaders() {
        ProducerRecord<String, String> input = new ProducerRecord<>("orders", "k", "v");
        input.headers().add("trace-id", "abc".getBytes());
        CausalDependencies deps = CausalDependencies.builder(TOPICS).require("prices", 0, 3).build();

        ProducerRecord<String, String> stamped = deps.stamp(input);

        assertEquals("abc", new String(stamped.headers().lastHeader("trace-id").value()),
                "an unrelated header on the input record must be preserved by stamp");
        assertEquals(Optional.of(deps), CausalDependencies.fromRecord(asConsumerRecord(stamped)),
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
     * {@code observe} folds a consumed record into an empty accumulator as the dependencies the
     * consumed record carried, unioned with the consumed record's own position.
     *
     * Asserts that the result equals the carried dependencies plus the {@code (topic, partition,
     * offset)} of the consumed record.
     */
    @Test
    void observeUnionsCarriedDependenciesWithTheConsumedRecordsOwnPosition() {
        CausalDependencies carried = CausalDependencies.builder(TOPICS).require("prices", 0, 4).build();
        ConsumerRecord<String, String> consumed = new ConsumerRecord<>("orders", 0, 11L, "k", "v");
        consumed.headers().add(ParsleyHeader.CAUSAL_DEPENDENCIES, carried.toBytes());

        CausalDependencies expected = CausalDependencies.builder(TOPICS)
                .require("prices", 0, 4).require("orders", 0, 11).build();
        assertEquals(expected, CausalDependencies.using(TOPICS).observe(consumed),
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
        ConsumerRecord<String, String> consumed = new ConsumerRecord<>("orders", 0, 6L, "k", "v");

        CausalDependencies expected = CausalDependencies.builder(TOPICS).require("orders", 0, 6).build();
        assertEquals(expected, CausalDependencies.using(TOPICS).observe(consumed),
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
        ConsumerRecord<String, String> priceLow = new ConsumerRecord<>("prices", 0, 3L, "k", "v");
        ConsumerRecord<String, String> priceHigh = new ConsumerRecord<>("prices", 0, 7L, "k", "v");
        CausalDependencies orderCarried = CausalDependencies.builder(TOPICS).require("prices", 0, 2).build();
        ConsumerRecord<String, String> order = new ConsumerRecord<>("orders", 0, 5L, "k", "v");
        order.headers().add(ParsleyHeader.CAUSAL_DEPENDENCIES, orderCarried.toBytes());

        CausalDependencies frontier = CausalDependencies.using(TOPICS)
                .observe(priceLow)
                .observe(priceHigh)
                .observe(order);

        CausalDependencies expected = CausalDependencies.builder(TOPICS)
                .require("prices", 0, 7).require("orders", 0, 5).build();
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
        ConsumerRecord<String, String> consumed = new ConsumerRecord<>("orders", 0, 0L, "k", "v");
        assertThrows(IllegalStateException.class, () -> CausalDependencies.empty().observe(consumed),
                "observe must throw when no CausalTopics resolver is bound");
    }

    /**
     * The resolver rejects a topic name it does not know, rather than inventing a UUID.
     *
     * Asserts that resolving an unregistered topic throws {@code IllegalArgumentException}.
     */
    @Test
    void resolverRejectsAnUnknownTopic() {
        ConsumerRecord<String, String> consumed = new ConsumerRecord<>("unknown", 0, 0L, "k", "v");
        assertThrows(IllegalArgumentException.class, () -> CausalDependencies.using(TOPICS).observe(consumed),
                "observe must throw when the consumed record's topic cannot be resolved to a UUID");
    }

    /**
     * {@code isWatermark} distinguishes a Parsley protocol watermark — identified by its marker header
     * — from an ordinary business record, so a plain Kafka client can decide which records to surface
     * to application code.
     *
     * Asserts that a record carrying the watermark marker header is reported as a watermark, and a
     * plain business record is not.
     */
    @Test
    void isWatermarkDistinguishesWatermarksFromBusinessRecords() {
        ConsumerRecord<String, String> business = new ConsumerRecord<>("orders", 0, 1L, "k", "v");
        ConsumerRecord<String, String> watermark =
                watermarkRecord("orders", 0, 2L, CausalDependencies.empty());

        assertFalse(CausalDependencies.isWatermark(business),
                "a business record must not be reported as a watermark");
        assertTrue(CausalDependencies.isWatermark(watermark),
                "a record carrying the watermark marker header must be reported as a watermark");
    }

    /**
     * {@code observe} folds a watermark's carried completeness frontier into the accumulator but never
     * its own position: a watermark is protocol metadata occupying an offset with no business payload,
     * so counting that offset would force downstream to wait on a record that delivers nothing. This
     * mirrors how a Parsley engine folds a received watermark.
     *
     * Asserts that observing a watermark on {@code orders@99} carrying {@code prices@4} yields exactly
     * {@code prices@4} — the carried frontier, with no {@code orders} coordinate from the watermark's
     * own position.
     */
    @Test
    void observeFoldsAWatermarksCarriedFrontierButNotItsOwnPosition() {
        CausalDependencies carriedFrontier = CausalDependencies.builder(TOPICS).require("prices", 0, 4).build();
        ConsumerRecord<String, String> watermark = watermarkRecord("orders", 0, 99L, carriedFrontier);

        CausalDependencies expected = CausalDependencies.builder(TOPICS).require("prices", 0, 4).build();
        assertEquals(expected, CausalDependencies.using(TOPICS).observe(watermark),
                "observe must fold a watermark's carried frontier but not its own (topic, partition, offset)");
    }

    /**
     * A running {@code observe} accumulator advances across a service that emitted only a watermark on
     * this path: the watermark's carried frontier lifts the client's frontier to upstream completeness
     * the client never saw a business record for, keeping the per-coordinate maximum.
     *
     * Asserts that after a business record on {@code prices@2} and then a watermark carrying
     * {@code prices@6}, the frontier is {@code prices@6} — advanced by the watermark — with no
     * coordinate for the watermark's own topic.
     */
    @Test
    void observeAdvancesARunningFrontierAcrossAWatermarkOnlyService() {
        ConsumerRecord<String, String> price = new ConsumerRecord<>("prices", 0, 2L, "k", "v");
        CausalDependencies upstreamComplete = CausalDependencies.builder(TOPICS).require("prices", 0, 6).build();
        ConsumerRecord<String, String> watermark = watermarkRecord("orders", 0, 50L, upstreamComplete);

        CausalDependencies frontier = CausalDependencies.using(TOPICS).observe(price).observe(watermark);

        CausalDependencies expected = CausalDependencies.builder(TOPICS).require("prices", 0, 6).build();
        assertEquals(expected, frontier,
                "a watermark must advance the running frontier to the upstream completeness it carries");
    }

    private static ConsumerRecord<String, String> watermarkRecord(
            String topic, int partition, long offset, CausalDependencies carriedFrontier) {
        ConsumerRecord<String, String> watermark = new ConsumerRecord<>(topic, partition, offset, null, null);
        watermark.headers().add(ParsleyHeader.WATERMARK, new byte[0]);
        watermark.headers().add(ParsleyHeader.CAUSAL_DEPENDENCIES, carriedFrontier.toBytes());
        return watermark;
    }

    private static ConsumerRecord<String, String> asConsumerRecord(ProducerRecord<String, String> record) {
        ConsumerRecord<String, String> consumed =
                new ConsumerRecord<>(record.topic(), 0, 0L, record.key(), record.value());
        record.headers().forEach(h -> consumed.headers().add(h.key(), h.value()));
        return consumed;
    }
}
