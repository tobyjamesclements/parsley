package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Establishes when the bootstrap's stable offset listing is retried before the group join.
 *
 * <p>The stable listing silently skips any partition whose offset has a pending
 * transactional commit, so against a live sibling committing every EOS commit interval, a
 * snapshot routinely misses a partition that is not missing at all. Concluding "missing"
 * from one snapshot sends the start into the group join, which can only grind against the
 * sibling's protocol until the join deadline and then refuse a legitimate scale-out (D86).
 * The retry loop itself — relisting through the scripted-listing seam, and its
 * interrupted-refusal arm — is pinned alongside the shape predicate it consults.
 */
@Timeout(value = 30)
class BootstrapPreCheckTest {
    private static final TopicPartition P0 = new TopicPartition("t", 0);
    private static final TopicPartition P1 = new TopicPartition("t", 1);
    private static final TopicPartition FORMER = new TopicPartition("former", 0);
    private static final String APP = "app-shipper";

    /**
     * Partial coverage of the received set is the unstable-skip shape: a group with
     * committed offsets for some received partitions almost certainly has them for all,
     * and the gap is a pending commit the listing skipped. Retry before joining.
     */
    @Test
    void partialCoverageOfTheReceivedSetLooksUnstable() {
        assertTrue(ParsleyRuntime.preCheckLooksUnstable(Set.of(P0, P1), Set.of(P0)),
                "a listing covering one received partition but not the other must be retried, not joined");
    }

    /** A first start lists nothing for the received set; waiting would tax every first start. */
    @Test
    void noCoverageOfTheReceivedSetDoesNotLookUnstable() {
        assertFalse(ParsleyRuntime.preCheckLooksUnstable(Set.of(P0, P1), Set.of()),
                "an empty listing is a first start, not an unstable skip");
        assertFalse(ParsleyRuntime.preCheckLooksUnstable(Set.of(P0, P1), Set.of(FORMER)),
                "offsets only on formerly-received partitions carry no evidence about the received set");
    }

    /** Full coverage is the fast path; nothing to retry for. */
    @Test
    void fullCoverageDoesNotLookUnstable() {
        assertFalse(ParsleyRuntime.preCheckLooksUnstable(Set.of(P0, P1), Set.of(P0, P1)),
                "a listing covering every received partition needs no retry");
        assertFalse(ParsleyRuntime.preCheckLooksUnstable(Set.of(P0, P1), Set.of(P0, P1, FORMER)),
                "extra formerly-received offsets do not disturb the fast path");
    }

    /**
     * Catches the retry loop dropping its relist: a first listing missing one received
     * partition to a pending transactional commit must be relisted, and the relist that
     * covers the received set must be the listing the start acts on — adopting the
     * partial snapshot instead sends a healthy scale-out into the group join it cannot
     * win (D86).
     */
    @Test
    void aStableRelistEndsThePreCheckRetryAndIsAdopted() {
        Map<TopicPartition, OffsetAndMetadata> partial = Map.of(P0, new OffsetAndMetadata(3));
        Map<TopicPartition, OffsetAndMetadata> full =
                Map.of(P0, new OffsetAndMetadata(3), P1, new OffsetAndMetadata(4));
        AtomicInteger relists = new AtomicInteger();

        Map<TopicPartition, OffsetAndMetadata> adopted = ParsleyRuntime.awaitStablePreCheck(APP,
                Set.of(P0, P1), partial, () -> {
                    relists.incrementAndGet();
                    return full;
                });

        assertEquals(full, adopted,
                "the covering relist, not the unstable first snapshot, must be what the start"
                        + " acts on");
        assertEquals(1, relists.get(),
                "a relist that covers the received set must end the retry immediately");
    }

    /**
     * Catches the fast paths regressing into the wait: a first listing that already
     * covers the received set — and a first start's empty listing — must be adopted
     * without a single relist or sleep, or every healthy start pays the unstable-skip
     * tax (D86 promises the wait only for the partial shape).
     */
    @Test
    void aCoveringOrEmptyFirstListingIsAdoptedWithoutRelisting() {
        Map<TopicPartition, OffsetAndMetadata> full =
                Map.of(P0, new OffsetAndMetadata(3), P1, new OffsetAndMetadata(4));
        ParsleyRuntime.StableOffsetListing neverListed = () -> {
            throw new AssertionError("a listing that is not the unstable-skip shape must not be relisted");
        };

        assertEquals(full, ParsleyRuntime.awaitStablePreCheck(APP, Set.of(P0, P1), full, neverListed),
                "full first coverage is the fast path and must come back unchanged");
        assertEquals(Map.of(), ParsleyRuntime.awaitStablePreCheck(APP, Set.of(P0, P1), Map.of(), neverListed),
                "an empty first listing is a first start and must not wait at all");
    }

    /**
     * Catches the interrupt diagnosis being dropped or the interrupt being swallowed: a
     * thread interrupted while waiting out a pending commit must refuse with the
     * interrupted-while-listing message — never adopt the partial listing and join — and
     * must leave the interrupt flag set so the caller shutting the runtime down still
     * sees its own signal.
     */
    @Test
    void interruptionDuringThePreCheckRetryRefusesAndPreservesTheInterrupt() {
        Map<TopicPartition, OffsetAndMetadata> partial = Map.of(P0, new OffsetAndMetadata(3));
        try {
            Thread.currentThread().interrupt();
            IllegalStateException refusal = assertThrows(IllegalStateException.class,
                    () -> ParsleyRuntime.awaitStablePreCheck(APP, Set.of(P0, P1), partial, () -> {
                        throw new AssertionError("an interrupted wait must refuse before relisting");
                    }),
                    "an interrupted pre-check wait must refuse the start, not act on the"
                            + " unstable snapshot it was waiting to replace");
            assertTrue(refusal.getMessage().contains(
                            APP + ": interrupted while listing read positions; refusing to start"),
                    "the refusal must name the interrupted listing, not a generic failure: "
                            + refusal.getMessage());
            assertTrue(Thread.currentThread().isInterrupted(),
                    "the interrupt flag must be restored; swallowing it hides the shutdown"
                            + " signal from the caller");
        } finally {
            // Clear the flag so it cannot leak into whatever test the runner schedules next.
            Thread.interrupted();
        }
    }
}
