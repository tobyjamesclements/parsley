package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.tobyjamesclements.parsley.kafka.StartPathFixtures.assertRefusesWhenInterrupted;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Establishes when the bootstrap's stable offset listing is retried before the group join.
 *
 * <p>The stable listing silently skips any partition whose offset has a pending
 * transactional commit, so against a live sibling committing every EOS commit interval, a
 * snapshot routinely misses a partition that is not missing at all. Concluding "missing"
 * from one snapshot sends the start into the group join, which can only grind against the
 * sibling's protocol until the join deadline and then refuse a legitimate scale-out (D86).
 * Every listing — the first included — goes through the one scripted-listing seam, so
 * these tests script the whole answer sequence a start sees: the retry loop's relist
 * adoption, its fast paths, its give-up budget and its interrupted-refusal arm, alongside
 * the shape predicate it consults.
 */
@Timeout(value = 30)
class BootstrapPreCheckTest {
    private static final TopicPartition P0 = new TopicPartition("t", 0);
    private static final TopicPartition P1 = new TopicPartition("t", 1);
    private static final TopicPartition FORMER = new TopicPartition("former", 0);
    private static final String APP = "app-shipper";
    /** Scripted listings carry no real broker latency, so the loop's backoff is ~zero. */
    private static final Duration NO_BACKOFF = Duration.ZERO;
    /** A budget the scripted scenarios never exhaust: their retries end by shape, not time. */
    private static final Duration AMPLE_BUDGET = Duration.ofSeconds(5);

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
        AtomicInteger listings = new AtomicInteger();

        Map<TopicPartition, OffsetAndMetadata> adopted = ParsleyRuntime.awaitStablePreCheck(APP,
                Set.of(P0, P1), () -> listings.incrementAndGet() == 1 ? partial : full,
                NO_BACKOFF, AMPLE_BUDGET);

        assertEquals(full, adopted,
                "the covering relist, not the unstable first snapshot, must be what the start"
                        + " acts on");
        assertEquals(2, listings.get(),
                "the first look plus one covering relist must be all the seam pays: a relist"
                        + " that covers the received set must end the retry immediately");
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
        AtomicInteger coveringListings = new AtomicInteger();
        AtomicInteger emptyListings = new AtomicInteger();

        assertEquals(full, ParsleyRuntime.awaitStablePreCheck(APP, Set.of(P0, P1), () -> {
                    if (coveringListings.incrementAndGet() > 1) {
                        throw new AssertionError(
                                "a listing that is not the unstable-skip shape must not be relisted");
                    }
                    return full;
                }, NO_BACKOFF, AMPLE_BUDGET),
                "full first coverage is the fast path and must come back unchanged");
        assertEquals(Map.of(), ParsleyRuntime.awaitStablePreCheck(APP, Set.of(P0, P1), () -> {
                    if (emptyListings.incrementAndGet() > 1) {
                        throw new AssertionError("a first start's empty listing must not be relisted");
                    }
                    return Map.of();
                }, NO_BACKOFF, AMPLE_BUDGET),
                "an empty first listing is a first start and must not wait at all");
        assertEquals(1, coveringListings.get(),
                "the covering fast path must pay exactly the one first look");
        assertEquals(1, emptyListings.get(),
                "the first-start fast path must pay exactly the one first look");
    }

    /**
     * Catches the give-up budget being dropped: a listing that stays partial past the
     * retry budget is not a pending commit resolving — pending commits resolve within
     * the transaction timeout the budget models — and the retry must stop and adopt the
     * still-partial listing, falling through to the group join, which remains
     * authoritative on whether the gap is real (D86). Deleting the budget check turns
     * that fall-through into an unbounded relist loop, which the scripted listing's
     * budget converts into a visible failure rather than a hang.
     */
    @Test
    void aListingThatStaysPartialPastTheBudgetIsAdoptedForTheJoin() {
        Map<TopicPartition, OffsetAndMetadata> partial = Map.of(P0, new OffsetAndMetadata(3));
        AtomicInteger listings = new AtomicInteger();

        Map<TopicPartition, OffsetAndMetadata> adopted = ParsleyRuntime.awaitStablePreCheck(APP,
                Set.of(P0, P1), () -> {
                    if (listings.incrementAndGet() > 1_000_000) {
                        throw new AssertionError("the pre-check retry never gave up: the budget"
                                + " must bound the relist loop, not the listing count");
                    }
                    return partial;
                }, NO_BACKOFF, Duration.ofMillis(20));

        assertEquals(partial, adopted,
                "past the budget the still-partial listing is what the start acts on; the"
                        + " join it falls through to is authoritative on the gap (D86)");
        assertTrue(listings.get() >= 2,
                "the partial shape must be relisted at least once before the budget gives up,"
                        + " or the retry promised for pending commits never happened");
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
        AtomicInteger listings = new AtomicInteger();

        assertRefusesWhenInterrupted(
                () -> ParsleyRuntime.awaitStablePreCheck(APP, Set.of(P0, P1), () -> {
                    if (listings.incrementAndGet() > 1) {
                        throw new AssertionError("an interrupted wait must refuse before relisting");
                    }
                    return partial;
                }, NO_BACKOFF, AMPLE_BUDGET),
                APP + ": interrupted while listing read positions; refusing to start");
        assertEquals(1, listings.get(),
                "only the first look is paid; the interrupt fires in the wait before any"
                        + " relist");
    }

    /**
     * The retention-boundary check on re-established read positions is pinned on both sides
     * of its boundary, over coverage a real engine wrote. A re-established position of
     * covered + 1 means nothing was discarded (the next unread position is exactly where
     * retention now begins) and must start; covered + 2 means one position was discarded
     * unread and must refuse (SPEC Safety 8; D74's {@code offset - 1 > covered} spelling).
     * The broker test only reaches the wide gap, so an off-by-one that refuses every
     * legitimate expiry restart at the exact boundary stays green there.
     */
    @Test
    void reEstablishedPositionAtExactlyTheCoveredBoundaryStartsAndOnePastItRefuses() throws Exception {
        java.util.UUID topicId = new java.util.UUID(300, 1);
        io.github.tobyjamesclements.parsley.core.ChannelId channel =
                new io.github.tobyjamesclements.parsley.core.ChannelId(topicId, 0);
        io.github.tobyjamesclements.parsley.sim.MemoryOrderingStore store =
                new io.github.tobyjamesclements.parsley.sim.MemoryOrderingStore();
        io.github.tobyjamesclements.parsley.core.ProcessEngine engine =
                new io.github.tobyjamesclements.parsley.core.ProcessEngine("p", Map.of(channel, "t"), store);
        engine.onReceive(io.github.tobyjamesclements.parsley.core.EngineTestFactory.plain(channel, 41, "M"));
        engine.markDelivered(channel, 41);
        engine.flushHolds();
        store.commit();
        Map<byte[], byte[]> orderingState = new java.util.TreeMap<>(java.util.Arrays::compareUnsigned);
        store.scanPrefix(new byte[0], orderingState::put);
        Map<String, TopicInfo> topics = Map.of("t", new TopicInfo(topicId, 1));

        java.lang.reflect.Method check = ParsleyRuntime.class.getDeclaredMethod(
                "refusePositionsDiscardedUnread", String.class, Map.class, Map.class, Map.class);
        check.setAccessible(true);
        check.invoke(null, APP, topics, orderingState, Map.of(P0, new OffsetAndMetadata(42)));

        java.lang.reflect.InvocationTargetException refused = org.junit.jupiter.api.Assertions.assertThrows(
                java.lang.reflect.InvocationTargetException.class,
                () -> check.invoke(null, APP, topics, orderingState, Map.of(P0, new OffsetAndMetadata(43))),
                "one position discarded beyond coverage must refuse");
        assertTrue(refused.getCause() instanceof io.github.tobyjamesclements.parsley.core.ParsleyFailClosedException e
                        && e.reason() == io.github.tobyjamesclements.parsley.core.ParsleyFailClosedException.Reason
                                .POSITIONS_DISCARDED_UNREAD,
                () -> "expected POSITIONS_DISCARDED_UNREAD, got " + refused.getCause());
    }
}
