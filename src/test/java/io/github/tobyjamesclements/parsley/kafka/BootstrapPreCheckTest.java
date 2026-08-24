package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Set;

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
 */
class BootstrapPreCheckTest {
    private static final TopicPartition P0 = new TopicPartition("t", 0);
    private static final TopicPartition P1 = new TopicPartition("t", 1);
    private static final TopicPartition FORMER = new TopicPartition("former", 0);

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
}
