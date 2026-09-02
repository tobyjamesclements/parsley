package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.common.errors.InconsistentGroupProtocolException;
import org.apache.kafka.streams.errors.StreamsException;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import io.github.tobyjamesclements.parsley.core.ParsleyFailClosedException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the two mechanisms behind a concurrent cold start (D48's residual S1, closed by
 * D108) over their seams, deterministically: the pre-start wait for other instances'
 * bootstrap members, and the decision to replace a stream thread whose join a lingering
 * member refused. {@code ConcurrentColdStartIntegrationTest} corroborates both on a real
 * broker, where the collision window is probabilistic.
 */
class StreamsJoinCollisionTest {

    /**
     * A join refused as a protocol conflict is replaced while another instance's bootstrap
     * member can still be lingering, and not after: past the deadline the conflict is
     * persistent, and the client stops with a substrate diagnosis that names the window and
     * the application id. Any other failure is never replaced.
     */
    @Test
    void aRefusedJoinIsReplacedOnlyWhileTheBootstrapWindowIsOpen() {
        Throwable collision = new StreamsException("join", new InconsistentGroupProtocolException("protocol"));
        assertTrue(ParsleyRuntime.shouldReplaceThread(collision, 5, 10),
                "a collision before the deadline replaces the thread");
        assertFalse(ParsleyRuntime.shouldReplaceThread(collision, 10, 10),
                "a collision at the deadline is persistent and stops the client");
        assertFalse(ParsleyRuntime.shouldReplaceThread(new RuntimeException("something else"), 5, 10),
                "only the protocol conflict is ever replaced");

        ParsleyFailClosedException diagnosis =
                ParsleyRuntime.persistentProtocolConflict("shop-shipper", Duration.ofSeconds(20), collision);
        assertEquals(ParsleyFailClosedException.Reason.SUBSTRATE_MISCONFIGURED, diagnosis.reason(),
                "a member speaking another protocol is the substrate's condition, not the process's");
        assertTrue(diagnosis.getMessage().contains("shop-shipper") && diagnosis.getMessage().contains("PT20S"),
                "the diagnosis names the group and the window: " + diagnosis.getMessage());
        assertSame(collision, diagnosis.getCause(), "the refused join stays attached as the cause");
    }

    /**
     * The wait for other instances' bootstrap members polls until they leave, gives up at
     * its bound with the member still present, treats a failed describe as no evidence, and
     * ends on an interrupted sleep; the sleeps between polls are the hundred milliseconds
     * D108 records.
     */
    @Test
    void theBootstrapMemberWaitEndsWhenTheMemberLeavesAndGivesUpAtTheBound() {
        AtomicInteger polls = new AtomicInteger();
        AtomicLong clock = new AtomicLong();
        List<Long> sleeps = new ArrayList<>();
        java.util.function.LongPredicate sleeper = millis -> {
            sleeps.add(millis);
            clock.addAndGet(millis * 1_000_000L);
            return true;
        };

        assertTrue(ParsleyRuntime.awaitMembersGone(() -> polls.incrementAndGet() < 3, 10_000_000_000L,
                        clock::get, sleeper),
                "the wait ends once the member has left");
        assertEquals(3, polls.get(), "the member is polled until it leaves");
        assertEquals(List.of(100L, 100L), sleeps, "one hundred-millisecond sleep between polls");

        sleeps.clear();
        clock.set(0);
        assertFalse(ParsleyRuntime.awaitMembersGone(() -> true, 1_000_000_000L, clock::get, sleeper),
                "a member that never leaves gives the wait up at its bound");
        assertEquals(11, sleeps.size(),
                "the wait gives up on the first poll strictly past its bound: ten sleeps reach it, one more passes it");

        sleeps.clear();
        assertTrue(ParsleyRuntime.awaitMembersGone(() -> {
                    throw new IllegalStateException("describe failed");
                }, 1_000_000_000L, clock::get, sleeper),
                "a failed describe is not evidence of a member; the join itself is guarded");
        assertTrue(sleeps.isEmpty(), "nothing is waited for on a failed describe");

        clock.set(0);
        assertTrue(ParsleyRuntime.awaitMembersGone(() -> true, 1_000_000_000L, clock::get, millis -> false),
                "an interrupted sleep ends the wait");
    }
}
