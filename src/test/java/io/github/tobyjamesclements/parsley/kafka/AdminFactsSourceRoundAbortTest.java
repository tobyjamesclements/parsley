package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownTopicIdException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.github.tobyjamesclements.parsley.core.ChannelId;
import io.github.tobyjamesclements.parsley.core.PositionFacts;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Establishes that a facts round aborts — rather than degrades — when its evidence cannot
 * be gathered: a by-id describe failing for any cause other than an unknown/invalid-topic
 * answer, and an interrupt during the earliest-offset wait.
 *
 * <p>The abort is load-bearing for the verdicts: an aborted round observed nothing, so it
 * resets the confirmation windows' continuity, and a name-gone streak interrupted by a
 * broker outage must not confirm death from the two isolated sightings bracketing it
 * (D85/D88). Tolerating the failure instead would complete the round as if the broker had
 * answered — exactly the degradation D83 scoped its tolerance against: unknown-topic and
 * invalid-topic are answers about the id, everything else is an outage of the round.
 */
@Timeout(value = 30)
class AdminFactsSourceRoundAbortTest {
    private static final ChannelId R = new ChannelId(ScriptedAdminFacts.Z_ID, 0);
    private static final long WINDOW_MILLIS = ScriptedAdminFacts.WINDOW_MILLIS;

    /**
     * Fails every by-id describe future exactly as scripted, but through the real
     * {@code describeByIds} classification — the tolerate-or-abort branch under test —
     * with the by-name answer for "z" always affirmative name-gone.
     */
    static final class ScriptedDescribeFacts extends ScriptedAdminFacts {
        /** What this round's by-id describe futures complete exceptionally with. */
        volatile Exception byIdFailure = new UnknownTopicIdException("unknown topic id");

        @Override
        Map<Uuid, KafkaFuture<TopicDescription>> describeByIdFutures(Set<UUID> topicIds) {
            Map<Uuid, KafkaFuture<TopicDescription>> futures = new HashMap<>();
            for (UUID id : topicIds) {
                KafkaFutureImpl<TopicDescription> future = new KafkaFutureImpl<>();
                future.completeExceptionally(byIdFailure);
                futures.put(TopicInfo.toKafkaUuid(id), future);
            }
            return futures;
        }

        @Override
        Map<String, Object> describeByNames(Set<String> names) {
            Map<String, Object> outcome = new HashMap<>();
            if (names.contains("z")) {
                outcome.put("z", NameVerdict.NAME_GONE);
            }
            return outcome;
        }

        PositionFacts nameGoneRound(long atMillis) throws Exception {
            nowMillis.set(atMillis);
            return gather(Set.of(R), Set.of());
        }
    }

    /**
     * The rethrow that aborts a round on a describe failure whose cause is not an
     * unknown/invalid-topic answer: only those are tolerated as unanswerable ids (D83); a
     * broker-side failure must abort the whole round through the real classification, and
     * the abort resets the confirmation windows, so a name-gone streak interrupted by the
     * outage reopens rather than matures (D85). Rethreading the failure into the tolerate
     * branch would complete the outage round as observation and let two isolated
     * sightings bracketing it confirm death.
     *
     * <p>The post-outage sighting lands strictly inside the confirmation window (t=900 of
     * a 1000ms window after a t=0 anchor), because a sighting at one full window or later
     * trips {@code observeWindow}'s blind-gap restart on its own and would hide the
     * abort's reset entirely; the discriminating assertion is the one at t=1000, exactly
     * where an un-reset pre-outage anchor would mature into a confirmed death.
     */
    @Test
    void aBrokerFailureOnTheByIdDescribeAbortsTheRoundAndResetsTheStreak() throws Exception {
        ScriptedDescribeFacts facts = new ScriptedDescribeFacts();

        assertTrue(facts.nameGoneRound(0).deadChannels().isEmpty(),
                "the first name-gone observation opens the window, never confirms");

        TimeoutException outage = new TimeoutException("broker unreachable");
        facts.byIdFailure = outage;
        Exception aborted = assertThrows(Exception.class, () -> facts.nameGoneRound(WINDOW_MILLIS / 2),
                "a describe failure that is not an unknown/invalid-topic answer must abort the"
                        + " round, not be tolerated as one more unanswerable id");
        assertTrue(TestChains.chainContains(aborted, outage),
                () -> "the abort carries the broker failure for the retry log: " + aborted);
        facts.byIdFailure = new UnknownTopicIdException("unknown topic id");

        assertTrue(facts.nameGoneRound(9 * WINDOW_MILLIS / 10).deadChannels().isEmpty(),
                "the streak was interrupted by the outage: this sighting — inside the window,"
                        + " where the blind-gap rule alone would not restart it — must reopen"
                        + " the window, not extend the pre-outage anchor");
        assertTrue(facts.nameGoneRound(WINDOW_MILLIS).deadChannels().isEmpty(),
                "one window after the pre-outage anchor is exactly where an un-reset streak"
                        + " would mature: the abort's reset is what keeps this sighting from"
                        + " confirming death");
        assertTrue(facts.nameGoneRound(WINDOW_MILLIS + WINDOW_MILLIS / 2).deadChannels().isEmpty(),
                "the reopened window has not yet spanned its length");
        assertEquals(Set.of(R), facts.nameGoneRound(2 * WINDOW_MILLIS - WINDOW_MILLIS / 10).deadChannels(),
                "an unbroken post-outage run spanning the window from the reopened anchor must"
                        + " still confirm death through the real describe classification");
    }

    /**
     * Resolves "z" by id but leaves every earliest-offset future incomplete, so the round
     * blocks in the per-partition wait until something ends it.
     */
    static final class BlockedOffsetsFacts extends ScriptedAdminFacts {
        final CountDownLatch offsetWaitEntered = new CountDownLatch(1);

        @Override
        Map<UUID, String> describeByIds(Set<UUID> topicIds) {
            Map<UUID, String> names = new HashMap<>();
            for (UUID id : topicIds) {
                names.put(id, "z");
            }
            return names;
        }

        @Override
        Map<TopicPartition, KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>> earliestOffsetFutures(
                Map<TopicPartition, OffsetSpec> queries) {
            Map<TopicPartition, KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>> futures = new HashMap<>();
            for (TopicPartition tp : queries.keySet()) {
                futures.put(tp, new KafkaFutureImpl<>());
            }
            offsetWaitEntered.countDown();
            return futures;
        }
    }

    /**
     * The interrupt rethrow out of the earliest-offset wait: an interrupt — the runtime
     * tearing down the facts executor — must abort the round as interrupted, not be
     * downgraded to the per-partition "withholding its facts" warning that completes the
     * round as if the broker had merely been slow. The processor's own catch is what
     * handles the interrupt (D54's background-gather contract), so it must reach that
     * catch as an InterruptedException, not vanish inside the round.
     */
    @Test
    void anInterruptDuringTheEarliestOffsetWaitAbortsTheRound() throws Exception {
        BlockedOffsetsFacts facts = new BlockedOffsetsFacts();
        AtomicReference<Object> outcome = new AtomicReference<>();
        Thread gatherer = new Thread(() -> {
            try {
                outcome.set(facts.gather(Set.of(R), Set.of()));
            } catch (Exception e) {
                outcome.set(e);
            }
        }, "facts-gatherer");

        gatherer.start();
        assertTrue(facts.offsetWaitEntered.await(10, TimeUnit.SECONDS),
                "the scripted round must reach the earliest-offset wait");
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (gatherer.getState() != Thread.State.TIMED_WAITING
                && gatherer.getState() != Thread.State.WAITING) {
            assertTrue(System.nanoTime() - deadline < 0,
                    "the gathering thread never blocked on the incomplete offset future; state "
                            + gatherer.getState());
            Thread.onSpinWait();
        }
        gatherer.interrupt();
        gatherer.join(TimeUnit.SECONDS.toMillis(10));
        assertFalse(gatherer.isAlive(), "the interrupted round must end, not keep waiting");
        assertInstanceOf(InterruptedException.class, outcome.get(),
                "an interrupt during the offset wait must abort the round interrupted, not"
                        + " complete it under a per-partition warning; got " + outcome.get());
    }

}
