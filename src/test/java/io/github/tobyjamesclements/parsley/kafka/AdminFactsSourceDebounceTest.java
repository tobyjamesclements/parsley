package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import io.github.tobyjamesclements.parsley.core.ChannelId;
import io.github.tobyjamesclements.parsley.core.PositionFacts;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Establishes that the dead-channel confirmation window is continuous.
 *
 * <p>A dead verdict lets held messages release past the channel, so it is the fail-open
 * direction this debounce guards. The window matures only over an unbroken run of
 * affirmative name-gone answers: a round in which the broker could not answer restarts it,
 * so two isolated name-gone observations spanning an outage never confirm death.
 */
class AdminFactsSourceDebounceTest {
    private static final UUID Z_ID = new UUID(3, 3);
    private static final ChannelId R = new ChannelId(Z_ID, 0);
    private static final long WINDOW_MILLIS = 1_000;

    /** Topic z is always undescribable by id; each round's by-name answer is scripted. */
    static final class ScriptedFacts extends AdminFactsSource {
        final AtomicLong nowMillis;
        /** This round's by-name answer for "z": a NameVerdict, a UUID, or null for no answer. */
        volatile Object nameAnswerThisRound;
        volatile boolean abortThisRound;
        volatile boolean failPositionsThisRound;

        private ScriptedFacts(AtomicLong nowMillis) {
            super(null, "g", Map.of(Z_ID, "z"), Map.of(), WINDOW_MILLIS, nowMillis::get);
            this.nowMillis = nowMillis;
        }

        ScriptedFacts() {
            this(new AtomicLong());
        }

        @Override
        Map<UUID, String> describeByIds(Set<UUID> topicIds) throws Exception {
            if (abortThisRound) {
                throw new org.apache.kafka.common.errors.TimeoutException("broker unreachable");
            }
            return Map.of();
        }

        @Override
        Map<String, Object> describeByNames(Set<String> names) {
            Map<String, Object> outcome = new HashMap<>();
            Object answer = nameAnswerThisRound;
            if (names.contains("z") && answer != null) {
                outcome.put("z", answer);
            }
            return outcome;
        }

        @Override
        Map<TopicPartition, KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>> earliestOffsetFutures(
                Map<TopicPartition, OffsetSpec> queries) {
            return Map.of();
        }

        @Override
        Map<TopicPartition, OffsetAndMetadata> committedOffsets() {
            if (failPositionsThisRound) {
                throw new org.apache.kafka.common.errors.TimeoutException("group coordinator unreachable");
            }
            return Map.of();
        }

        PositionFacts round(long atMillis, boolean nameGone) throws Exception {
            return round(atMillis, nameGone ? NameVerdict.NAME_GONE : null);
        }

        PositionFacts round(long atMillis, Object nameAnswer) throws Exception {
            nowMillis.set(atMillis);
            nameAnswerThisRound = nameAnswer;
            return gather(Set.of(R), Map.of(), Set.of());
        }

        void abortedRound(long atMillis) {
            nowMillis.set(atMillis);
            abortThisRound = true;
            try {
                gather(Set.of(R), Map.of(), Set.of());
                throw new AssertionError("the aborted round must propagate its failure");
            } catch (AssertionError e) {
                throw e;
            } catch (Exception expected) {
                // the outage: the round failed outright, observing nothing
            } finally {
                abortThisRound = false;
            }
        }

        /** A round whose name observations land but whose position queries then fail. */
        void lateAbortedRound(long atMillis, boolean nameGone) {
            nowMillis.set(atMillis);
            nameAnswerThisRound = nameGone ? NameVerdict.NAME_GONE : null;
            failPositionsThisRound = true;
            try {
                gather(Set.of(R), Map.of(), Set.of());
                throw new AssertionError("the late-aborted round must propagate its failure");
            } catch (AssertionError e) {
                throw e;
            } catch (Exception expected) {
                // the round aborted after its observations landed
            } finally {
                failPositionsThisRound = false;
            }
        }
    }

    @Test
    void isolatedNameGoneAnswersSpanningAnOutageDoNotConfirmDeath() throws Exception {
        ScriptedFacts facts = new ScriptedFacts();

        assertTrue(facts.round(0, true).deadChannels().isEmpty(), "first observation opens the window");
        assertTrue(facts.round(10_000, false).deadChannels().isEmpty(), "the outage breaks the window");
        assertTrue(facts.round(20_000, true).deadChannels().isEmpty(),
                "an isolated re-observation after the outage must reopen the window, not mature the old one");
    }

    /**
     * A real broker outage aborts rounds outright — thrown describes, not completed rounds
     * with unavailable answers — and must break the window's continuity just the same.
     */
    @Test
    void isolatedNameGoneAnswersSpanningAbortedRoundsDoNotConfirmDeath() throws Exception {
        ScriptedFacts facts = new ScriptedFacts();

        assertTrue(facts.round(0, true).deadChannels().isEmpty(), "first observation opens the window");
        for (long at = 200; at <= 9_800; at += 200) {
            facts.abortedRound(at);
        }
        assertTrue(facts.round(20_000, true).deadChannels().isEmpty(),
                "an isolated re-observation after aborted rounds must reopen the window, not mature the old one");
    }

    /**
     * The converse bound on the abort reset: a round whose name observations landed — and
     * were affirmative — before a later position query failed has not broken continuity.
     * Were such rounds to clear the windows, a recurring late-stage failure (a degraded
     * group coordinator with a healthy metadata plane) would keep a genuinely dead channel
     * unconfirmable forever.
     */
    @Test
    void lateStageAbortsDoNotBreakConfirmationContinuity() throws Exception {
        ScriptedFacts facts = new ScriptedFacts();

        assertTrue(facts.round(0, true).deadChannels().isEmpty(), "first observation opens the window");
        facts.lateAbortedRound(400, true);
        assertEquals(Set.of(R), facts.round(WINDOW_MILLIS + 200, true).deadChannels(),
                "an unbroken affirmative run must confirm death despite late-stage aborts between rounds");
    }

    @Test
    void anUnbrokenRunOfNameGoneAnswersDoesConfirmDeath() throws Exception {
        ScriptedFacts facts = new ScriptedFacts();

        assertTrue(facts.round(0, true).deadChannels().isEmpty());
        assertTrue(facts.round(WINDOW_MILLIS / 2, true).deadChannels().isEmpty(),
                "the window has not yet spanned its length");
        assertEquals(Set.of(R), facts.round(WINDOW_MILLIS, true).deadChannels(),
                "continuous name-gone observation across the window confirms death");
    }

    /**
     * Two affirmative sightings bracketing a blind period — no round asked about the id in
     * between, because the shared facts executor was starved or the asking task sat out a
     * rebalance — are isolated observations, not a continuous window: nothing observed the
     * name staying gone across the gap. A gap reaching the window length restarts the
     * window (D85); the tail of the test is the positive control that an unbroken run
     * after the restart still confirms.
     */
    @Test
    void aBlindGapBetweenNameGoneSightingsRestartsTheWindow() throws Exception {
        ScriptedFacts facts = new ScriptedFacts();

        assertTrue(facts.round(0, true).deadChannels().isEmpty(), "first observation opens the window");
        assertTrue(facts.round(4 * WINDOW_MILLIS, true).deadChannels().isEmpty(),
                "a sighting after a blind gap must reopen the window, not mature the stale anchor");
        assertTrue(facts.round(4 * WINDOW_MILLIS + WINDOW_MILLIS / 2, true).deadChannels().isEmpty(),
                "the reopened window has not yet spanned its length");
        assertEquals(Set.of(R), facts.round(5 * WINDOW_MILLIS, true).deadChannels(),
                "an unbroken run from the reopened window still confirms death");
    }

    /**
     * A recreated answer is served from one broker's metadata view, which can lag this
     * process's own resolution and serve the previous incarnation's binding — for a topic
     * deleted and recreated just before this process resolved the new identity, that stale
     * answer is indistinguishable from a genuine mid-run recreation. One such snapshot must
     * not convict a live received topic into a permanent identity-changed stop; conviction
     * takes the same continuously-corroborated window as death (D85).
     */
    @Test
    void recreationConvictsOnlyAcrossAContinuousWindow() throws Exception {
        ScriptedFacts facts = new ScriptedFacts();
        UUID newIncarnation = new UUID(9, 9);

        assertTrue(facts.round(0, newIncarnation).recreatedChannels().isEmpty(),
                "one recreated answer opens a window, never convicts alone");
        assertTrue(facts.round(WINDOW_MILLIS / 2, newIncarnation).recreatedChannels().isEmpty(),
                "the window has not yet spanned its length");
        assertEquals(Set.of(R), facts.round(WINDOW_MILLIS, newIncarnation).recreatedChannels(),
                "continuous recreated observation across the window convicts");
    }

    /**
     * The stale-broker shape itself: recreated answers interleaved with same-id answers
     * from healthy brokers. Each same-id answer is affirmative proof the recreated answer
     * was the stale one, so the window restarts and the verdict never matures.
     */
    @Test
    void staleRecreatedAnswersInterleavedWithSameIdNeverConvict() throws Exception {
        ScriptedFacts facts = new ScriptedFacts();
        UUID oldIncarnation = new UUID(9, 9);

        for (long at = 0; at <= 6 * WINDOW_MILLIS; at += WINDOW_MILLIS / 2) {
            Object answer = (at / (WINDOW_MILLIS / 2)) % 2 == 0 ? oldIncarnation : Z_ID;
            assertTrue(facts.round(at, answer).recreatedChannels().isEmpty(),
                    "a stale old binding interleaved with healthy same-id answers must never convict (at "
                            + at + "ms)");
        }
    }

    /**
     * The substrate never reuses a topic id, so a confirmed-dead id's name resolving to
     * that very id is affirmative proof the confirming answers were stale. The verdict is
     * rescinded rather than held against the evidence, and a genuinely dead topic still
     * reconfirms through a fresh window afterwards (D85).
     */
    @Test
    void aDeadVerdictIsRescindedWhenTheNameResolvesToTheSameId() throws Exception {
        ScriptedFacts facts = new ScriptedFacts();

        facts.round(0, true);
        facts.round(WINDOW_MILLIS / 2, true);
        assertEquals(Set.of(R), facts.round(WINDOW_MILLIS, true).deadChannels(),
                "the spurious confirmation the rescission must recover from");

        assertTrue(facts.round(WINDOW_MILLIS + 500, Z_ID).deadChannels().isEmpty(),
                "the name resolving to the condemned id rescinds the dead verdict");

        assertTrue(facts.round(2 * WINDOW_MILLIS, true).deadChannels().isEmpty(),
                "after rescission a fresh window opens rather than maturing stale state");
        facts.round(2 * WINDOW_MILLIS + WINDOW_MILLIS / 2, true);
        assertEquals(Set.of(R), facts.round(3 * WINDOW_MILLIS, true).deadChannels(),
                "a genuinely dead topic still reconfirms through an unbroken fresh window");
    }

    /**
     * A dead verdict upgrades to recreated when the name reappears under a different id —
     * but through the same corroborated window, not from one answer: the reappearance
     * answer is as stale-prone as the disappearance one.
     */
    @Test
    void aDeadVerdictUpgradesToRecreatedOnlyAcrossAContinuousWindow() throws Exception {
        ScriptedFacts facts = new ScriptedFacts();
        UUID newIncarnation = new UUID(9, 9);

        facts.round(0, true);
        facts.round(WINDOW_MILLIS / 2, true);
        assertEquals(Set.of(R), facts.round(WINDOW_MILLIS, true).deadChannels(), "death confirmed");

        PositionFacts first = facts.round(2 * WINDOW_MILLIS, newIncarnation);
        assertTrue(first.recreatedChannels().isEmpty(), "one reappearance answer opens a window, not a verdict");
        assertEquals(Set.of(R), first.deadChannels(), "the dead verdict stands while the upgrade window matures");
        facts.round(2 * WINDOW_MILLIS + WINDOW_MILLIS / 2, newIncarnation);
        PositionFacts matured = facts.round(3 * WINDOW_MILLIS, newIncarnation);
        assertEquals(Set.of(R), matured.recreatedChannels(),
                "continuous reappearance under a different id upgrades dead to recreated");
        assertTrue(matured.deadChannels().isEmpty(), "the upgraded verdict replaces death");
    }

    /**
     * An id whose name was never learned has nothing to corroborate against, and absence of
     * by-id evidence alone must never confirm death: a DENY-Describe ACL makes a live topic
     * describe unknown by id, and for a nameless id no by-name answer can reveal the denial.
     * A time-only verdict here would prune a live cause (SPEC Structural 13); the id stays
     * unconfirmed however long the by-id silence lasts.
     */
    @Test
    void anIdWithNoKnownNameIsNeverConfirmedDeadOnTimeAlone() throws Exception {
        AtomicLong nowMillis = new AtomicLong();
        AdminFactsSource facts = new AdminFactsSource(null, "g", Map.of(), Map.of(),
                WINDOW_MILLIS, nowMillis::get) {
            @Override
            Map<UUID, String> describeByIds(Set<UUID> topicIds) {
                return Map.of();
            }

            @Override
            Map<String, Object> describeByNames(Set<String> names) {
                return Map.of();
            }

            @Override
            Map<TopicPartition, KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>> earliestOffsetFutures(
                    Map<TopicPartition, OffsetSpec> queries) {
                return Map.of();
            }

            @Override
            Map<TopicPartition, OffsetAndMetadata> committedOffsets() {
                return Map.of();
            }
        };

        for (long at = 0; at <= 40 * WINDOW_MILLIS; at += WINDOW_MILLIS) {
            nowMillis.set(at);
            assertTrue(facts.gather(Set.of(), Map.of(), Set.of(R)).deadChannels().isEmpty(),
                    "a nameless id must never be confirmed dead by elapsed time alone (at " + at + "ms)");
        }
    }
}
