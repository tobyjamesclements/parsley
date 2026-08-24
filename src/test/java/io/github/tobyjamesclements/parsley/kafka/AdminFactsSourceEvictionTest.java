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
 * Establishes that verdict-tracking state is evicted by time, and only by time.
 *
 * <p>An id no task has asked about for the eviction horizon — eight confirmation windows,
 * floored at five minutes (D44) — has its verdicts and learned name forgotten, so a
 * confirmed-dead id that later reappears in the ask set is re-evaluated through a fresh
 * corroborated window (D75/D85) rather than condemned on sight from evidence nobody has
 * corroborated for the whole quiet spell, and the tracking maps for departed frontier ids
 * cannot grow without bound. Declared ids are pinned outside the horizon: their name
 * bindings are the only evidence that can classify a later recreation while a task sits
 * out a long rebalance (D44), so their verdicts and names survive it.
 *
 * <p>The sweep runs inside each round after that round's own ask set is stamped, so an id
 * asked in the current round is never evicted; the tests therefore drive later rounds
 * through an unrelated sweeper channel and observe forgetting only through {@code gather}
 * results.
 */
class AdminFactsSourceEvictionTest {
    /** Declared in the constructor's known-names map, and therefore pinned. */
    private static final UUID Z_ID = new UUID(3, 3);
    /** Frontier-only ids: their names are learned from describes, never pinned. */
    private static final UUID DEAD_ID = new UUID(1, 1);
    private static final UUID LEARNED_ID = new UUID(2, 2);
    private static final UUID SWEEPER_ID = new UUID(7, 7);
    private static final ChannelId R = new ChannelId(Z_ID, 0);
    private static final ChannelId D = new ChannelId(DEAD_ID, 0);
    private static final ChannelId N = new ChannelId(LEARNED_ID, 0);
    private static final ChannelId SWEEPER = new ChannelId(SWEEPER_ID, 0);
    private static final long WINDOW_MILLIS = 1_000;
    /** max(8 × WINDOW_MILLIS, 5 minutes): at this window length the five-minute floor dominates. */
    private static final long EVICTION_MILLIS = 300_000;

    /** Topics resolve by id and by name exactly as scripted for the round; everything else is silent. */
    static final class ScriptedFacts extends AdminFactsSource {
        final AtomicLong nowMillis;
        /** Ids the broker currently resolves by id, with their names. */
        final Map<UUID, String> liveById = new HashMap<>();
        /** This round's by-name answers: a NameVerdict or a resolved UUID per name. */
        final Map<String, Object> nameAnswers = new HashMap<>();

        ScriptedFacts() {
            this(new AtomicLong());
        }

        private ScriptedFacts(AtomicLong nowMillis) {
            super(null, "g", Map.of(Z_ID, "z"), Map.of(), WINDOW_MILLIS, nowMillis::get);
            this.nowMillis = nowMillis;
        }

        @Override
        Map<UUID, String> describeByIds(Set<UUID> topicIds) {
            Map<UUID, String> names = new HashMap<>();
            for (UUID id : topicIds) {
                String name = liveById.get(id);
                if (name != null) {
                    names.put(id, name);
                    recordLearnedName(id, name);
                }
            }
            return names;
        }

        @Override
        Map<String, Object> describeByNames(Set<String> names) {
            Map<String, Object> outcome = new HashMap<>();
            for (String name : names) {
                Object answer = nameAnswers.get(name);
                if (answer != null) {
                    outcome.put(name, answer);
                }
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
            return Map.of();
        }

        /** A round asking about {@code channel} alone, with whatever answers are scripted. */
        PositionFacts round(long atMillis, ChannelId channel) throws Exception {
            nowMillis.set(atMillis);
            return gather(Set.of(channel), Map.of(), Set.of());
        }

        /** A round in which {@code channel}'s topic resolves by id, teaching the source its name. */
        PositionFacts liveRound(long atMillis, ChannelId channel, String name) throws Exception {
            liveById.put(channel.topicId(), name);
            try {
                return round(atMillis, channel);
            } finally {
                liveById.remove(channel.topicId());
            }
        }

        /** A round in which {@code channel}'s topic is undescribable by id and its name answers gone. */
        PositionFacts nameGoneRound(long atMillis, ChannelId channel, String name) throws Exception {
            nameAnswers.put(name, NameVerdict.NAME_GONE);
            try {
                return round(atMillis, channel);
            } finally {
                nameAnswers.remove(name);
            }
        }

        /** A round asking only about an unrelated channel, so the sweep runs while others go unasked. */
        void sweepRound(long atMillis) throws Exception {
            round(atMillis, SWEEPER);
        }
    }

    /** Learns the topic's name, then runs the continuous name-gone window that confirms death (D75/D85). */
    private static void confirmDead(ScriptedFacts facts, ChannelId channel, String name, long fromMillis)
            throws Exception {
        facts.liveRound(fromMillis, channel, name);
        facts.nameGoneRound(fromMillis + WINDOW_MILLIS, channel, name);
        facts.nameGoneRound(fromMillis + WINDOW_MILLIS + WINDOW_MILLIS / 2, channel, name);
        assertEquals(Set.of(channel),
                facts.nameGoneRound(fromMillis + 2 * WINDOW_MILLIS, channel, name).deadChannels(),
                "precondition: an unbroken name-gone run across the window confirms death (D75/D85)");
    }

    /**
     * The leak direction the horizon exists to close (D44): a departed frontier id's dead
     * verdict must not outlive the eviction horizon. Regression caught: a sweep that never
     * runs, or that clears the ask timestamp without the verdict, keeps a confirmed-dead
     * id dead forever — its channel would be condemned the moment it reappears, on
     * evidence nobody corroborated across the whole quiet spell, and the tracking maps
     * would grow without bound.
     */
    @Test
    void anUnaskedDeadVerdictIsForgottenPastTheHorizonAndMustBeReEarned() throws Exception {
        ScriptedFacts facts = new ScriptedFacts();
        confirmDead(facts, D, "d", 0);

        // D was last asked at 2_000ms; this round asks only the sweeper, one second past the horizon.
        facts.sweepRound(2_000 + EVICTION_MILLIS + 1_000);

        long back = 2_000 + EVICTION_MILLIS + 2_000;
        assertTrue(facts.nameGoneRound(back, D, "d").deadChannels().isEmpty(),
                "an id evicted past the horizon must not be reported dead the moment it reappears");
        // The evicted name binding must be re-learned before death can be corroborated again.
        facts.liveRound(back + 100, D, "d");
        facts.nameGoneRound(back + 100 + WINDOW_MILLIS, D, "d");
        facts.nameGoneRound(back + 100 + WINDOW_MILLIS + WINDOW_MILLIS / 2, D, "d");
        assertEquals(Set.of(D),
                facts.nameGoneRound(back + 100 + 2 * WINDOW_MILLIS, D, "d").deadChannels(),
                "after eviction death is re-earned through a fresh continuous window, not remembered");
    }

    /**
     * Eviction forgets the learned name binding itself, not only verdicts. Regression
     * caught: a sweep that keeps stale bindings lets an id nobody asked about for the
     * whole horizon be condemned through a name learned in another era — the binding is
     * exactly the evidence the horizon declares stale, and without corroboration against
     * a name there must be no death verdict at all (D75).
     */
    @Test
    void anEvictedIdsLearnedNameCannotCorroborateDeathUntilRelearned() throws Exception {
        ScriptedFacts facts = new ScriptedFacts();
        facts.liveRound(0, N, "n");

        // N was last asked at 0ms; this round asks only the sweeper, one second past the horizon.
        facts.sweepRound(EVICTION_MILLIS + 1_000);

        long back = EVICTION_MILLIS + 2_000;
        assertTrue(facts.nameGoneRound(back, N, "n").deadChannels().isEmpty(),
                "the first name-gone answer after eviction must confirm nothing");
        assertTrue(facts.nameGoneRound(back + WINDOW_MILLIS / 2, N, "n").deadChannels().isEmpty(),
                "the reopened run has not spanned a window, and the evicted binding must not shortcut it");
        assertTrue(facts.nameGoneRound(back + WINDOW_MILLIS, N, "n").deadChannels().isEmpty(),
                "a continuous name-gone run must not confirm death: the learned name was evicted with"
                        + " the id, so there is nothing to corroborate against until it is re-learned");

        facts.liveRound(back + WINDOW_MILLIS + 100, N, "n");
        facts.nameGoneRound(back + 2 * WINDOW_MILLIS + 100, N, "n");
        facts.nameGoneRound(back + 2 * WINDOW_MILLIS + 100 + WINDOW_MILLIS / 2, N, "n");
        assertEquals(Set.of(N),
                facts.nameGoneRound(back + 3 * WINDOW_MILLIS + 100, N, "n").deadChannels(),
                "once the name is re-learned an unbroken run confirms again: eviction is amnesia, not immunity");
    }

    /**
     * Declared ids are pinned outside the horizon (D44): their name bindings are the only
     * evidence that can classify a later recreation while a task sits out a long
     * rebalance. Regression caught: a sweep that ignores the pin evicts a declared
     * topic's dead verdict during a quiet spell, and the task that finally asks again is
     * told the channel is unconfirmed instead of dead.
     */
    @Test
    void aPinnedIdKeepsItsVerdictAcrossTheHorizon() throws Exception {
        ScriptedFacts facts = new ScriptedFacts();
        confirmDead(facts, R, "z", 0);

        // Identical treatment to the frontier id: unasked for a second past the horizon.
        facts.sweepRound(2_000 + EVICTION_MILLIS + 1_000);

        assertEquals(Set.of(R), facts.nameGoneRound(2_000 + EVICTION_MILLIS + 2_000, R, "z").deadChannels(),
                "a declared (pinned) id keeps its dead verdict however long no task asks about it");
    }

    /**
     * The acceptance shape behind the horizon: a non-pinned confirmed-dead id is excluded
     * from by-id describes and, its name forgotten at confirmation, has no by-name recheck
     * — the verdict is sticky by construction, and eviction is its only exit. Regression
     * caught: without the sweep clearing the verdict, a topic reappearing after a long
     * quiet spell stays condemned on sight instead of being re-evaluated as live.
     */
    @Test
    void evictionLetsAReappearingTopicBeReEvaluatedInsteadOfStayingDead() throws Exception {
        ScriptedFacts facts = new ScriptedFacts();
        confirmDead(facts, D, "d", 0);

        assertEquals(Set.of(D), facts.liveRound(2_100, D, "d").deadChannels(),
                "inside the horizon the verdict is sticky: a confirmed-dead frontier id is not re-described");

        facts.sweepRound(2_100 + EVICTION_MILLIS + 1_000);

        PositionFacts reappeared = facts.liveRound(2_100 + EVICTION_MILLIS + 2_000, D, "d");
        assertTrue(reappeared.deadChannels().isEmpty(),
                "past the horizon the reappearing topic must be re-evaluated: it describes live, not dead");
        assertTrue(reappeared.recreatedChannels().isEmpty(),
                "re-evaluation reports the reappeared topic clean, carrying no stale verdict");
    }

    /**
     * The converse bound, pinning the five-minute floor: with the window at one second,
     * eight windows alone would be a mere eight seconds. Regression caught: a formula
     * weakened to 8 × window — or any horizon under the floor — evicts a verdict during a
     * routine quiet spell no longer than a slow rebalance, and a genuinely dead channel
     * pops back unconfirmed.
     */
    @Test
    void aVerdictUnaskedForLessThanTheHorizonIsRetained() throws Exception {
        ScriptedFacts facts = new ScriptedFacts();
        confirmDead(facts, D, "d", 0);

        // D was last asked at 2_000ms: unasked for 299 seconds here, one second inside the horizon.
        facts.sweepRound(2_000 + EVICTION_MILLIS - 1_000);

        assertEquals(Set.of(D), facts.nameGoneRound(2_000 + EVICTION_MILLIS - 500, D, "d").deadChannels(),
                "an id unasked for less than the horizon keeps its dead verdict: the floor holds the"
                        + " horizon at five minutes even when eight windows would be shorter");
    }
}
