package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import io.github.tobyjamesclements.parsley.core.ChannelId;
import io.github.tobyjamesclements.parsley.core.PositionFacts;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Establishes that a round's own tail is watched time (D107). D88 judges a confirmation
 * window's continuity on the blind time between rounds and says in-round latency is
 * watched time; before D107 the reference stamp was taken at classification, before the
 * earliest-offset wait and the confirming describe, so everything after it was charged to
 * the next round as blind time, and a round whose tail reached the window length restarted
 * its windows every time and never confirmed a deleted topic. The stamp now lands when the
 * round ends. The tail here is the confirming describe; with D114 it and the offset wait
 * are the whole of a background round's tail.
 */
class FactsRoundTailContinuityTest {
    private static final long WINDOW_MILLIS = 3_000; // the window's floor, whatever the facts interval

    /** Topic z is gone by name; each round's post-sample tail takes {@code tailMillis} of clock. */
    static final class TailedRoundFacts extends AdminFactsSource {
        static final UUID Z_ID = new UUID(3, 3);
        static final ChannelId R = new ChannelId(Z_ID, 0);
        final AtomicLong clock;
        final long tailMillis;

        TailedRoundFacts(AtomicLong clock, String group, long tailMillis) {
            super(null, group, Map.of(Z_ID, "z"), WINDOW_MILLIS, clock::get);
            this.clock = clock;
            this.tailMillis = tailMillis;
        }

        @Override
        Map<UUID, String> describeByIds(Set<UUID> topicIds) {
            return Map.of();
        }

        @Override
        Map<String, Object> describeByNames(Set<String> names) {
            Map<String, Object> outcome = new HashMap<>();
            if (names.contains("z")) {
                outcome.put("z", NameVerdict.NAME_GONE);
            }
            return outcome;
        }

        @Override
        Map<TopicPartition, KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>> earliestOffsetFutures(
                Map<TopicPartition, OffsetSpec> queries) {
            return Map.of();
        }

        /** Always called, after the round sampled {@code now}: stands for the offset wait and the confirming describe. */
        @Override
        Map<UUID, String> confirmIdentities(Set<UUID> topicIds) {
            clock.addAndGet(tailMillis);
            return Map.of();
        }

        PositionFacts round() throws Exception {
            return gather(Set.of(R), Set.of());
        }
    }

    /** A 2s tail confirms on the third back-to-back round. */
    @Test
    void aTwoSecondTailConfirmsOnTheThirdBackToBackRound() throws Exception {
        TailedRoundFacts a = new TailedRoundFacts(new AtomicLong(), "a", 2_000);
        assertTrue(a.round().deadChannels().isEmpty(), "first observation opens the window");
        assertTrue(a.round().deadChannels().isEmpty(), "second observation at 2s of the 3s window");
        assertEquals(Set.of(TailedRoundFacts.R), a.round().deadChannels(),
                "third observation at 4s spans the window and confirms");
    }

    /**
     * A 3s tail after the classification-time sample equals the whole window. Before D107
     * every round saw a blind gap of a full window and restarted it, and ten minutes of
     * unbroken name-gone answers produced no verdict; the round-end stamp makes the tail
     * watched time, so the second round matures the window.
     */
    @Test
    void aThreeSecondTailStillConfirmsADeadTopicForALoneProcess() throws Exception {
        TailedRoundFacts a = new TailedRoundFacts(new AtomicLong(), "a", 3_000);
        assertTrue(a.round().deadChannels().isEmpty(), "first observation opens the window");
        assertEquals(Set.of(TailedRoundFacts.R), a.round().deadChannels(),
                "the second observation, one watched round later, spans the window and confirms");
    }

    /**
     * Three processes in one runtime, each with a 2s tail, rounds interleaved round-robin
     * on the single facts thread: a source's blind time is the two sibling rounds between
     * its own, 4s, which exceeds the window — so sibling rounds still restart it, and the
     * verdict needs the coalesced round D107 records as the follow-up. Pinned so the
     * remaining cadence limit is stated by a test rather than assumed away.
     */
    @Test
    void siblingRoundsLongerThanTheWindowStillRestartIt() throws Exception {
        AtomicLong clock = new AtomicLong();
        List<TailedRoundFacts> sources = List.of(
                new TailedRoundFacts(clock, "a", 2_000),
                new TailedRoundFacts(clock, "b", 2_000),
                new TailedRoundFacts(clock, "c", 2_000));
        for (int sweep = 0; sweep < 5; sweep++) {
            for (TailedRoundFacts source : sources) {
                assertTrue(source.round().deadChannels().isEmpty(),
                        "sweep " + sweep + ": two sibling rounds of 2s each are a 4s blind gap for the third");
            }
        }
    }
}
