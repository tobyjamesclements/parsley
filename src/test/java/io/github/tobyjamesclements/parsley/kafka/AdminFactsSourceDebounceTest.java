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
        volatile boolean nameGoneThisRound;

        private ScriptedFacts(AtomicLong nowMillis) {
            super(null, "g", Map.of(Z_ID, "z"), Map.of(), WINDOW_MILLIS, nowMillis::get);
            this.nowMillis = nowMillis;
        }

        ScriptedFacts() {
            this(new AtomicLong());
        }

        @Override
        Map<UUID, String> describeByIds(Set<UUID> topicIds) {
            return Map.of();
        }

        @Override
        Map<String, Object> describeByNames(Set<String> names) {
            Map<String, Object> outcome = new HashMap<>();
            if (names.contains("z") && nameGoneThisRound) {
                outcome.put("z", NameVerdict.NAME_GONE);
            }
            return outcome;
        }

        @Override
        Map<TopicPartition, KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>> earliestOffsets(
                Map<TopicPartition, OffsetSpec> queries) {
            return Map.of();
        }

        @Override
        Map<TopicPartition, OffsetAndMetadata> committedOffsets() {
            return Map.of();
        }

        PositionFacts round(long atMillis, boolean nameGone) throws Exception {
            nowMillis.set(atMillis);
            nameGoneThisRound = nameGone;
            return gather(Set.of(R), Map.of(), Set.of());
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

    @Test
    void anUnbrokenRunOfNameGoneAnswersDoesConfirmDeath() throws Exception {
        ScriptedFacts facts = new ScriptedFacts();

        assertTrue(facts.round(0, true).deadChannels().isEmpty());
        assertEquals(Set.of(R), facts.round(WINDOW_MILLIS + 1_000, true).deadChannels(),
                "continuous name-gone observation across the window confirms death");
    }
}
