package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the corroboration behind declared-topic resolution (D113). A describe is served
 * from one broker's metadata view, which can lag a topic created moments before the
 * start; a single stale unknown-topic answer used to refuse the start for a topic that
 * existed. Absence is now concluded from three consistent unknown answers, and every
 * other failure still refuses at once.
 */
@Timeout(value = 10, unit = TimeUnit.SECONDS)
class DeclaredTopicResolutionTest {
    private static final Duration NO_BACKOFF = Duration.ZERO;

    private static TopicDescription description(String name) {
        Node node = new Node(0, "localhost", 9092);
        return new TopicDescription(name, false,
                List.of(new TopicPartitionInfo(0, node, List.of(node), List.of(node))), Set.of(), Uuid.randomUuid());
    }

    private static ExecutionException unknown(String name) {
        return new ExecutionException(new UnknownTopicOrPartitionException(name + ": unknown"));
    }

    /**
     * A topic a lagging broker reports unknown twice and then describes resolves: the
     * start is not refused on a single stale answer, and the resolved identity is the one
     * the corroborating answer carried.
     */
    @Test
    void aLaggingUnknownAnswerIsRetriedAndTheTopicResolvesOnceDescribed() {
        AtomicInteger describes = new AtomicInteger();
        TopicDescription real = description("orders");
        Map<String, TopicInfo> topics = ParsleyRuntime.resolveTopicsCorroborated(() -> {
            if (describes.incrementAndGet() < 3) {
                throw unknown("orders");
            }
            return Map.of("orders", real);
        }, NO_BACKOFF);
        assertEquals(3, describes.get(), "two stale answers are retried, the third resolves");
        assertEquals(new java.util.UUID(real.topicId().getMostSignificantBits(), real.topicId().getLeastSignificantBits()),
                topics.get("orders").topicId(), "the resolved identity is the corroborating answer's");
    }

    /**
     * Three consistent unknown answers refuse the start with the resolution diagnosis and
     * the unknown-topic cause attached, and no fourth describe is paid: a topic that is
     * genuinely missing is refused as before, one second later.
     */
    @Test
    void threeConsistentUnknownAnswersRefuseNamingTheMissingTopic() {
        AtomicInteger describes = new AtomicInteger();
        IllegalStateException refusal = assertThrows(IllegalStateException.class,
                () -> ParsleyRuntime.resolveTopicsCorroborated(() -> {
                    describes.incrementAndGet();
                    throw unknown("orders");
                }, NO_BACKOFF),
                "a topic unknown on three corroborated answers must refuse the start");
        assertEquals(3, describes.get(), "absence is concluded from exactly three answers");
        assertTrue(refusal.getMessage().contains("declared topics could not be resolved; refusing to start"),
                refusal.getMessage());
        assertTrue(refusal.getCause().getCause() instanceof UnknownTopicOrPartitionException,
                "the unknown-topic cause stays attached: " + refusal.getCause());
    }

    /**
     * Any failure other than an unknown topic refuses at once: a timeout or a broker
     * outage is not a matter of corroboration, and retrying it would only delay the
     * diagnosis.
     */
    @Test
    void aFailureThatIsNotAnUnknownTopicRefusesAtOnce() {
        AtomicInteger describes = new AtomicInteger();
        RuntimeException broken = new RuntimeException("broker unreachable");
        IllegalStateException refusal = assertThrows(IllegalStateException.class,
                () -> ParsleyRuntime.resolveTopicsCorroborated(() -> {
                    describes.incrementAndGet();
                    throw broken;
                }, NO_BACKOFF),
                "a generic describe failure refuses the start");
        assertEquals(1, describes.get(), "nothing is corroborated for a failure that is not an unknown topic");
        assertSame(broken, refusal.getCause(), "the failure stays attached as the cause");
    }

    /**
     * A refusal from the identity floor passes through untouched: the reserved zero topic
     * id is a broker below the floor, not a lagging answer, and it must keep its own
     * diagnosis rather than be retried or rewrapped.
     */
    @Test
    void aTopicIdentityRefusalPassesThroughUnwrapped() {
        Node node = new Node(0, "localhost", 9092);
        TopicDescription zeroId = new TopicDescription("orders", false,
                List.of(new TopicPartitionInfo(0, node, List.of(node), List.of(node))), Set.of(), Uuid.ZERO_UUID);
        AtomicInteger describes = new AtomicInteger();
        assertThrows(io.github.tobyjamesclements.parsley.core.ParsleyFailClosedException.class,
                () -> ParsleyRuntime.resolveTopicsCorroborated(() -> {
                    describes.incrementAndGet();
                    return Map.of("orders", zeroId);
                }, NO_BACKOFF),
                "the identity floor's refusal is the runtime's own, not a resolution failure");
        assertEquals(1, describes.get(), "a refusal is never retried");
    }
}
