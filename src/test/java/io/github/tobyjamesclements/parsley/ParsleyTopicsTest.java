package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link ParsleyTopics} resolution and caching against hand-rolled {@link ParsleyTopicAdmin}
 * doubles (the project uses no mocking framework). The live {@code Properties}-backed path is covered
 * end-to-end by the integration tests, since it resolves through a real Kafka admin client.
 */
class ParsleyTopicsTest {

    private static final Uuid T1_ID = Uuid.randomUuid();

    /** A {@link ParsleyTopicAdmin} whose {@code topicIds} is supplied as a lambda; the rest are stubs. */
    @FunctionalInterface
    private interface TopicIds {
        Map<String, Uuid> apply(List<String> topics) throws Exception;
    }

    private static ParsleyTopicAdmin admin(TopicIds topicIds) {
        return new ParsleyTopicAdmin() {
            @Override
            public Map<String, Uuid> topicIds(List<String> topics) throws Exception {
                return topicIds.apply(topics);
            }

            @Override
            public Map<String, Integer> partitionCounts(List<String> topics) {
                return Map.of();
            }


            @Override
            public Map<String, String> cleanupPolicies(List<String> topics) {
                return Map.of();
            }

            @Override
            public void close() {
            }
        };
    }

    /**
     * A resolver resolves a known topic name to the UUID its admin reports.
     *
     * Asserts the resolved UUID matches the admin's.
     */
    @Test
    void resolvesTopicIdThroughTheAdmin() {
        ParsleyTopics topics = new KafkaTopics(admin(t -> Map.of("t1", T1_ID)));
        assertEquals(T1_ID, topics.topicId("t1"), "the resolver must return the admin's UUID for the topic");
    }

    /**
     * A resolved UUID is cached: resolving the same topic twice queries the admin only once, since
     * topic UUIDs are stable for a topic's lifetime.
     *
     * Asserts the admin is queried exactly once across two lookups.
     */
    @Test
    void cachesResolvedTopicIds() {
        AtomicInteger queries = new AtomicInteger();
        ParsleyTopics topics = new KafkaTopics(admin(t -> {
            queries.incrementAndGet();
            return Map.of("t1", T1_ID);
        }));

        assertEquals(T1_ID, topics.topicId("t1"), "first lookup must resolve");
        assertEquals(T1_ID, topics.topicId("t1"), "second lookup must resolve to the same UUID");
        assertEquals(1, queries.get(), "a cached UUID must not re-query the admin");
    }

    /**
     * A topic the broker does not know (the admin surfaces an
     * {@link UnknownTopicOrPartitionException}) is reported as an {@code IllegalArgumentException} —
     * a caller error, not an infrastructure fault.
     *
     * Asserts an {@code IllegalArgumentException} naming the topic is thrown.
     */
    @Test
    void unknownTopicFromTheBrokerBecomesIllegalArgument() {
        ParsleyTopics topics = new KafkaTopics(admin(t -> {
            throw new ExecutionException(new UnknownTopicOrPartitionException("nope"));
        }));

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> topics.topicId("nope"), "an unknown topic must be an IllegalArgumentException");
        assertTrue(ex.getMessage().contains("nope"), "the message must name the topic; got: " + ex.getMessage());
    }

    /**
     * A genuine broker failure (an {@link ExecutionException} not caused by an unknown topic) is
     * reported as an {@code IllegalStateException} — distinct from the caller-error case.
     *
     * Asserts an {@code IllegalStateException} is thrown.
     */
    @Test
    void brokerFailureBecomesIllegalState() {
        ParsleyTopics topics = new KafkaTopics(admin(t -> {
            throw new ExecutionException(new RuntimeException("broker down"));
        }));

        assertThrows(IllegalStateException.class, () -> topics.topicId("t1"),
                "a broker failure must be an IllegalStateException, not an IllegalArgumentException");
    }

    /**
     * An interrupt during resolution is reported as an {@code IllegalStateException} and re-raises the
     * thread's interrupt flag rather than swallowing it.
     *
     * Asserts an {@code IllegalStateException} is thrown and the interrupt flag is set.
     */
    @Test
    void interruptedResolutionBecomesIllegalStateAndPreservesTheInterruptFlag() {
        ParsleyTopics topics = new KafkaTopics(admin(t -> {
            throw new InterruptedException();
        }));

        assertThrows(IllegalStateException.class, () -> topics.topicId("t1"),
                "an interrupt during resolution must be an IllegalStateException");
        assertTrue(Thread.interrupted(), "the interrupt flag must be preserved (and is cleared here)");
    }

    /**
     * An admin that returns a result omitting the requested topic (rather than throwing) still yields
     * an {@code IllegalArgumentException} for the missing topic.
     *
     * Asserts an {@code IllegalArgumentException} is thrown.
     */
    @Test
    void anEmptyResolutionResultBecomesIllegalArgument() {
        ParsleyTopics topics = new KafkaTopics(admin(t -> Map.of()));
        assertThrows(IllegalArgumentException.class, () -> topics.topicId("t1"),
                "a topic missing from the admin's result must be an IllegalArgumentException");
    }

    /**
     * The map-backed resolver resolves known topics and rejects unknown ones with an
     * {@code IllegalArgumentException}.
     *
     * Asserts a known topic resolves and an unknown one throws.
     */
    @Test
    void mapBackedResolverResolvesKnownTopicsAndRejectsUnknownOnes() {
        ParsleyTopics topics = ParsleyTopics.of(Map.of("t1", T1_ID));
        assertEquals(T1_ID, topics.topicId("t1"), "a known topic must resolve from the map");
        assertThrows(IllegalArgumentException.class, () -> topics.topicId("t2"),
                "an unknown topic must be rejected");
    }

    /**
     * The factory methods and resolution reject {@code null} arguments.
     *
     * Asserts {@code NullPointerException} for each null entry point.
     */
    @Test
    void nullArgumentsAreRejected() {
        assertThrows(NullPointerException.class, () -> ParsleyTopics.of((Properties) null),
                "of(Properties) must reject null props");
        assertThrows(NullPointerException.class, () -> ParsleyTopics.of((Map<String, Uuid>) null),
                "of(Map) must reject a null map");
        assertThrows(NullPointerException.class, () -> ParsleyTopics.of(Map.of("t1", T1_ID)).topicId(null),
                "topicId must reject a null topic");
    }
}
