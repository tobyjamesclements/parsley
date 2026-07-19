package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link ParsleyMessage}'s build/encode/decode helpers — the boundary that turns Kafka records
 * and headers into the typed engine envelope and back.
 */
class ParsleyMessageTest {

    private static final TopicPartition T1 = new TopicPartition("t1", 3);
    private static final Uuid T1_ID = Uuid.randomUuid();
    private static final Uuid DEP_ID = Uuid.randomUuid();

    /**
     * {@code from} decodes the {@code parsley-causal-dependencies} header into the typed clock and
     * keeps the user headers, dropping the dependency header from the user set.
     *
     * Asserts the coordinate, dependencies, and user headers are populated as expected.
     */
    @Test
    void fromDecodesDependenciesAndKeepsUserHeaders() {
        ParsleyVectorClock deps = ParsleyVectorClock.empty().observe(DEP_ID, 0, 5);
        Headers headers = ParsleyHeader.mutableHeaders();
        headers.add("user", "u".getBytes());
        headers.add(ParsleyHeader.CAUSAL_DEPENDENCIES, deps.toBytes());

        ParsleyMessage<String, String> message =
                ParsleyMessage.from(new Record<>("k", "v", 42L, headers), T1, 7L, T1_ID);

        assertEquals("t1", message.topic(), "topic must come from the source coordinate");
        assertEquals(T1_ID, message.topicId(), "topicId must come from the resolved UUID");
        assertEquals(3, message.partition(), "partition must come from the source coordinate");
        assertEquals(7L, message.offset(), "offset must come from the source coordinate");
        assertEquals(deps, message.dependencies(), "dependencies must be decoded from the header");
        assertEquals(List.of(new ParsleyHeader("user", "u".getBytes())).size(), message.headers().size(),
                "only the user header is carried");
        assertEquals("user", message.headers().get(0).key(), "the user header is preserved");
    }

    /**
     * {@code from} treats an absent dependency header as empty (vacuously satisfied) dependencies.
     *
     * Asserts the decoded dependencies are empty.
     */
    @Test
    void fromTreatsAbsentDependenciesAsEmpty() {
        ParsleyMessage<String, String> message =
                ParsleyMessage.from(new Record<>("k", "v", 0L, ParsleyHeader.mutableHeaders()), T1, 0L, T1_ID);
        assertTrue(message.dependencies().isEmpty(), "absent dependencies must decode as empty");
    }

    /**
     * {@code from} throws {@link ParsleyVectorClockResolutionException} when the dependency header is
     * present but cannot be decoded — the caller applies
     * {@code parsley.clock.resolution.failure.policy} rather than {@code from} silently degrading.
     *
     * Asserts the exception is thrown and carries the source coordinate.
     */
    @Test
    void fromThrowsOnUndecodableDependencies() {
        Headers headers = ParsleyHeader.mutableHeaders();
        headers.add(ParsleyHeader.CAUSAL_DEPENDENCIES, new byte[]{9, 9, 9});

        ParsleyVectorClockResolutionException thrown = assertThrows(ParsleyVectorClockResolutionException.class,
                () -> ParsleyMessage.from(new Record<>("k", "v", 0L, headers), T1, 0L, T1_ID),
                "an undecodable dependencies header must throw rather than decode as empty");
        assertEquals("t1", thrown.topic(), "the exception must carry the source topic");
        assertEquals(3, thrown.partition(), "the exception must carry the source partition");
        assertEquals(0L, thrown.offset(), "the exception must carry the source offset");
    }

    /**
     * The explicit-dependencies {@code from} overload builds with the caller-supplied clock and
     * skips header decoding, while still carrying the user headers and dropping the dependency
     * header — the path the {@code continue} policy uses to forward an unresolvable record with
     * empty dependencies.
     *
     * Asserts the supplied dependencies are used and the user header is kept.
     */
    @Test
    void fromWithExplicitDependenciesSkipsDecoding() {
        Headers headers = ParsleyHeader.mutableHeaders();
        headers.add("user", "u".getBytes());
        headers.add(ParsleyHeader.CAUSAL_DEPENDENCIES, new byte[]{9, 9, 9});

        ParsleyMessage<String, String> message = ParsleyMessage.from(
                new Record<>("k", "v", 0L, headers), T1, 0L, T1_ID, ParsleyVectorClock.empty());

        assertTrue(message.dependencies().isEmpty(), "the supplied empty dependencies must be used");
        assertEquals(1, message.headers().size(), "only the user header is carried");
        assertEquals("user", message.headers().get(0).key(), "the user header is preserved");
    }
}
