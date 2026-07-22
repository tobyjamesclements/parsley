package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@link CausalBufferLimit#unbounded()} never evicts regardless of how many records accumulate or
 * how long they wait. Records are held until their causal dependencies are satisfied and then
 * released in causal order.
 */
class ParsleyBufferUnboundedLimitTest {

    private static final TopicPartition C1 = new TopicPartition("c1", 0);
    private static final TopicPartition C2 = new TopicPartition("c2", 0);
    private static final Uuid C1_ID = Uuid.randomUuid();
    private static final Uuid C2_ID = Uuid.randomUuid();

    /**
     * Records buffered under an unbounded limit are never evicted, regardless of depth. Once the
     * blocking dependency is satisfied the buffer drains to zero in causal order.
     *
     * <p>Five C2 records each depend on C1/0 having been observed at offset 4; C1 has not been
     * seen yet. All five buffer without any eviction. When C1@0–@4 arrive and advance the C1
     * frontier to 4, the dependency is satisfied and all five C2 records are released in order.
     */
    @Test
    void unboundedLimitNeverEvictsAndReleasesInCausalOrderWhenDependenciesSatisfied() {
        MockBufferStore<String, String> buffer = new MockBufferStore<>();
        ParsleyCausalBroadcast<String, String> causalBroadcast = ParsleyTestFixtures.broadcast(
                ParsleyVectorClock.empty(),
                buffer, new MockCandidateIndex(), new MockForwardedIndex(), ParsleyMetrics.NOOP,
                System::currentTimeMillis);

        List<ParsleyMessage<String, String>> out = new ArrayList<>();
        ParsleyVectorClock needsC1 = ParsleyVectorClock.empty().observe(C1_ID, 0, 4);
        for (long offset = 0; offset < 5; offset++) {
            out.addAll(causalBroadcast.receive(message(C2, offset, C2_ID, needsC1)).delivered());
        }

        assertEquals(5, buffer.size(), "all five records must be held — no eviction must have occurred");
        assertEquals(List.of(), out, "no record must be forwarded before the dependency is satisfied");

        for (long offset = 0; offset < 5; offset++) {
            out.addAll(causalBroadcast.receive(message(C1, offset, C1_ID, ParsleyVectorClock.empty())).delivered());
        }

        assertEquals(0, buffer.size(), "buffer must be empty after the dependency is satisfied");
        assertEquals(
                List.of(0L, 1L, 2L, 3L, 4L, 0L, 1L, 2L, 3L, 4L),
                out.stream().map(ParsleyMessage::offset).toList(),
                "C1@0–@4 must be forwarded first, then C2@0–@4 released in causal order");
    }

    private static ParsleyMessage<String, String> message(TopicPartition tp, long offset,
                                                           Uuid topicId, ParsleyVectorClock dependencies) {
        return new ParsleyMessage<>(tp.topic(), topicId, tp.partition(), offset, 0L,
                "k", "v", List.of(), dependencies);
    }
}
