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
class CausalBufferUnboundedLimitTest {

    private static final TopicPartition T1 = new TopicPartition("t1", 0);
    private static final TopicPartition T2 = new TopicPartition("t2", 0);
    private static final Uuid T1_ID = Uuid.randomUuid();
    private static final Uuid T2_ID = Uuid.randomUuid();

    /**
     * Records buffered under an unbounded limit are never evicted, regardless of depth. Once the
     * blocking dependency is satisfied the buffer drains to zero in causal order.
     *
     * <p>Five T2 records each depend on T1/0 having been observed at offset 4; T1 has not been
     * seen yet. All five buffer without any eviction. When T1@0–@4 arrive and advance the T1
     * frontier to 4, the dependency is satisfied and all five T2 records are released in order.
     */
    @Test
    void unboundedLimitNeverEvictsAndReleasesInCausalOrderWhenDependenciesSatisfied() {
        MockBufferStore<String, String> buffer = new MockBufferStore<>();
        ParsleyEngine<String, String> engine = new ParsleyEngine<>(
                ParsleyClock.empty(),
                buffer, new MockCandidateIndex(), new MockForwardedIndex(), new MockOrphanIndex(), ParsleyMetrics.NOOP,
                CausalAudit.NOOP, System::currentTimeMillis);

        List<ParsleyMessage<String, String>> out = new ArrayList<>();
        ParsleyClock needsT1 = ParsleyClock.empty().observe(T1_ID, 0, 4);
        for (long offset = 0; offset < 5; offset++) {
            out.addAll(engine.onRecord(message(T2, offset, T2_ID, needsT1)).delivered());
        }

        assertEquals(5, buffer.size(), "all five records must be held — no eviction must have occurred");
        assertEquals(List.of(), out, "no record must be forwarded before the dependency is satisfied");

        for (long offset = 0; offset < 5; offset++) {
            out.addAll(engine.onRecord(message(T1, offset, T1_ID, ParsleyClock.empty())).delivered());
        }

        assertEquals(0, buffer.size(), "buffer must be empty after the dependency is satisfied");
        assertEquals(
                List.of(0L, 1L, 2L, 3L, 4L, 0L, 1L, 2L, 3L, 4L),
                out.stream().map(ParsleyMessage::offset).toList(),
                "T1@0–@4 must be forwarded first, then T2@0–@4 released in causal order");
    }

    private static ParsleyMessage<String, String> message(TopicPartition tp, long offset,
                                                           Uuid topicId, ParsleyClock dependencies) {
        return new ParsleyMessage<>(tp.topic(), topicId, tp.partition(), offset, 0L,
                "k", "v", List.of(), dependencies);
    }
}
