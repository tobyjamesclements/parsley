package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;

/**
 * Thrown when a {@link CausalBufferLimit} fires and would otherwise force a held record's delivery
 * out of causal order, while {@code parsley.buffer.eviction.failure.policy = fail} (the default).
 *
 * <p>Unlike {@link ParsleyBufferDeserializationException}, this is never a decode failure — the
 * record is perfectly decodable, it simply hasn't had its causal dependencies satisfied yet. Failing
 * fast here leaves it in the buffer (nothing is removed), so it is retried — and may yet be released
 * naturally, or evicted, on the next attempt — rather than delivered out of order. This is a
 * {@link RuntimeException}, fatal only to the owning Streams task, never crashing the JVM.
 *
 * <p>{@code parsley.buffer.eviction.failure.policy = continue} restores Parsley's original
 * always-forward behaviour: the record is evicted and delivered anyway (logged, counted as a
 * violation) instead of this exception being thrown.
 */
final class ParsleyBufferEvictionLimitException extends RuntimeException {

    private final String topic;
    private final Uuid topicId;
    private final int partition;
    private final long offset;
    private final CausalDependencies gap;

    ParsleyBufferEvictionLimitException(String topic, Uuid topicId, int partition, long offset,
                                        CausalBufferLimit limit, CausalDependencies gap) {
        super("buffer limit (" + limit + ") fired on the oldest held record " + topic + "-" + partition
                + "@" + offset + " with unsatisfied dependencies " + gap
                + "; failing fast (parsley.buffer.eviction.failure.policy = fail) rather than deliver it"
                + " out of causal order. It remains buffered.");
        this.topic = topic;
        this.topicId = topicId;
        this.partition = partition;
        this.offset = offset;
        this.gap = gap;
    }

    String topic() {
        return topic;
    }

    Uuid topicId() {
        return topicId;
    }

    int partition() {
        return partition;
    }

    long offset() {
        return offset;
    }

    /** The dependencies that remained unsatisfied when the limit fired. */
    CausalDependencies gap() {
        return gap;
    }
}
