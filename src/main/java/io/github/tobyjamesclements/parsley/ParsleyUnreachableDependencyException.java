package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;

/**
 * Thrown when a record's declared dependencies name a coordinate this node has no input channel for —
 * an undeclared topic, or a partition a different task instance owns — and no dead-letter sink is
 * configured to divert it instead.
 *
 * <p>Such a coordinate can never be confirmed by this node no matter how long it waits, but that does
 * not make the dependency safe to disregard: this node can prove it cannot check the coordinate, not
 * that the coordinate is genuinely irrelevant. Fail-closed, not vacuous satisfaction — the record is
 * never buffered and its source offset is not committed past it, so it is reprocessed on restart. With
 * a dead-letter sink configured, {@link ParsleyEngine} diverts the record instead of throwing this; see
 * {@link ParsleyEngine.DeadLetter.Reason#UNREACHABLE_DEPENDENCY}.
 */
final class ParsleyUnreachableDependencyException extends RuntimeException {

    private final String topic;
    private final Uuid topicId;
    private final int partition;
    private final long offset;

    ParsleyUnreachableDependencyException(String topic, Uuid topicId, int partition, long offset) {
        super("record " + topic + "-" + partition + "@" + offset
                + " depends on a coordinate this node has no channel for; the record was not forwarded");
        this.topic = topic;
        this.topicId = topicId;
        this.partition = partition;
        this.offset = offset;
    }

    String topic() {
        return topic;
    }

    /** The source topic's stable UUID. */
    Uuid topicId() {
        return topicId;
    }

    int partition() {
        return partition;
    }

    long offset() {
        return offset;
    }
}
