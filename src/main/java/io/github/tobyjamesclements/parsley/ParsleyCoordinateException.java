package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.jspecify.annotations.Nullable;

/**
 * Shared base for a Parsley exception anchored to a single source coordinate — the {@code (topic,
 * topicId, partition, offset)} quartet every fail-closed engine exception carries for operator
 * diagnostics. Modelled on Kafka's own {@code org.apache.kafka.common.errors.ApiException} family: a
 * common base carrying the fields every subtype needs, with each subclass contributing only what is
 * specific to its own failure (a {@code details()} diagnostic string, raw undecodable bytes, and so on).
 */
abstract class ParsleyCoordinateException extends RuntimeException {

    private final String topic;
    private final Uuid topicId;
    private final int partition;
    private final long offset;

    ParsleyCoordinateException(String message, @Nullable Throwable cause,
                               String topic, Uuid topicId, int partition, long offset) {
        super(message, cause);
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
