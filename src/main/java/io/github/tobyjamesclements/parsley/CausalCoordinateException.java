package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.jspecify.annotations.Nullable;

/**
 * A {@link CausalDeliveryException} anchored to a single source coordinate — the {@code (topic,
 * topicId, partition, offset)} quartet identifying the record the failure is about, exposed for
 * operator diagnostics. Modelled on Kafka's own {@code org.apache.kafka.common.errors.ApiException}
 * family: a common base carrying the fields every subtype needs, with each subclass contributing
 * only what is specific to its own failure (a {@code details()} diagnostic string, raw undecodable
 * bytes, and so on).
 */
public abstract class CausalCoordinateException extends CausalDeliveryException {

    private final String topic;
    private final Uuid topicId;
    private final int partition;
    private final long offset;

    CausalCoordinateException(String message, @Nullable Throwable cause,
                              String topic, Uuid topicId, int partition, long offset) {
        super(message, cause);
        this.topic = topic;
        this.topicId = topicId;
        this.partition = partition;
        this.offset = offset;
    }

    /** The source topic's name at the time the failure was raised. */
    public String topic() {
        return topic;
    }

    /** The source topic's stable UUID. */
    public Uuid topicId() {
        return topicId;
    }

    /** The source partition. */
    public int partition() {
        return partition;
    }

    /** The record's offset on that partition. */
    public long offset() {
        return offset;
    }
}
