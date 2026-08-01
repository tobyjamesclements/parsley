package io.github.tobyjamesclements.parsley;

import java.util.Objects;

/**
 * One output of user logic, a key and value bound for a topic.
 *
 * <p>Created by {@link Topic#send}, returned from handlers and folds, and applied by the
 * runtime, which stamps it, partitions it, and produces it transactionally with the delivery
 * that caused it.
 *
 * <p>Two emissions are equal when they name the same topic and carry equal key, value and
 * timestamp, so pure logic is testable with plain equality.
 *
 * <p>A timestamp of {@code -1}, which {@link Topic#send(Object, Object)} uses, means the
 * emission inherits the timestamp of the message being handled.
 */
public final class Emission {

    /** The timestamp meaning inherit from the message being handled. */
    static final long INHERIT_TIMESTAMP = -1L;

    private final Topic<?, ?> topic;
    private final Object key;
    private final Object value;
    private final long timestamp;

    /**
     * Binds a key and value for a topic.
     *
     * @param topic the destination topic
     * @param key the emitted key, or null
     * @param value the emitted value, or null
     * @param timestamp the record timestamp, or {@link #INHERIT_TIMESTAMP}
     */
    Emission(Topic<?, ?> topic, Object key, Object value, long timestamp) {
        this.topic = topic;
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }

    /**
     * Returns the destination topic.
     *
     * @return the topic this emission is bound for
     */
    Topic<?, ?> topic() {
        return topic;
    }

    /**
     * Returns the emitted key.
     *
     * @return the key, or null
     */
    Object key() {
        return key;
    }

    /**
     * Returns the emitted value.
     *
     * @return the value, or null
     */
    Object value() {
        return value;
    }

    /**
     * Returns the record timestamp.
     *
     * @return the timestamp, or {@link #INHERIT_TIMESTAMP} to inherit the handled message's
     */
    long timestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Emission e
                && topic.name().equals(e.topic.name())
                && Objects.equals(key, e.key)
                && Objects.equals(value, e.value)
                && timestamp == e.timestamp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic.name(), key, value, timestamp);
    }

    @Override
    public String toString() {
        return topic.name() + "<-(" + key + ", " + value
                + (timestamp == INHERIT_TIMESTAMP ? "" : ", @" + timestamp) + ")";
    }
}
