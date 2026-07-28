package io.github.tobyjamesclements.parsley;

import java.util.Objects;

/**
 * One output of user logic: a key and value bound for a topic, as a value. Emissions are
 * created by {@link Topic#send}, returned from handlers and folds, and applied by the
 * runtime — stamped, partitioned, and produced transactionally with the delivery that
 * caused them. Two emissions are equal when they name the same topic and carry equal key,
 * value, and timestamp, so pure logic is testable with plain equality.
 *
 * <p>A timestamp of {@code -1} (the {@link Topic#send(Object, Object)} default) means the
 * emission inherits the timestamp of the message being handled.
 */
public final class Emission {

    static final long INHERIT_TIMESTAMP = -1L;

    private final Topic<?, ?> topic;
    private final Object key;
    private final Object value;
    private final long timestamp;

    Emission(Topic<?, ?> topic, Object key, Object value, long timestamp) {
        this.topic = topic;
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }

    Topic<?, ?> topic() {
        return topic;
    }

    Object key() {
        return key;
    }

    Object value() {
        return value;
    }

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
