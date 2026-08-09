package io.github.tobyjamesclements.parsley.api;

import org.apache.kafka.common.serialization.Serde;

/**
 * A typed topic a process receives from or sends to.
 *
 * <p>A channel binds a topic name to the serdes for its keys and values. Causal metadata
 * travels in reserved headers and never in the key or value, so a channel's serdes describe
 * only the application's own data.
 *
 * @param <K> key type
 * @param <V> value type
 * @see ProcessDefinition.Builder#receives(Channel, Handler)
 * @see ProcessDefinition.Builder#sends(Channel...)
 */
public final class Channel<K, V> {

    /** Where a process begins reading a channel it has no committed position for. */
    public enum InitialPosition {
        /** Begin at the earliest retained message. */
        EARLIEST,
        /** Begin at the end, skipping messages already retained. */
        LATEST
    }

    private final String topic;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final InitialPosition initialPosition;

    private Channel(String topic, Serde<K> keySerde, Serde<V> valueSerde, InitialPosition initialPosition) {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic must be non-blank");
        }
        this.topic = topic;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.initialPosition = initialPosition;
    }

    /**
     * Defines a channel starting at {@link InitialPosition#EARLIEST}.
     *
     * @param topic      the Kafka topic name
     * @param keySerde   serde for keys
     * @param valueSerde serde for values
     * @param <K>        key type
     * @param <V>        value type
     * @return the channel
     * @throws IllegalArgumentException if {@code topic} is null or blank
     */
    public static <K, V> Channel<K, V> of(String topic, Serde<K> keySerde, Serde<V> valueSerde) {
        return new Channel<>(topic, keySerde, valueSerde, InitialPosition.EARLIEST);
    }

    /**
     * Returns a copy of this channel with a different starting position.
     *
     * <p>The starting position applies only where no committed position exists. It has no
     * effect on a process resuming from committed state.
     *
     * @param initialPosition where to begin reading
     * @return a new channel, leaving this one unchanged
     */
    public Channel<K, V> startingAt(InitialPosition initialPosition) {
        return new Channel<>(topic, keySerde, valueSerde, initialPosition);
    }

    /**
     * Returns the Kafka topic name.
     *
     * @return the Kafka topic name
     */
    public String topic() {
        return topic;
    }

    /**
     * Returns the serde for keys.
     *
     * @return the serde for keys
     */
    public Serde<K> keySerde() {
        return keySerde;
    }

    /**
     * Returns the serde for values.
     *
     * @return the serde for values
     */
    public Serde<V> valueSerde() {
        return valueSerde;
    }

    /**
     * Returns where reading begins when no committed position exists.
     *
     * @return where reading begins when no committed position exists
     */
    public InitialPosition initialPosition() {
        return initialPosition;
    }

    /**
     * Returns the topic name, wrapped for diagnostics.
     *
     * @return the topic name, wrapped for diagnostics
     */
    @Override
    public String toString() {
        return "Channel(" + topic + ")";
    }
}
