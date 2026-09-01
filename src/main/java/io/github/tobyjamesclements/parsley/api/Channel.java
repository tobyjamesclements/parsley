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
        if (!KafkaNames.isValidTopicName(topic)) {
            throw new IllegalArgumentException("topic must be a valid Kafka topic name ("
                    + KafkaNames.RULE + "): " + topic);
        }
        if (topic.contains(Store.RESERVED_PREFIX)) {
            throw new IllegalArgumentException("topic may not contain the reserved namespace "
                    + Store.RESERVED_PREFIX + ", which parsley uses for its own topics: " + topic);
        }
        if (keySerde == null) {
            throw new IllegalArgumentException(topic + ": keySerde must be non-null");
        }
        if (valueSerde == null) {
            throw new IllegalArgumentException(topic + ": valueSerde must be non-null");
        }
        if (initialPosition == null) {
            throw new IllegalArgumentException(topic + ": initialPosition must be non-null");
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
     * @throws IllegalArgumentException if {@code topic} is not a valid Kafka topic name,
     *                                  contains the reserved {@link Store#RESERVED_PREFIX}
     *                                  namespace, or a serde is null
     */
    public static <K, V> Channel<K, V> of(String topic, Serde<K> keySerde, Serde<V> valueSerde) {
        return new Channel<>(topic, keySerde, valueSerde, InitialPosition.EARLIEST);
    }

    /**
     * Returns a copy of this channel with a different starting position.
     *
     * <p>The starting position applies only on a process's first start ever, before it has
     * any ordering state. A channel added later to a process that has run, or a partition
     * whose committed position has expired, begins at {@link InitialPosition#EARLIEST}
     * whatever was declared: a later {@code LATEST} would make a restart observable in
     * what is delivered (D36). Positions below the first receipt count as already
     * satisfied.
     *
     * @param initialPosition where to begin reading
     * @return a new channel, leaving this one unchanged
     * @throws IllegalArgumentException if {@code initialPosition} is null
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
