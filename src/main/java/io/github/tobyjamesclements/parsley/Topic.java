package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.serialization.Serde;

/**
 * A typed topic a process receives from or sends to.
 *
 * <p>A topic binds a Kafka topic name to the serdes for its keys and values. Causal metadata
 * travels in reserved headers and never in the key or value, so a topic's serdes describe
 * only the application's own data.
 *
 * <p>This is the declaration, not the channel. A running process receives from one
 * {@link Channel} per partition of each topic it declares, and a channel is named by the
 * broker-assigned topic identity rather than by this name.
 *
 * @param <K> key type
 * @param <V> value type
 * @see Process.Builder#receives(Topic, Handler)
 * @see Process.Builder#sends(Topic...)
 */
public final class Topic<K, V> {

    /** Where a process begins reading a topic it has no committed position for. */
    public enum InitialPosition {
        /** Begin at the earliest retained message. */
        EARLIEST,
        /** Begin at the end, skipping messages already retained. */
        LATEST
    }

    private final String name;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final InitialPosition initialPosition;

    private Topic(String name, Serde<K> keySerde, Serde<V> valueSerde, InitialPosition initialPosition) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("topic name must be non-blank");
        }
        if (name.contains(Parsley.RESERVED_PREFIX)) {
            throw new IllegalArgumentException("topic may not contain the reserved namespace "
                    + Parsley.RESERVED_PREFIX + ", which parsley uses for its own topics: " + name);
        }
        if (keySerde == null) {
            throw new IllegalArgumentException(name + ": keySerde must be non-null");
        }
        if (valueSerde == null) {
            throw new IllegalArgumentException(name + ": valueSerde must be non-null");
        }
        if (initialPosition == null) {
            throw new IllegalArgumentException(name + ": initialPosition must be non-null");
        }
        this.name = name;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.initialPosition = initialPosition;
    }

    /**
     * Defines a topic starting at {@link InitialPosition#EARLIEST}.
     *
     * @param name       the Kafka topic name
     * @param keySerde   serde for keys
     * @param valueSerde serde for values
     * @param <K>        key type
     * @param <V>        value type
     * @return the topic
     * @throws IllegalArgumentException if {@code name} is null or blank, contains the
     *                                  reserved {@link Parsley#RESERVED_PREFIX} namespace, or
     *                                  a serde is null
     */
    public static <K, V> Topic<K, V> of(String name, Serde<K> keySerde, Serde<V> valueSerde) {
        return new Topic<>(name, keySerde, valueSerde, InitialPosition.EARLIEST);
    }

    /**
     * Returns a copy of this topic with a different starting position.
     *
     * <p>The starting position applies only on a process's first start ever, before it has
     * any ordering state. A topic added later to a process that has run, or a partition
     * whose committed position has expired, begins at {@link InitialPosition#EARLIEST}
     * whatever was declared: a later {@code LATEST} would make a restart observable in
     * what is delivered. Positions below the first receipt count as already
     * satisfied.
     *
     * @param initialPosition where to begin reading
     * @return a new topic, leaving this one unchanged
     * @throws IllegalArgumentException if {@code initialPosition} is null
     */
    public Topic<K, V> startingAt(InitialPosition initialPosition) {
        return new Topic<>(name, keySerde, valueSerde, initialPosition);
    }

    /**
     * Returns the Kafka topic name.
     *
     * @return the Kafka topic name
     */
    public String name() {
        return name;
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
        return "Topic(" + name + ")";
    }
}
