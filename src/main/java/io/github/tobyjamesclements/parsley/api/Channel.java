package io.github.tobyjamesclements.parsley.api;

import org.apache.kafka.common.serialization.Serde;

/**
 * A typed handle for a channel as an application declares it: a topic plus the serdes for its key and value types
 * (SPEC Structural 4, 17; Assumption 14 — a declared topic stands for the topic-partitions the host induces).
 * The serdes are the application's own codecs: what they produce is exactly what lands in the record's key and value,
 * so any reader with these codecs alone can decode it (SPEC Safety 4, 5).
 */
public final class Channel<K, V> {

    /** Where a process starts reading a channel it has never read before. */
    public enum InitialPosition { EARLIEST, LATEST }

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

    public static <K, V> Channel<K, V> of(String topic, Serde<K> keySerde, Serde<V> valueSerde) {
        return new Channel<>(topic, keySerde, valueSerde, InitialPosition.EARLIEST);
    }

    public Channel<K, V> startingAt(InitialPosition initialPosition) {
        return new Channel<>(topic, keySerde, valueSerde, initialPosition);
    }

    public String topic() {
        return topic;
    }

    public Serde<K> keySerde() {
        return keySerde;
    }

    public Serde<V> valueSerde() {
        return valueSerde;
    }

    public InitialPosition initialPosition() {
        return initialPosition;
    }

    @Override
    public String toString() {
        return "Channel(" + topic + ")";
    }
}
