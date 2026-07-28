package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.serialization.Serde;

/**
 * A typed topic: name and serdes declared once, then shared. A pipeline hop is the same
 * {@code CausalTopic} appearing as one stage's sink and another's source, which makes serde
 * agreement across the hop hold by construction — declare each topic exactly once and pass
 * the object around.
 */
public final class CausalTopic<K, V> {

    private final String name;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;

    private CausalTopic(String name, Serde<K> keySerde, Serde<V> valueSerde) {
        this.name = name;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    public static <K, V> CausalTopic<K, V> of(String name, Serde<K> keySerde, Serde<V> valueSerde) {
        return new CausalTopic<>(name, keySerde, valueSerde);
    }

    String name() {
        return name;
    }

    Serde<K> keySerde() {
        return keySerde;
    }

    Serde<V> valueSerde() {
        return valueSerde;
    }

    @Override
    public String toString() {
        return name;
    }
}
