package io.github.tobyjamesclements.parsley.api;

import org.apache.kafka.common.serialization.Serde;

/**
 * A typed application state store declaration. Stores are ordinary Kafka Streams persistent key-value stores — host
 * state facilities, not exclusively owned by this implementation (SPEC Structural 8, 18). Names with the reserved
 * {@code __parsley.} prefix are refused so application state can never alias ordering state.
 */
public final class StoreDef<K, V> {

    public static final String RESERVED_PREFIX = "__parsley.";

    private final String name;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;

    private StoreDef(String name, Serde<K> keySerde, Serde<V> valueSerde) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("store name must be non-blank");
        }
        if (name.startsWith(RESERVED_PREFIX)) {
            throw new IllegalArgumentException("store name may not use the reserved prefix " + RESERVED_PREFIX);
        }
        this.name = name;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    public static <K, V> StoreDef<K, V> of(String name, Serde<K> keySerde, Serde<V> valueSerde) {
        return new StoreDef<>(name, keySerde, valueSerde);
    }

    public String name() {
        return name;
    }

    public Serde<K> keySerde() {
        return keySerde;
    }

    public Serde<V> valueSerde() {
        return valueSerde;
    }

    @Override
    public String toString() {
        return "StoreDef(" + name + ")";
    }
}
