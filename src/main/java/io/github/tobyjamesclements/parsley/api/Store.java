package io.github.tobyjamesclements.parsley.api;

import org.apache.kafka.common.serialization.Serde;

/**
 * A typed key-value store a process reads and writes.
 *
 * <p>Stores declared here hold application state. Parsley keeps its own ordering state in
 * separate stores under {@link #RESERVED_PREFIX}, which application names may not use.
 *
 * @param <K> key type
 * @param <V> value type
 * @see ProcessDefinition.Builder#stores(Store...)
 * @see StateReader
 */
public final class Store<K, V> {

    /** Name prefix Parsley reserves for its own stores. */
    public static final String RESERVED_PREFIX = "__parsley.";

    private final String name;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;

    private Store(String name, Serde<K> keySerde, Serde<V> valueSerde) {
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

    /**
     * Defines a store.
     *
     * @param name       the store name
     * @param keySerde   serde for keys
     * @param valueSerde serde for values
     * @param <K>        key type
     * @param <V>        value type
     * @return the store definition
     * @throws IllegalArgumentException if {@code name} is null, blank, or begins with
     *                                  {@link #RESERVED_PREFIX}
     */
    public static <K, V> Store<K, V> of(String name, Serde<K> keySerde, Serde<V> valueSerde) {
        return new Store<>(name, keySerde, valueSerde);
    }

    /**
     * Returns the store name.
     *
     * @return the store name
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
     * Returns the store name, wrapped for diagnostics.
     *
     * @return the store name, wrapped for diagnostics
     */
    @Override
    public String toString() {
        return "Store(" + name + ")";
    }
}
