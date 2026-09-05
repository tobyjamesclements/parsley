package io.github.tobyjamesclements.parsley.api;

import org.apache.kafka.common.serialization.Serde;

/**
 * A typed key-value store a process reads and writes.
 *
 * <p>Stores declared here hold application state. Parsley keeps its own ordering state in
 * separate stores under {@link Parsley#RESERVED_PREFIX}, which application names may not contain
 * anywhere: an embedded occurrence would compose a changelog topic name inside parsley's
 * namespace.
 *
 * @param <K> key type
 * @param <V> value type
 * @see Process.Builder#stores(Store...)
 * @see State
 */
public final class Store<K, V> {

    private final String name;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;

    private Store(String name, Serde<K> keySerde, Serde<V> valueSerde) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("store name must be non-blank");
        }
        if (name.contains(Parsley.RESERVED_PREFIX)) {
            throw new IllegalArgumentException("store name may not contain the reserved namespace "
                    + Parsley.RESERVED_PREFIX + ": an embedded occurrence composes a changelog topic name"
                    + " inside parsley's own namespace: " + name);
        }
        if (keySerde == null) {
            throw new IllegalArgumentException(name + ": keySerde must be non-null");
        }
        if (valueSerde == null) {
            throw new IllegalArgumentException(name + ": valueSerde must be non-null");
        }
        this.name = name;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    /**
     * Defines a store.
     *
     * <p>Declare each store once and pass that instance both to
     * {@link Process.Builder#stores(Store...)} and to every read and write: the
     * seam matches stores by instance, since a read returns a value cast to the instance's
     * types, so a second {@code Store.of} for the same name is refused at the first access
     * as {@code STATE_ACCESS_TO_UNDECLARED_STORE}. Channels, by contrast, are matched by
     * topic name.
     *
     * @param name       the store name
     * @param keySerde   serde for keys
     * @param valueSerde serde for values
     * @param <K>        key type
     * @param <V>        value type
     * @return the store
     * @throws IllegalArgumentException if {@code name} is null or blank, contains
     *                                  {@link Parsley#RESERVED_PREFIX}, or a serde is null
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
