package io.github.tobyjamesclements.parsley;

/**
 * A typed topic, with its name and codecs declared once and then shared.
 *
 * <p>Declare each topic exactly once and pass the object around. A pipeline hop is the same
 * {@code Topic} appearing as one stage's sink and another's source, so codec agreement across
 * the hop holds by construction.
 *
 * <p>A topic is also the factory for {@link Emission}s to itself. {@link #send(Object, Object)}
 * is the only way user logic expresses output, and it type-checks the key and value against the
 * topic's codecs at the construction site.
 *
 * @param <K> the key type this topic carries
 * @param <V> the value type this topic carries
 */
public final class Topic<K, V> {

    private final String name;
    private final Codec<K> keyCodec;
    private final Codec<V> valueCodec;

    private Topic(String name, Codec<K> keyCodec, Codec<V> valueCodec) {
        this.name = name;
        this.keyCodec = keyCodec;
        this.valueCodec = valueCodec;
    }

    /**
     * Declares a topic with its codecs.
     *
     * @param <K> the key type this topic carries
     * @param <V> the value type this topic carries
     * @param name the topic name, matching {@code [a-zA-Z0-9._-]+}
     * @param keyCodec the codec for keys, which must be canonical and deterministic
     * @param valueCodec the codec for values
     * @return the declared topic
     * @throws IllegalArgumentException if the name is not a legal topic name
     */
    public static <K, V> Topic<K, V> of(String name, Codec<K> keyCodec, Codec<V> valueCodec) {
        if (!name.matches("[a-zA-Z0-9._-]+")) {
            throw new IllegalArgumentException("not a legal topic name: " + name);
        }
        return new Topic<>(name, keyCodec, valueCodec);
    }

    /**
     * Returns an emission to this topic, timestamped by the message being handled when it is
     * applied.
     *
     * @param key the emitted key, or null
     * @param value the emitted value, or null
     * @return the emission
     */
    public Emission send(K key, V value) {
        return new Emission(this, key, value, Emission.INHERIT_TIMESTAMP);
    }

    /**
     * Returns an emission to this topic with an explicit timestamp.
     *
     * @param key the emitted key, or null
     * @param value the emitted value, or null
     * @param timestamp the record timestamp, not negative
     * @return the emission
     * @throws IllegalArgumentException if {@code timestamp} is negative
     */
    public Emission send(K key, V value, long timestamp) {
        if (timestamp < 0) throw new IllegalArgumentException("negative timestamp: " + timestamp);
        return new Emission(this, key, value, timestamp);
    }

    /**
     * Returns the topic's name.
     *
     * @return the name declared at {@link #of}
     */
    String name() {
        return name;
    }

    /**
     * Returns the codec keys are encoded and decoded with.
     *
     * @return the key codec
     */
    Codec<K> keyCodec() {
        return keyCodec;
    }

    /**
     * Returns the codec values are encoded and decoded with.
     *
     * @return the value codec
     */
    Codec<V> valueCodec() {
        return valueCodec;
    }

    @Override
    public String toString() {
        return name;
    }
}
