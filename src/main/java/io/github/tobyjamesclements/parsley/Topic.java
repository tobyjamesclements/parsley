package io.github.tobyjamesclements.parsley;

/**
 * A typed topic: name and codecs declared once, then shared. A pipeline hop is the same
 * {@code Topic} appearing as one stage's sink and another's source, which makes codec
 * agreement across the hop hold by construction — declare each topic exactly once and pass
 * the object around.
 *
 * <p>A topic is also the factory for {@link Emission}s to itself: {@code topic.send(k, v)}
 * is the only way user logic expresses output, and it type-checks the key and value against
 * the topic's codecs at the construction site.
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

    public static <K, V> Topic<K, V> of(String name, Codec<K> keyCodec, Codec<V> valueCodec) {
        if (!name.matches("[a-zA-Z0-9._-]+")) {
            throw new IllegalArgumentException("not a legal topic name: " + name);
        }
        return new Topic<>(name, keyCodec, valueCodec);
    }

    /** An emission to this topic, timestamped by the message being handled when it is applied. */
    public Emission send(K key, V value) {
        return new Emission(this, key, value, Emission.INHERIT_TIMESTAMP);
    }

    /** An emission to this topic with an explicit timestamp. */
    public Emission send(K key, V value, long timestamp) {
        if (timestamp < 0) throw new IllegalArgumentException("negative timestamp: " + timestamp);
        return new Emission(this, key, value, timestamp);
    }

    String name() {
        return name;
    }

    Codec<K> keyCodec() {
        return keyCodec;
    }

    Codec<V> valueCodec() {
        return valueCodec;
    }

    @Override
    public String toString() {
        return name;
    }
}
