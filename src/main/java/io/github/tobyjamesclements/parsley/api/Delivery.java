package io.github.tobyjamesclements.parsley.api;

import java.util.List;

import io.github.tobyjamesclements.parsley.core.HeaderKV;

/**
 * One message, established as causally deliverable and handed to application logic.
 *
 * <p>Every cause of this message has already been delivered to this process. Reserved
 * headers carrying causal metadata are removed before construction, so {@link #headers()}
 * shows only what the application itself sent.
 *
 * @param <K> key type
 * @param <V> value type
 * @see Handler#handle(Delivery, StateReader)
 */
public final class Delivery<K, V> {
    private final Channel<K, V> channel;
    private final int partition;
    private final long position;
    private final long timestamp;
    private final K key;
    private final V value;
    private final List<HeaderKV> headers;

    Delivery(Channel<K, V> channel, int partition, long position, long timestamp, K key, V value, List<HeaderKV> headers) {
        this.channel = channel;
        this.partition = partition;
        this.position = position;
        this.timestamp = timestamp;
        this.key = key;
        this.value = value;
        this.headers = headers.stream()
                .filter(header -> !header.key().startsWith(io.github.tobyjamesclements.parsley.core.CausesCodec.RESERVED_HEADER_PREFIX))
                .toList();
    }

    /**
     * Builds a delivery. Intended for tests driving a {@link Handler} directly.
     *
     * @param channel   the channel the message arrived on
     * @param partition the partition within that channel
     * @param position  the offset within that partition
     * @param timestamp the message timestamp
     * @param key       the message key
     * @param value     the message value
     * @param headers   the message headers, reserved entries included and filtered out here
     * @param <K>       key type
     * @param <V>       value type
     * @return the delivery
     */
    public static <K, V> Delivery<K, V> of(Channel<K, V> channel, int partition, long position, long timestamp,
                                           K key, V value, List<HeaderKV> headers) {
        return new Delivery<>(channel, partition, position, timestamp, key, value, headers);
    }

    /**
     * Returns the channel this message arrived on.
     *
     * @return the channel this message arrived on
     */
    public Channel<K, V> channel() {
        return channel;
    }

    /**
     * Returns the partition within the channel.
     *
     * @return the partition within the channel
     */
    public int partition() {
        return partition;
    }

    /**
     * Returns the offset of this message within its partition.
     *
     * @return the offset of this message within its partition
     */
    public long position() {
        return position;
    }

    /**
     * Returns the message timestamp.
     *
     * @return the message timestamp
     */
    public long timestamp() {
        return timestamp;
    }

    /**
     * Returns the message key.
     *
     * @return the message key
     */
    public K key() {
        return key;
    }

    /**
     * Returns the message value.
     *
     * @return the message value
     */
    public V value() {
        return value;
    }

    /**
     * Returns the application's own headers, with reserved entries removed.
     *
     * @return the application's own headers, with reserved entries removed
     */
    public List<HeaderKV> headers() {
        return headers;
    }
}
