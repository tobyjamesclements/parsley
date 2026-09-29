package io.github.tobyjamesclements.parsley;

import java.util.List;


/**
 * One message, established as causally deliverable and handed to application logic.
 *
 * <p>Every cause of this message has already been delivered to this process. Reserved
 * headers carrying causal metadata are removed before construction, so {@link #headers()}
 * shows only what the application itself sent.
 *
 * @param <K> key type
 * @param <V> value type
 * @see Handler#handle(Delivery, State)
 */
public final class Delivery<K, V> {
    private final Topic<K, V> topic;
    private final Channel channel;
    private final long position;
    private final long timestamp;
    private final K key;
    private final V value;
    private final List<Header> headers;

    Delivery(Topic<K, V> topic, Channel channel, long position, long timestamp, K key, V value, List<Header> headers) {
        this.topic = topic;
        this.channel = channel;
        this.position = position;
        this.timestamp = timestamp;
        this.key = key;
        this.value = value;
        this.headers = headers.stream()
                .filter(header -> !header.key().startsWith(CausesCodec.RESERVED_HEADER_PREFIX))
                .toList();
    }

    /**
     * Builds a delivery. Intended for tests driving a {@link Handler} directly.
     *
     * @param topic     the declared topic the message arrived on
     * @param channel   the channel it arrived on: one partition of that topic, by identity
     * @param position  the offset within that channel
     * @param timestamp the message timestamp
     * @param key       the message key
     * @param value     the message value
     * @param headers   the message headers, reserved entries included and filtered out here
     * @param <K>       key type
     * @param <V>       value type
     * @return the delivery
     * @throws IllegalArgumentException if {@code topic} or {@code channel} is null
     */
    public static <K, V> Delivery<K, V> of(Topic<K, V> topic, Channel channel, long position, long timestamp,
                                           K key, V value, List<Header> headers) {
        if (topic == null) {
            throw new IllegalArgumentException("topic must be non-null");
        }
        if (channel == null) {
            throw new IllegalArgumentException(topic.name() + ": channel must be non-null");
        }
        return new Delivery<>(topic, channel, position, timestamp, key, value, headers);
    }

    /**
     * Returns the declared topic this message arrived on.
     *
     * @return the declared topic this message arrived on
     */
    public Topic<K, V> topic() {
        return topic;
    }

    /**
     * Returns the channel this message arrived on.
     *
     * @return the channel this message arrived on
     */
    public Channel channel() {
        return channel;
    }

    /**
     * Returns the partition within the topic, which is {@link Channel#partition()}.
     *
     * @return the partition within the topic
     */
    public int partition() {
        return channel.partition();
    }

    /**
     * Returns the offset of this message within its channel.
     *
     * @return the offset of this message within its channel
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
    public List<Header> headers() {
        return headers;
    }
}
