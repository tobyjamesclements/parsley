package io.github.tobyjamesclements.parsley.api;

import java.util.List;

import io.github.tobyjamesclements.parsley.core.HeaderKV;

/**
 * A delivered message: exactly what the seam passes to application logic, together with the process's application
 * state (SPEC Structural 3). The key and value are decoded with the channel's own serdes; the message's metadata —
 * channel, position, timestamp, headers — rides alongside, never inside, the key and value (SPEC Safety 4).
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

    public static <K, V> Delivery<K, V> of(Channel<K, V> channel, int partition, long position, long timestamp,
                                           K key, V value, List<HeaderKV> headers) {
        return new Delivery<>(channel, partition, position, timestamp, key, value, headers);
    }

    public Channel<K, V> channel() {
        return channel;
    }

    public int partition() {
        return partition;
    }

    /** The message's position on its channel: its offset. */
    public long position() {
        return position;
    }

    /** The record's timestamp. Data for the application; never an input to delivery order (SPEC Structural 7). */
    public long timestamp() {
        return timestamp;
    }

    public K key() {
        return key;
    }

    public V value() {
        return value;
    }

    /** The application headers the record carried. Headers under the reserved {@code parsley.} prefix are
     * parsley's transport detail, not application data, and are filtered out — so forwarding these headers on an
     * emission (a natural pattern) never trips the reserved-prefix refusal (D56). */
    public List<HeaderKV> headers() {
        return headers;
    }
}
