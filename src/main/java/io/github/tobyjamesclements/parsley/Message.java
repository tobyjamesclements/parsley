package io.github.tobyjamesclements.parsley;

/**
 * A causally delivered message, carrying the decoded key and value with the message's own
 * coordinate.
 *
 * <p>For a message released from the hold queue, the coordinate is the message's own, not that
 * of whichever later record triggered the release.
 *
 * <p>Handlers and folds receive messages. Tests construct them directly and assert on the
 * returned emissions, using {@link #of} where the coordinate is irrelevant.
 *
 * @param <K> the decoded key type
 * @param <V> the decoded value type
 * @param topic the source topic the message arrived on
 * @param partition the partition it arrived on
 * @param offset its offset on that partition
 * @param timestamp its record timestamp
 * @param key the decoded key, or null
 * @param value the decoded value, or null
 */
public record Message<K, V>(String topic, int partition, long offset, long timestamp, K key, V value) {

    /**
     * Returns a message at coordinate zero, for unit tests of pure logic.
     *
     * @param <K> the decoded key type
     * @param <V> the decoded value type
     * @param topic the source topic to name
     * @param key the decoded key, or null
     * @param value the decoded value, or null
     * @return the message, at partition, offset and timestamp zero
     */
    public static <K, V> Message<K, V> of(String topic, K key, V value) {
        return new Message<>(topic, 0, 0L, 0L, key, value);
    }
}
