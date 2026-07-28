package io.github.tobyjamesclements.parsley;

/**
 * A causally delivered message: the decoded key and value plus the message's own coordinate —
 * its source topic, partition, offset, and timestamp. For a message released from the hold
 * queue, the coordinate is the message's own, not that of whichever later record triggered
 * the release.
 *
 * <p>Handlers and folds receive messages; tests construct them directly ({@link #of} for the
 * common case where the coordinate is irrelevant) and assert on the returned emissions.
 */
public record Message<K, V>(String topic, int partition, long offset, long timestamp, K key, V value) {

    /** A message at coordinate zero, for unit tests of pure logic. */
    public static <K, V> Message<K, V> of(String topic, K key, V value) {
        return new Message<>(topic, 0, 0L, 0L, key, value);
    }
}
