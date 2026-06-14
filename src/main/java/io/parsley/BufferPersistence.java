package io.parsley;


/**
 * Mirrors the {@link CausalEngine}'s in-memory buffer to durable storage so held records survive a
 * restart.
 *
 * <p>{@link #onHeld} fires when a record is buffered because its dependencies are unmet;
 * {@link #onUnheld} fires when a record leaves the buffer for <em>any</em> reason — released by an
 * advancing frontier, or evicted by a limit under any policy (forwarded, dropped, or dead-lettered).
 * Implementations write the held record on {@code onHeld} and delete it on {@code onUnheld}.
 *
 * @param <K> the record key type
 * @param <V> the record value type
 */
interface BufferPersistence<K, V> {

    /**
     * Called when {@code record} is added to the buffer.
     *
     * @param record       the buffered record
     * @param dependencies the record's decoded causal dependencies
     */
    void onHeld(CausalRecord<K, V> record, VectorClock dependencies);

    /**
     * Called when {@code record} leaves the buffer, regardless of why.
     *
     * @param record the record that left the buffer
     */
    void onUnheld(CausalRecord<K, V> record);

    /**
     * Returns a persistence that does nothing — used by the {@link CausalEngine} convenience
     * constructor when buffer durability is not required (e.g. in unit tests that exercise the
     * engine without a state store).
     *
     * @param <K> the record key type
     * @param <V> the record value type
     * @return a no-op {@code BufferPersistence}
     */
    static <K, V> BufferPersistence<K, V> noop() {
        return new BufferPersistence<>() {
            @Override
            public void onHeld(CausalRecord<K, V> record, VectorClock dependencies) {}

            @Override
            public void onUnheld(CausalRecord<K, V> record) {}
        };
    }
}
