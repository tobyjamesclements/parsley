package io.github.tobyjamesclements.parsley.api;

/**
 * Read access to the process's declared application state, as of the start of the current delivery — the other half
 * of what the seam passes to application logic (SPEC Structural 3, 8). Writes return through {@link Effects}.
 */
public interface StateReader {

    /** The value under {@code key} in {@code store}, or null when absent. */
    <K, V> V get(StoreDef<K, V> store, K key);
}
