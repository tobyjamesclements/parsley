package io.parsley;


import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Holds records whose causal dependencies are not yet satisfied, in insertion order.
 *
 * <p>This is a passive store: it {@linkplain #drain releases} records the frontier has caught up to
 * and {@linkplain #evictAll surrenders} its whole contents when a limit fires. The policy-driven
 * decision about what happens to evicted records (forward, drop, or dead-letter) and all violation
 * reporting live in {@link CausalEngine}.
 *
 * @param <K> the record key type
 * @param <V> the record value type
 */
final class CausalBuffer<K, V> {

    /** A buffered record together with its decoded dependency clock. */
    record Buffered<K, V>(CausalRecord<K, V> record, VectorClock dependencies) {}

    // Keyed by a monotonic sequence rather than stored in a List so each entry has a stable unique
    // id; the LinkedHashMap preserves insertion (causal arrival) order, which drain() and evictAll()
    // rely on to release records in the order they were received.
    private final LinkedHashMap<Long, Buffered<K, V>> buffer = new LinkedHashMap<>();
    private long sequence = 0;

    /**
     * Adds a record with its decoded dependency clock.
     *
     * @param record       the record to buffer
     * @param dependencies the record's decoded causal dependencies
     */
    void add(CausalRecord<K, V> record, VectorClock dependencies) {
        buffer.put(sequence++, new Buffered<>(record, dependencies));
    }

    /**
     * Removes and returns every buffered record whose dependencies are satisfied by
     * {@code frontier}, in insertion order.
     *
     * @param frontier the current causal frontier
     * @return the released records, in order; empty if none are releasable
     */
    List<CausalRecord<K, V>> drain(VectorClock frontier) {
        List<CausalRecord<K, V>> released = new ArrayList<>();
        buffer.entrySet().removeIf(entry -> {
            if (entry.getValue().dependencies().satisfiedBy(frontier)) {
                released.add(entry.getValue().record());
                return true;
            }
            return false;
        });
        return released;
    }

    /**
     * Removes and returns every buffered entry, in insertion order.
     *
     * @return all buffered entries; empty if the buffer is empty
     */
    List<Buffered<K, V>> evictAll() {
        List<Buffered<K, V>> all = new ArrayList<>(buffer.values());
        buffer.clear();
        return all;
    }

    /**
     * Returns the number of records currently buffered.
     *
     * @return the buffer size
     */
    int size() {
        return buffer.size();
    }
}
