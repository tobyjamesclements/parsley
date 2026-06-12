package io.parsley;

import java.util.List;
import java.util.Optional;

/**
 * A broker-neutral message envelope processed by the causal buffering engine.
 *
 * <p>Broker adapters convert their native record type (e.g. a Kafka {@code ConsumerRecord})
 * into a {@code CausalRecord} before handing it to the engine, and convert released records
 * back when forwarding downstream.
 *
 * @param <K>                 the record key type
 * @param <V>                 the record value type
 * @param <T>                 the concrete {@link VectorClock} type
 * @param key                 the record key; may be {@code null}
 * @param value               the record value; may be {@code null}
 * @param timestamp           the record's broker timestamp (epoch milliseconds)
 * @param headers             the record's metadata attributes, in original order
 * @param encodedDependencies the raw bytes of the {@code parsley-vector-clock} attribute, or
 *                            {@code null} if the record carried none
 * @param position            a clock representing this record's own source coordinate — e.g.
 *                            an otherwise-empty clock advanced to the record's
 *                            partition/offset. The engine advances its frontier by merging
 *                            this clock.
 * @param source              a human-readable source coordinate for diagnostics, e.g.
 *                            {@code "orders-3@27"}
 */
public record CausalRecord<K, V, T extends VectorClock<T>>(
        K key,
        V value,
        long timestamp,
        List<CausalHeader> headers,
        byte[] encodedDependencies,
        T position,
        String source) {

    /**
     * Canonical constructor; defensively copies {@code headers}.
     */
    public CausalRecord {
        headers = List.copyOf(headers);
    }

    /**
     * Returns the raw bytes of the {@code parsley-vector-clock} attribute, if present.
     *
     * @return the encoded dependency clock, or empty if the record carried none
     */
    public Optional<byte[]> dependencies() {
        return Optional.ofNullable(encodedDependencies);
    }
}
