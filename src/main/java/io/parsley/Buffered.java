package io.parsley;

/**
 * A buffered record together with its decoded causal dependency clock — the value the buffer holds
 * for each held record. Shared by the {@link BufferStore} and the {@link BufferedRecordCodec} that
 * serialises it to durable storage.
 *
 * @param <K> the record key type
 * @param <V> the record value type
 */
record Buffered<K, V>(CausalRecord<K, V> record, VectorClock dependencies) {}
