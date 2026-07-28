package io.github.tobyjamesclements.parsley;

import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;

import java.time.Duration;

/**
 * What a {@link SourceHandler} may do besides compute: emit to the stage's declared sinks,
 * reach the stage's own state stores, and schedule punctuators. Deliberately narrow — the
 * stage owns the causal boundary (stamping, partitioning, serialization), so there is no
 * generic forward and no raw processor context.
 */
public interface StageContext {

    /**
     * Emits one record to a declared sink, timestamped like the record being handled. The
     * stage serializes with the topic's serdes, partitions deterministically, and stamps at
     * the single stamping site. Throws if {@code topic} is not a declared sink of this stage,
     * or when called from a punctuator (no record is in flight — use the timestamped
     * overload).
     */
    <K, V> void emit(CausalTopic<K, V> topic, K key, V value);

    /** Emits with an explicit timestamp (required from punctuators). */
    <K, V> void emit(CausalTopic<K, V> topic, K key, V value, long timestamp);

    /** A state store declared on the stage via {@code Builder.stores}. */
    <S extends StateStore> S store(String name);

    /** Schedules a punctuator on the underlying task. */
    Cancellable schedule(Duration interval, PunctuationType type, Punctuator callback);
}
