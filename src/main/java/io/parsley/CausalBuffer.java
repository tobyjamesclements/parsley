package io.parsley;

import org.apache.kafka.common.serialization.Serde;

import java.util.Objects;

/**
 * Registers one causal source: a topic name paired with the serdes Parsley's buffer uses to
 * (de)serialise records held from that topic.
 *
 * <p>Supply one per input topic to {@link CausalProcessors.Builder#addBuffer}. The topic's stable
 * UUID is resolved from the broker automatically, so a buffer only carries what the DSL cannot — the
 * per-topic serdes the buffer store round-trips held records with.
 *
 * <p>For Schema Registry serdes (e.g. Avro), pass the <strong>same configured instance</strong> you
 * pass to {@code Consumed.with(...)}: Parsley uses the serde as-is and resolves the registry subject
 * from the source topic, exactly as Kafka Streams does. See {@link CausalProcessorSupplier} for the
 * held-record round-trip behaviour.
 *
 * @param topic      the source topic name
 * @param keySerde   the key serde the buffer (de)serialises held keys with
 * @param valueSerde the value serde the buffer (de)serialises held values with
 * @param <K>        the key type
 * @param <V>        the value type
 */
public record CausalBuffer<K, V>(String topic, Serde<K> keySerde, Serde<V> valueSerde) {

    public CausalBuffer {
        Objects.requireNonNull(topic, "topic must not be null");
        Objects.requireNonNull(keySerde, "keySerde must not be null");
        Objects.requireNonNull(valueSerde, "valueSerde must not be null");
    }

    /**
     * Creates a buffer registration for {@code topic} with the given serdes.
     *
     * @param topic      the source topic name
     * @param keySerde   the key serde
     * @param valueSerde the value serde
     * @param <K>        the key type
     * @param <V>        the value type
     * @return a new {@code CausalBuffer}
     */
    public static <K, V> CausalBuffer<K, V> of(String topic, Serde<K> keySerde, Serde<V> valueSerde) {
        return new CausalBuffer<>(topic, keySerde, valueSerde);
    }
}
