package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.serialization.Serde;

import java.util.function.Function;

/**
 * Resolves the key/value {@link Serde} for a held record <strong>by the record's own source
 * topic</strong> — never the buffer store's changelog name — so topic-scoped serdes (e.g. Avro +
 * Schema Registry {@code TopicNameStrategy}) resolve the correct subject. This is the serde-resolution
 * half of buffer (de)serialisation; {@link ParsleySerializer} owns the wire format.
 *
 * @param <K> the record key type
 * @param <V> the record value type
 */
final class ParsleyResolver<K, V> {

    private final Function<String, Serde<K>> keySerdeByTopic;
    private final Function<String, Serde<V>> valueSerdeByTopic;

    ParsleyResolver(Function<String, Serde<K>> keySerdeByTopic,
                    Function<String, Serde<V>> valueSerdeByTopic) {
        this.keySerdeByTopic = keySerdeByTopic;
        this.valueSerdeByTopic = valueSerdeByTopic;
    }

    /**
     * Returns the key serde for {@code topic}.
     */
    Serde<K> keySerde(String topic) {
        return keySerdeByTopic.apply(topic);
    }

    /**
     * Returns the value serde for {@code topic}.
     */
    Serde<V> valueSerde(String topic) {
        return valueSerdeByTopic.apply(topic);
    }
}
