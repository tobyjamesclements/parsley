package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertSame;

class ParsleyResolverTest {

    /**
     * {@code ParsleyResolver} delegates key and value serde resolution to the provided
     * factory functions, passing the topic name as the selector. Different topics can be
     * mapped to different serdes.
     *
     * Asserts that the resolver returns the per-topic serde for a known topic and the
     * default serde for any other topic, for both key and value.
     */
    @Test
    void resolvesKeyAndValueSerdesByTopic() {
        Serde<String> t1Key = Serdes.String();
        Serde<String> defaultKey = Serdes.String();
        Serde<String> t1Value = Serdes.String();
        Serde<String> defaultValue = Serdes.String();

        ParsleyResolver<String, String> resolver = new ParsleyResolver<>(
                topic -> topic.equals("t1") ? t1Key : defaultKey,
                topic -> topic.equals("t1") ? t1Value : defaultValue);

        assertSame(t1Key, resolver.keySerde("t1"), "resolver must return the per-topic key serde for t1");
        assertSame(defaultKey, resolver.keySerde("t2"), "resolver must return the default key serde for other topics");
        assertSame(t1Value, resolver.valueSerde("t1"), "resolver must return the per-topic value serde for t1");
        assertSame(defaultValue, resolver.valueSerde("t2"), "resolver must return the default value serde for other topics");
    }
}
