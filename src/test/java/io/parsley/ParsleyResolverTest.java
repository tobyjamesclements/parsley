package io.parsley;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertSame;

class ParsleyResolverTest {

    @Test
    void resolvesKeyAndValueSerdesByTopic() {
        Serde<String> ordersKey = Serdes.String();
        Serde<String> defaultKey = Serdes.String();
        Serde<String> ordersValue = Serdes.String();
        Serde<String> defaultValue = Serdes.String();

        ParsleyResolver<String, String> resolver = new ParsleyResolver<>(
                topic -> topic.equals("orders") ? ordersKey : defaultKey,
                topic -> topic.equals("orders") ? ordersValue : defaultValue);

        assertSame(ordersKey, resolver.keySerde("orders"));
        assertSame(defaultKey, resolver.keySerde("prices"));
        assertSame(ordersValue, resolver.valueSerde("orders"));
        assertSame(defaultValue, resolver.valueSerde("prices"));
    }
}
