package io.parsley.producer;

import io.parsley.VectorClock;
import io.parsley.internal.Attributes;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class KafkaCausalProducerTest {

    private static final TopicPartition PRICES = new TopicPartition("prices", 0);

    private MockProducer<String, String> mock() {
        return new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());
    }

    @Test
    void sendAttachesTheVectorClockHeader() {
        MockProducer<String, String> mock = mock();
        try (KafkaCausalProducer<String, String> producer = new KafkaCausalProducer<>(mock)) {
            VectorClock clock = VectorClock.empty().advance(PRICES, 3);
            producer.send(new ProducerRecord<>("orders", "k", "v"), clock);

            assertEquals(1, mock.history().size());
            ProducerRecord<String, String> sent = mock.history().get(0);
            Header header = sent.headers().lastHeader(Attributes.VECTOR_CLOCK);
            assertNotNull(header, "vector-clock header must be present");
            assertEquals(clock, VectorClock.fromBytes(header.value()));
        }
    }

    @Test
    void sendPreservesExistingHeadersTopicKeyAndValue() {
        MockProducer<String, String> mock = mock();
        try (KafkaCausalProducer<String, String> producer = new KafkaCausalProducer<>(mock)) {
            ProducerRecord<String, String> record = new ProducerRecord<>("orders", "k", "v");
            record.headers().add("trace-id", "abc".getBytes());

            producer.send(record, VectorClock.empty());

            ProducerRecord<String, String> sent = mock.history().get(0);
            assertEquals("orders", sent.topic());
            assertEquals("k", sent.key());
            assertEquals("v", sent.value());
            assertNotNull(sent.headers().lastHeader("trace-id"));
        }
    }
}
