package io.parsley;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class ParsleyProducerTest {

    private MockProducer<String, String> mock() {
        return new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());
    }

    @Test
    void sendAttachesTheVectorClockHeader() {
        MockProducer<String, String> mock = mock();
        try (ParsleyProducer<String, String> producer = new ParsleyProducer<>(mock)) {
            CausalDependencies clock = CausalDependencies.empty().advance(CausalPosition.deriveUuid("prices"), 0, 3);
            producer.send(new ProducerRecord<>("orders", "k", "v"), clock);

            assertEquals(1, mock.history().size());
            ProducerRecord<String, String> sent = mock.history().get(0);
            Header header = sent.headers().lastHeader(ParsleyAttributes.VECTOR_CLOCK);
            assertNotNull(header, "vector-clock header must be present");
            assertEquals(clock, CausalDependencies.fromBytes(header.value()));
        }
    }

    @Test
    void sendPreservesExistingHeadersTopicKeyAndValue() {
        MockProducer<String, String> mock = mock();
        try (ParsleyProducer<String, String> producer = new ParsleyProducer<>(mock)) {
            ProducerRecord<String, String> record = new ProducerRecord<>("orders", "k", "v");
            record.headers().add("trace-id", "abc".getBytes());

            producer.send(record, CausalDependencies.empty());

            ProducerRecord<String, String> sent = mock.history().get(0);
            assertEquals("orders", sent.topic());
            assertEquals("k", sent.key());
            assertEquals("v", sent.value());
            assertNotNull(sent.headers().lastHeader("trace-id"));
        }
    }
}
