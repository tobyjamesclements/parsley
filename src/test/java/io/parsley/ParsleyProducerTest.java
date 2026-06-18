package io.parsley;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class ParsleyProducerTest {

    private MockProducer<String, String> mock() {
        return new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());
    }

    @Test
    void sendAttachesTheCausalDependenciesHeader() {
        MockProducer<String, String> mock = mock();
        try (ParsleyProducer<String, String> producer = new ParsleyProducer<>(mock)) {
            CausalDependencies deps = CausalDependencies.builder().require(new CausalPosition(CausalPosition.deriveUuid("prices"), 0, 3)).build();
            producer.send(new ProducerRecord<>("orders", "k", "v"), deps);

            assertEquals(1, mock.history().size());
            ProducerRecord<String, String> sent = mock.history().get(0);
            Header header = sent.headers().lastHeader(ParsleyAttributes.CAUSAL_DEPENDENCIES);
            assertNotNull(header, "causal-dependencies header must be present");
            assertEquals(deps, CausalDependencies.fromBytes(header.value()));
        }
    }

    @Test
    void sendWithoutDependenciesStampsEmptyHeader() {
        MockProducer<String, String> mock = mock();
        try (ParsleyProducer<String, String> producer = new ParsleyProducer<>(mock)) {
            producer.send(new ProducerRecord<>("orders", "k", "v"));

            ProducerRecord<String, String> sent = mock.history().get(0);
            Header header = sent.headers().lastHeader(ParsleyAttributes.CAUSAL_DEPENDENCIES);
            assertNotNull(header, "causal-dependencies header must be present even with no explicit dependencies");
            assertEquals(CausalDependencies.empty(), CausalDependencies.fromBytes(header.value()));
        }
    }

    @Test
    void sendWithoutDependenciesWithCallbackStampsEmptyHeader() {
        MockProducer<String, String> mock = mock();
        try (ParsleyProducer<String, String> producer = new ParsleyProducer<>(mock)) {
            AtomicReference<Exception> callbackError = new AtomicReference<>();
            producer.send(new ProducerRecord<>("orders", "k", "v"), (metadata, ex) -> callbackError.set(ex));

            ProducerRecord<String, String> sent = mock.history().get(0);
            Header header = sent.headers().lastHeader(ParsleyAttributes.CAUSAL_DEPENDENCIES);
            assertNotNull(header, "causal-dependencies header must be present");
            assertEquals(CausalDependencies.empty(), CausalDependencies.fromBytes(header.value()));
            assertEquals(null, callbackError.get(), "callback must have been invoked without error");
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
