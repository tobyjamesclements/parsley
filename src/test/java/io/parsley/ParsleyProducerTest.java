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

    /**
     * {@code ParsleyProducer.send(record, deps)} attaches the provided
     * {@code CausalDependencies} as a wire-encoded header on the produced record.
     *
     * Asserts that the produced record carries exactly one record, the
     * causal-dependencies header is present, and it decodes back to the original deps.
     */
    @Test
    void sendAttachesTheCausalDependenciesHeader() {
        MockProducer<String, String> mock = mockProducer();
        try (ParsleyProducer<String, String> producer = new ParsleyProducer<>(mock)) {
            CausalDependencies deps = CausalDependencies.builder()
                    .require(new CausalPosition(CausalPosition.deriveUuid("t1"), 0, 3))
                    .build();
            producer.send(new ProducerRecord<>("t2", "k", "v"), deps);

            assertEquals(1, mock.history().size(), "exactly one record must be produced");
            ProducerRecord<String, String> sent = mock.history().get(0);
            Header header = sent.headers().lastHeader(ParsleyAttributes.CAUSAL_DEPENDENCIES);
            assertNotNull(header, "causal-dependencies header must be present");
            assertEquals(deps, CausalDependencies.fromBytes(header.value()),
                    "causal-dependencies header must encode the provided deps");
        }
    }

    /**
     * {@code ParsleyProducer.send(record)} with no explicit dependencies stamps an empty
     * causal-dependencies header on the produced record, so consumers can distinguish
     * Parsley-aware records from records with a missing header.
     *
     * Asserts that the causal-dependencies header is present and decodes to empty.
     */
    @Test
    void sendWithoutDependenciesStampsEmptyHeader() {
        MockProducer<String, String> mock = mockProducer();
        try (ParsleyProducer<String, String> producer = new ParsleyProducer<>(mock)) {
            producer.send(new ProducerRecord<>("t1", "k", "v"));

            ProducerRecord<String, String> sent = mock.history().get(0);
            Header header = sent.headers().lastHeader(ParsleyAttributes.CAUSAL_DEPENDENCIES);
            assertNotNull(header, "causal-dependencies header must be present even with no explicit dependencies");
            assertEquals(CausalDependencies.empty(), CausalDependencies.fromBytes(header.value()),
                    "causal-dependencies header must encode empty dependencies");
        }
    }

    /**
     * {@code ParsleyProducer.send(record, callback)} with no explicit dependencies stamps
     * an empty causal-dependencies header and invokes the callback without error.
     *
     * Asserts that the header is present and the callback receives no exception.
     */
    @Test
    void sendWithCallbackAndNoDependenciesStampsEmptyHeader() {
        MockProducer<String, String> mock = mockProducer();
        try (ParsleyProducer<String, String> producer = new ParsleyProducer<>(mock)) {
            AtomicReference<Exception> callbackError = new AtomicReference<>();
            producer.send(new ProducerRecord<>("t1", "k", "v"), (metadata, ex) -> callbackError.set(ex));

            ProducerRecord<String, String> sent = mock.history().get(0);
            Header header = sent.headers().lastHeader(ParsleyAttributes.CAUSAL_DEPENDENCIES);
            assertNotNull(header, "causal-dependencies header must be present");
            assertEquals(CausalDependencies.empty(), CausalDependencies.fromBytes(header.value()),
                    "causal-dependencies header must encode empty dependencies");
            assertEquals(null, callbackError.get(), "callback must be invoked without error");
        }
    }

    /**
     * {@code ParsleyProducer.send()} preserves the topic, key, value, and any user-provided
     * headers already on the record, adding only the causal-dependencies header.
     *
     * Asserts that the produced record retains the original topic, key, value, and
     * user-provided headers alongside the Parsley header.
     */
    @Test
    void sendPreservesExistingHeadersTopicKeyAndValue() {
        MockProducer<String, String> mock = mockProducer();
        try (ParsleyProducer<String, String> producer = new ParsleyProducer<>(mock)) {
            ProducerRecord<String, String> record = new ProducerRecord<>("t1", "k", "v");
            record.headers().add("trace-id", "abc".getBytes());

            producer.send(record, CausalDependencies.empty());

            ProducerRecord<String, String> sent = mock.history().get(0);
            assertEquals("t1", sent.topic(), "topic must be preserved");
            assertEquals("k", sent.key(), "key must be preserved");
            assertEquals("v", sent.value(), "value must be preserved");
            assertNotNull(sent.headers().lastHeader("trace-id"), "user-provided headers must be preserved");
        }
    }

    // --- helpers --------------------------------------------------------------------------------

    private static MockProducer<String, String> mockProducer() {
        return new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());
    }
}
