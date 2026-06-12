package io.parsley.kafka.internal;

import io.parsley.FenceToken;
import io.parsley.ParsleyAttributes;
import io.parsley.kafka.CausalProducer;
import io.parsley.kafka.KafkaVectorClock;
import io.parsley.kafka.KafkaVectorClockSerialiser;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class DefaultCausalProducerTest {

    private static final KafkaVectorClockSerialiser SERIALISER = new KafkaVectorClockSerialiser();
    private static final TopicPartition P0 = new TopicPartition("upstream", 0);

    /** The producer only reads {@link FenceToken#vectorClock()}; encoding is irrelevant here. */
    private record TestToken(KafkaVectorClock vectorClock) implements FenceToken<KafkaVectorClock> {
        @Override
        public String encode() {
            throw new UnsupportedOperationException();
        }
    }

    private static MockProducer<String, String> mockProducer() {
        return new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());
    }

    @Test
    void sendAttachesSerialisedClockHeader() {
        MockProducer<String, String> mock = mockProducer();
        DefaultCausalProducer<String, String> producer = new DefaultCausalProducer<>(mock);
        KafkaVectorClock clock = new KafkaVectorClock(Map.of(P0, 5L));

        var future = producer.send(new ProducerRecord<>("orders", "k", "v"), new TestToken(clock));

        assertNotNull(future);
        assertDoesNotThrow(() -> assertNotNull(future.get()));
        assertEquals(1, mock.history().size());
        ProducerRecord<String, String> sent = mock.history().getFirst();
        assertEquals("orders", sent.topic());
        assertEquals("k", sent.key());
        assertEquals("v", sent.value());
        Header header = sent.headers().lastHeader(ParsleyAttributes.VECTOR_CLOCK);
        assertNotNull(header, "parsley-vector-clock header must be attached");
        assertEquals(clock, SERIALISER.deserialise(header.value()));
    }

    @Test
    void sendWithCallbackDelegatesAndFires() throws Exception {
        MockProducer<String, String> mock = mockProducer();
        DefaultCausalProducer<String, String> producer = new DefaultCausalProducer<>(mock);
        CountDownLatch fired = new CountDownLatch(1);

        var future = producer.send(new ProducerRecord<>("orders", "k", "v"),
                new TestToken(KafkaVectorClock.empty()),
                (metadata, exception) -> fired.countDown());

        assertNotNull(future);
        assertTrue(fired.await(5, TimeUnit.SECONDS), "callback must fire");
        assertEquals(1, mock.history().size());
        assertNotNull(mock.history().getFirst().headers().lastHeader(ParsleyAttributes.VECTOR_CLOCK));
    }

    @Test
    void closeClosesDelegate() {
        MockProducer<String, String> mock = mockProducer();
        new DefaultCausalProducer<>(mock).close();
        assertTrue(mock.closed());
    }

    @Test
    void createConstructsWithoutABroker() {
        // KafkaProducer construction does not connect eagerly; an unreachable
        // bootstrap address is sufficient for the factory path.
        CausalProducer<String, String> producer = CausalProducer.create(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()));
        assertNotNull(producer);
        producer.close();
    }
}
