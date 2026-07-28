package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * The position-capture edge: the thread-local map the adapter reads, and the consumer proxy
 * that fills it after each poll. The proxy is driven with Kafka's {@code MockConsumer}; no
 * broker is involved.
 */
class PositionsTest {

    @AfterEach
    void clearCapturedPositions() {
        Positions.forCurrentThread().clear();
    }

    /** The map is mutable, stable across reads on one thread, and not visible to another. */
    @Test
    void capturedPositionsAreThreadLocal() throws InterruptedException {
        TopicPartition tp = new TopicPartition("t1", 0);
        Positions.forCurrentThread().put(tp, 42L);
        assertEquals(42L, Positions.forCurrentThread().get(tp),
                "the same thread must read back what it recorded");

        Long[] seenElsewhere = new Long[1];
        Thread other = new Thread(() -> seenElsewhere[0] = Positions.forCurrentThread().get(tp));
        other.start();
        other.join();
        assertNull(seenElsewhere[0], "another thread must see its own empty map");
    }

    /** After a poll returns, the proxy records the post-poll position of every assignment. */
    @Test
    void capturingProxyRecordsPositionsAfterPoll() {
        TopicPartition tp = new TopicPartition("t1", 0);
        MockConsumer<byte[], byte[]> inner = new MockConsumer<>("earliest");
        inner.assign(List.of(tp));
        inner.updateBeginningOffsets(Map.of(tp, 0L));
        inner.addRecord(new ConsumerRecord<>("t1", 0, 0L, new byte[0], new byte[0]));

        Consumer<byte[], byte[]> proxy = Positions.capturing(inner);
        var records = proxy.poll(Duration.ZERO);

        assertEquals(1, records.count(), "the proxy must pass the poll result through");
        assertEquals(1L, Positions.forCurrentThread().get(tp),
                "the position after consuming offset 0 must be captured as 1");
    }

    /** Non-poll calls pass through without touching the captured positions. */
    @Test
    void nonPollCallsDoNotCapture() {
        TopicPartition tp = new TopicPartition("t1", 0);
        MockConsumer<byte[], byte[]> inner = new MockConsumer<>("earliest");
        inner.assign(List.of(tp));
        inner.updateBeginningOffsets(Map.of(tp, 0L));

        Consumer<byte[], byte[]> proxy = Positions.capturing(inner);
        assertEquals(List.of(tp).size(), proxy.assignment().size(),
                "non-poll calls must delegate to the wrapped consumer");
        assertFalse(Positions.forCurrentThread().containsKey(tp),
                "only a completed poll may record positions");
    }

    /** The supplier builds all five client kinds; only the main consumer is wrapped. */
    @Test
    void supplierBuildsAllClientKinds() {
        KafkaClientSupplier supplier = Positions.capturingClientSupplier();
        Map<String, Object> config = new HashMap<>();
        config.put("bootstrap.servers", "localhost:9099");

        try (var admin = supplier.getAdmin(new HashMap<>(config))) {
            assertNotNull(admin, "the admin client must be built (KafkaStreams requires it)");
        }
        try (var producer = supplier.getProducer(new HashMap<>(config))) {
            assertNotNull(producer, "the producer must be built");
        }
        try (var consumer = supplier.getConsumer(new HashMap<>(config))) {
            assertNotNull(consumer, "the main consumer must be built (wrapped)");
        }
        try (var restore = supplier.getRestoreConsumer(new HashMap<>(config))) {
            assertNotNull(restore, "the restore consumer must be built");
        }
        try (var global = supplier.getGlobalConsumer(new HashMap<>(config))) {
            assertNotNull(global, "the global consumer must be built");
        }
    }
}
