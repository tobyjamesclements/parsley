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
 * that fills it after each poll. The proxy is driven with Kafka's {@code MockConsumer}, with
 * no broker involved.
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
        Positions.forCurrentThread().put(tp, new Positions.PollView(42L, -1L));
        assertEquals(new Positions.PollView(42L, -1L), Positions.forCurrentThread().get(tp),
                "the same thread must read back what it recorded");

        Positions.PollView[] seenElsewhere = new Positions.PollView[1];
        Thread other = new Thread(() -> seenElsewhere[0] = Positions.forCurrentThread().get(tp));
        other.start();
        other.join();
        assertNull(seenElsewhere[0], "another thread must see its own empty map");
    }

    /** After a poll returns, the proxy records position and last returned offset per assignment. */
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
        assertEquals(new Positions.PollView(1L, 0L), Positions.forCurrentThread().get(tp),
                "consuming offset 0 must capture position 1 and last returned offset 0");
    }

    /** A batch captures its highest offset, so the sweep can tell buffered records apart. */
    @Test
    void captureRecordsTheBatchHighWater() {
        TopicPartition tp = new TopicPartition("t1", 0);
        MockConsumer<byte[], byte[]> inner = new MockConsumer<>("earliest");
        inner.assign(List.of(tp));
        inner.updateBeginningOffsets(Map.of(tp, 0L));
        inner.addRecord(new ConsumerRecord<>("t1", 0, 0L, new byte[0], new byte[0]));
        inner.addRecord(new ConsumerRecord<>("t1", 0, 1L, new byte[0], new byte[0]));
        inner.addRecord(new ConsumerRecord<>("t1", 0, 2L, new byte[0], new byte[0]));

        Consumer<byte[], byte[]> proxy = Positions.capturing(inner);
        assertEquals(3, proxy.poll(Duration.ZERO).count(), "the whole batch must pass through");

        assertEquals(new Positions.PollView(3L, 2L), Positions.forCurrentThread().get(tp),
                "a three-record batch must capture position 3 and last returned offset 2");
    }

    /** An empty poll advances the position but keeps the last returned offset. */
    @Test
    void emptyPollKeepsTheLastReturnedOffset() {
        TopicPartition tp = new TopicPartition("t1", 0);
        MockConsumer<byte[], byte[]> inner = new MockConsumer<>("earliest");
        inner.assign(List.of(tp));
        inner.updateBeginningOffsets(Map.of(tp, 0L));
        inner.addRecord(new ConsumerRecord<>("t1", 0, 0L, new byte[0], new byte[0]));

        Consumer<byte[], byte[]> proxy = Positions.capturing(inner);
        proxy.poll(Duration.ZERO);
        // The consumer position moves past a run the broker never returns as records
        // (transaction markers, aborted batches); the next poll comes back empty.
        inner.seek(tp, 6L);
        proxy.poll(Duration.ZERO);

        assertEquals(new Positions.PollView(6L, 0L), Positions.forCurrentThread().get(tp),
                "an empty poll must advance the position and keep last returned offset 0");
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

    /** The supplier builds all five client kinds, and only the main consumer is wrapped. */
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
