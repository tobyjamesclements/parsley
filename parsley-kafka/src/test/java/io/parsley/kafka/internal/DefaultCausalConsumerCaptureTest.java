package io.parsley.kafka.internal;

import io.parsley.BufferLimit;
import io.parsley.BufferingPolicy;
import io.parsley.kafka.KafkaVectorClock;
import io.parsley.kafka.KafkaVectorClockSerialiser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for DefaultCausalConsumer's capture processor and poll plumbing. The consumer
 * is constructed against an unreachable broker (Kafka clients do not connect eagerly), and
 * the capture processor is driven directly with a MockProcessorContext.
 */
class DefaultCausalConsumerCaptureTest {

    private static final KafkaVectorClockSerialiser SERIALISER = new KafkaVectorClockSerialiser();
    private static final String INPUT = "input";

    private DefaultCausalConsumer<String, String> consumer;

    @AfterEach
    void closeConsumer() {
        if (consumer != null) {
            consumer.close();
        }
    }

    private DefaultCausalConsumer<String, String> consumer(Path stateDir) {
        Map<String, Object> streamsConfig = new HashMap<>();
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "capture-test-" + UUID.randomUUID());
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1");
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        consumer = new DefaultCausalConsumer<>(
                List.of(INPUT),
                BufferingPolicy.ignore(BufferLimit.ofDuration(Duration.ofSeconds(30))),
                Map.of(), streamsConfig);
        return consumer;
    }

    @Test
    void captureProcessorRebuildsConsumerRecordAndUpdatesFrontier(@TempDir Path stateDir) {
        var consumer = consumer(stateDir);
        KafkaVectorClock storedFrontier = new KafkaVectorClock(Map.of(new TopicPartition(INPUT, 0), 7L));
        TestKeyValueStore store = new TestKeyValueStore(CausalProcessor.FRONTIER_STORE);
        store.put(CausalProcessor.FRONTIER_KEY, SERIALISER.serialise(storedFrontier));

        MockProcessorContext<Void, Void> ctx = new MockProcessorContext<>();
        ctx.addStateStore(store);
        Processor<String, String, Void, Void> capture = consumer.captureSupplier().get();
        capture.init(ctx);

        // Two records: the first is taken by poll()'s blocking take, the second
        // exercises the drainTo path.
        capture.process(new Record<>("k", "v", 123L, sourceHeaders(6L)));
        capture.process(new Record<>("k2", "v2", 124L, sourceHeaders(7L)));

        ConsumerRecords<String, String> polled = consumer.poll(Duration.ofMillis(500));
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        polled.forEach(records::add);

        assertEquals(2, records.size());
        ConsumerRecord<String, String> record = records.getFirst();
        assertEquals(INPUT, record.topic());
        assertEquals(0, record.partition());
        assertEquals(6L, record.offset());
        assertEquals(123L, record.timestamp());
        assertEquals("k", record.key());
        assertEquals("v", record.value());
        assertEquals(7L, records.get(1).offset());
        assertEquals(storedFrontier, consumer.frontier());
    }

    private static RecordHeaders sourceHeaders(long offset) {
        RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader(CausalProcessor.SOURCE_TOPIC_HEADER,
                INPUT.getBytes(StandardCharsets.UTF_8)));
        headers.add(new RecordHeader(CausalProcessor.SOURCE_PARTITION_HEADER,
                ByteBuffer.allocate(4).putInt(0).array()));
        headers.add(new RecordHeader(CausalProcessor.SOURCE_OFFSET_HEADER,
                ByteBuffer.allocate(8).putLong(offset).array()));
        return headers;
    }

    @Test
    void fenceTokenWrapsCurrentFrontier(@TempDir Path stateDir) {
        var consumer = consumer(stateDir);
        assertEquals(KafkaVectorClock.empty(), consumer.frontier());
        assertEquals(consumer.frontier(), consumer.fenceToken().vectorClock());
    }

    @Test
    void pollOnEmptyQueueReturnsEmpty(@TempDir Path stateDir) {
        var consumer = consumer(stateDir);
        ConsumerRecords<String, String> polled = consumer.poll(Duration.ofMillis(50));
        assertNotNull(polled);
        assertEquals(0, polled.count());
    }

    @Test
    void headerExtractorsParseAndDefault() {
        RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader("s", "orders".getBytes(StandardCharsets.UTF_8)));
        headers.add(new RecordHeader("i", ByteBuffer.allocate(4).putInt(3).array()));
        headers.add(new RecordHeader("l", ByteBuffer.allocate(8).putLong(42L).array()));
        Record<String, String> record = new Record<>("k", "v", 0L, headers);

        assertEquals("orders", DefaultCausalConsumer.extractStringHeader(record, "s"));
        assertEquals(3, DefaultCausalConsumer.extractIntHeader(record, "i"));
        assertEquals(42L, DefaultCausalConsumer.extractLongHeader(record, "l"));

        assertEquals("", DefaultCausalConsumer.extractStringHeader(record, "missing"));
        assertEquals(0, DefaultCausalConsumer.extractIntHeader(record, "missing"));
        assertEquals(0L, DefaultCausalConsumer.extractLongHeader(record, "missing"));
    }
}
