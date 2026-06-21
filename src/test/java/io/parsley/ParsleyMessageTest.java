package io.parsley;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link ParsleyMessage}'s build/encode/decode helpers — the boundary that turns Kafka records
 * and headers into the typed engine envelope and back.
 */
class ParsleyMessageTest {

    private static final TopicPartition T1 = new TopicPartition("t1", 3);
    private static final Uuid T1_ID = Uuid.randomUuid();
    private static final Uuid DEP_ID = Uuid.randomUuid();

    /**
     * {@code from} decodes the {@code parsley-causal-dependencies} header into the typed clock and
     * keeps the user headers, dropping the dependency header from the user set.
     *
     * Asserts the coordinate, dependencies, and user headers are populated as expected.
     */
    @Test
    void fromDecodesDependenciesAndKeepsUserHeaders() {
        ParsleyClock deps = ParsleyClock.empty().observe(DEP_ID, 0, 5);
        Headers headers = ParsleyHeader.mutableHeaders();
        headers.add("user", "u".getBytes());
        headers.add(ParsleyHeader.CAUSAL_DEPENDENCIES, deps.toBytes());

        ParsleyMessage<String, String> message =
                ParsleyMessage.from(new Record<>("k", "v", 42L, headers), T1, 7L, T1_ID);

        assertEquals("t1", message.topic(), "topic must come from the source coordinate");
        assertEquals(T1_ID, message.topicId(), "topicId must come from the resolved UUID");
        assertEquals(3, message.partition(), "partition must come from the source coordinate");
        assertEquals(7L, message.offset(), "offset must come from the source coordinate");
        assertEquals(deps, message.dependencies(), "dependencies must be decoded from the header");
        assertEquals(List.of(new ParsleyHeader("user", "u".getBytes())).size(), message.headers().size(),
                "only the user header is carried");
        assertEquals("user", message.headers().get(0).key(), "the user header is preserved");
    }

    /**
     * {@code from} treats an absent dependency header as empty (vacuously satisfied) dependencies.
     *
     * Asserts the decoded dependencies are empty.
     */
    @Test
    void fromTreatsAbsentDependenciesAsEmpty() {
        ParsleyMessage<String, String> message =
                ParsleyMessage.from(new Record<>("k", "v", 0L, ParsleyHeader.mutableHeaders()), T1, 0L, T1_ID);
        assertTrue(message.dependencies().isEmpty(), "absent dependencies must decode as empty");
    }

    /**
     * {@code from} treats an undecodable dependency header as empty rather than failing — a
     * corrupt header must not deadlock or crash the gate.
     *
     * Asserts the decoded dependencies are empty.
     */
    @Test
    void fromTreatsGarbledDependenciesAsEmpty() {
        Headers headers = ParsleyHeader.mutableHeaders();
        headers.add(ParsleyHeader.CAUSAL_DEPENDENCIES, new byte[]{9, 9, 9});

        ParsleyMessage<String, String> message =
                ParsleyMessage.from(new Record<>("k", "v", 0L, headers), T1, 0L, T1_ID);
        assertTrue(message.dependencies().isEmpty(), "garbled dependencies must decode as empty");
    }

    /**
     * {@code toForwardHeaders} then {@code fromOutboxRecord} round-trips the source coordinate, the
     * dependency clock, and user headers through the outbox wire format.
     *
     * Asserts the decoded message equals the original coordinate, dependencies, and user headers,
     * and that no internal {@code _parsley_*} header leaks into the user-facing header set.
     */
    @Test
    void forwardHeadersRoundTripThroughTheOutbox() {
        ParsleyClock deps = ParsleyClock.empty().observe(DEP_ID, 0, 5);
        ParsleyMessage<byte[], byte[]> original = new ParsleyMessage<>(
                "t1", T1_ID, 3, 7L, 42L, "k".getBytes(), "v".getBytes(),
                List.of(new ParsleyHeader("user", "u".getBytes())), deps);

        ConsumerRecord<byte[], byte[]> wire = new ConsumerRecord<>(
                "outbox", 0, 0L, 42L, org.apache.kafka.common.record.TimestampType.CREATE_TIME,
                -1, -1, "k".getBytes(), "v".getBytes(), original.toForwardHeaders(), java.util.Optional.empty());

        ParsleyMessage<byte[], byte[]> decoded = ParsleyMessage.fromOutboxRecord(wire);

        assertEquals("t1", decoded.topic(), "topic must survive the outbox round-trip");
        assertEquals(T1_ID, decoded.topicId(), "topicId must survive the outbox round-trip");
        assertEquals(3, decoded.partition(), "partition must survive the outbox round-trip");
        assertEquals(7L, decoded.offset(), "offset must survive the outbox round-trip");
        assertEquals(deps, decoded.dependencies(), "dependencies must survive the outbox round-trip");
        assertEquals(1, decoded.headers().size(), "only the user header is carried back");
        assertEquals("user", decoded.headers().get(0).key(), "the user header survives");
        assertFalse(decoded.headers().stream().anyMatch(ParsleyHeader::isInternal),
                "no internal _parsley_* header may leak into the user header set");
    }
}
