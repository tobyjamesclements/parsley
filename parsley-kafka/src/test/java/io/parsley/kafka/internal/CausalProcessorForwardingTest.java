package io.parsley.kafka.internal;

import io.parsley.BufferLimit;
import io.parsley.BufferingPolicy;
import io.parsley.CausalViolationHandler;
import io.parsley.ParsleyAttributes;
import io.parsley.kafka.KafkaVectorClock;
import io.parsley.kafka.KafkaVectorClockSerialiser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.parsley.kafka.internal.CausalProcessor.FRONTIER_KEY;
import static io.parsley.kafka.internal.CausalProcessor.FRONTIER_STORE;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Pins two contracts of CausalProcessor's forwarding path:
 *
 * <ul>
 *   <li>every forwarded record carries the {@code parsley-src-*} enrichment headers that
 *       DefaultCausalConsumer relies on to rebuild ConsumerRecords, and
 *   <li>the frontier is persisted to the state store <em>before</em> the record that
 *       advanced it is forwarded (the crash-consistency invariant).
 * </ul>
 */
class CausalProcessorForwardingTest {

    private static final KafkaVectorClockSerialiser SERIALISER = new KafkaVectorClockSerialiser();
    private static final String INPUT = "input";
    private static final TopicPartition P0 = new TopicPartition(INPUT, 0);

    @Test
    void forwardedRecordsCarrySourceEnrichmentHeaders() {
        MockProcessorContext<String, String> ctx = new MockProcessorContext<>();
        ctx.addStateStore(new TestKeyValueStore(FRONTIER_STORE));
        CausalProcessor<String, String> processor = ignoreProcessor();
        processor.init(ctx);

        // Immediate-forward path.
        ctx.setRecordMetadata(INPUT, 0, 7);
        processor.process(record("direct", KafkaVectorClock.empty()));

        // Buffered-then-drained path: held until the frontier reaches input-0@7.
        // (Processed second so the drain releases it from the buffer.)
        ctx.setRecordMetadata(INPUT, 0, 8);
        processor.process(record("held", new KafkaVectorClock(Map.of(P0, 9L))));
        ctx.setRecordMetadata(INPUT, 0, 9);
        processor.process(record("advancer", KafkaVectorClock.empty()));

        var forwarded = ctx.forwarded();
        assertEquals(3, forwarded.size());
        assertSourceHeaders(forwarded.get(0).record(), INPUT, 0, 7);  // direct
        assertSourceHeaders(forwarded.get(1).record(), INPUT, 0, 9); // advancer
        assertSourceHeaders(forwarded.get(2).record(), INPUT, 0, 8); // held (drained)
        assertEquals("held", forwarded.get(2).record().key());
    }

    @Test
    void frontierPersistedBeforeEachForward() {
        List<Integer> forwardsSeenAtPersist = new ArrayList<>();
        MockProcessorContext<String, String> ctx = new MockProcessorContext<>();
        ctx.addStateStore(new TestKeyValueStore(FRONTIER_STORE, (key, value) -> {
            if (FRONTIER_KEY.equals(key)) {
                forwardsSeenAtPersist.add(ctx.forwarded().size());
            }
        }));
        CausalProcessor<String, String> processor = ignoreProcessor();
        processor.init(ctx);

        // One buffered record released by a later one — exercises both the direct-forward
        // and the drain persist sites.
        ctx.setRecordMetadata(INPUT, 0, 0);
        processor.process(record("held", new KafkaVectorClock(Map.of(P0, 1L))));
        ctx.setRecordMetadata(INPUT, 0, 1);
        processor.process(record("advancer", KafkaVectorClock.empty()));

        assertEquals(2, ctx.forwarded().size());
        // The i-th persist corresponds to the i-th forwarded record and must fire before
        // that record is forwarded: at persist time at most i prior forwards may have
        // happened. (The engine batches releases inside onRecord, so persists may cluster
        // ahead of the batch's forwards — observed counts can be LOWER than i, never
        // higher.) A forward-before-persist regression would show counts greater than i.
        assertEquals(2, forwardsSeenAtPersist.size());
        for (int i = 0; i < forwardsSeenAtPersist.size(); i++) {
            assertTrue(forwardsSeenAtPersist.get(i) <= i,
                    "persist #" + i + " happened after its record was forwarded ("
                            + forwardsSeenAtPersist.get(i) + " forwards already visible)");
        }
    }

    @Test
    void deadLetterSinkReceivesFaithfulConsumerRecord() {
        List<ConsumerRecord<String, String>> sink = new ArrayList<>();
        MockProcessorContext<String, String> ctx = new MockProcessorContext<>();
        ctx.addStateStore(new TestKeyValueStore(FRONTIER_STORE));
        CausalProcessor<String, String> processor = new CausalProcessor<>(
                BufferingPolicy.deadLetter(BufferLimit.ofSize(1), "dead"),
                CausalViolationHandler.noop(),
                SERIALISER,
                sink::add);
        processor.init(ctx);

        RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader("h1", new byte[]{42}));
        headers.add(new RecordHeader(ParsleyAttributes.VECTOR_CLOCK,
                SERIALISER.serialise(new KafkaVectorClock(Map.of(P0, 99L)))));
        ctx.setRecordMetadata(INPUT, 0, 5);
        processor.process(new Record<>("k", "v", 123L, headers));

        assertTrue(ctx.forwarded().isEmpty(), "dead-lettered record must not be forwarded");
        assertEquals(1, sink.size());
        ConsumerRecord<String, String> dead = sink.getFirst();
        assertEquals(INPUT, dead.topic());
        assertEquals(0, dead.partition());
        assertEquals(5L, dead.offset());
        assertEquals(123L, dead.timestamp());
        assertEquals("k", dead.key());
        assertEquals("v", dead.value());
        assertArrayEquals(new byte[]{42}, dead.headers().lastHeader("h1").value());
        assertNotNull(dead.headers().lastHeader(ParsleyAttributes.VECTOR_CLOCK),
                "vector-clock header must be preserved for diagnostics");
    }

    private static CausalProcessor<String, String> ignoreProcessor() {
        return new CausalProcessor<>(
                BufferingPolicy.ignore(BufferLimit.ofDuration(Duration.ofSeconds(30))),
                CausalViolationHandler.noop(),
                SERIALISER,
                null);
    }

    private static Record<String, String> record(String key, KafkaVectorClock clock) {
        RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader(ParsleyAttributes.VECTOR_CLOCK, SERIALISER.serialise(clock)));
        return new Record<>(key, "v", 0L, headers);
    }

    private static void assertSourceHeaders(Record<?, ?> record,
                                            String topic, int partition, long offset) {
        // Expected bytes computed independently of CausalProcessor's own helpers, so a
        // mutation of those helpers cannot also mutate the expectation.
        assertArrayEquals(topic.getBytes(StandardCharsets.UTF_8),
                header(record, CausalProcessor.SOURCE_TOPIC_HEADER));
        assertArrayEquals(java.nio.ByteBuffer.allocate(4).putInt(partition).array(),
                header(record, CausalProcessor.SOURCE_PARTITION_HEADER));
        assertArrayEquals(java.nio.ByteBuffer.allocate(8).putLong(offset).array(),
                header(record, CausalProcessor.SOURCE_OFFSET_HEADER));
    }

    private static byte[] header(Record<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        assertNotNull(header, "missing header " + name);
        return header.value();
    }

    @Test
    void durationLimitSchedulesPunctuatorThatEvicts() {
        MockProcessorContext<String, String> ctx = new MockProcessorContext<>();
        ctx.addStateStore(new TestKeyValueStore(FRONTIER_STORE));
        CausalProcessor<String, String> processor = ignoreProcessor();
        processor.init(ctx);

        var punctuators = ctx.scheduledPunctuators();
        assertEquals(1, punctuators.size(), "DurationLimit must schedule one punctuator");
        assertEquals(Duration.ofSeconds(30), punctuators.getFirst().getInterval());

        // Buffer an unsatisfiable record, then fire the punctuator: Ignore policy must
        // forward it out of order.
        ctx.setRecordMetadata(INPUT, 0, 0);
        processor.process(record("held", new KafkaVectorClock(Map.of(P0, 99L))));
        assertTrue(ctx.forwarded().isEmpty());

        punctuators.getFirst().getPunctuator().punctuate(0L);

        assertEquals(1, ctx.forwarded().size());
        assertEquals("held", ctx.forwarded().getFirst().record().key());
    }

    @Test
    void byteHelpersRoundTripAgainstIndependentEncoding() {
        assertArrayEquals(new byte[]{0, 0, 0, 7}, CausalProcessor.intToBytes(7));
        assertArrayEquals(new byte[]{0, 0, 0, 0, 0, 0, 1, 1}, CausalProcessor.longToBytes(257L));
        assertEquals(7, CausalProcessor.bytesToInt(new byte[]{0, 0, 0, 7}));
        assertEquals(257L, CausalProcessor.bytesToLong(new byte[]{0, 0, 0, 0, 0, 0, 1, 1}));
        assertEquals(Integer.MIN_VALUE, CausalProcessor.bytesToInt(CausalProcessor.intToBytes(Integer.MIN_VALUE)));
        assertEquals(Long.MIN_VALUE, CausalProcessor.bytesToLong(CausalProcessor.longToBytes(Long.MIN_VALUE)));
    }
}
