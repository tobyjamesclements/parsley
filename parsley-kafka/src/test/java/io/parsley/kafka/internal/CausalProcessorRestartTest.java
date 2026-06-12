package io.parsley.kafka.internal;

import io.parsley.BufferLimit;
import io.parsley.BufferingPolicy;
import io.parsley.CausalViolationReason;
import io.parsley.ParsleyAttributes;
import io.parsley.kafka.KafkaVectorClock;
import io.parsley.kafka.KafkaVectorClockSerialiser;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.parsley.kafka.internal.CausalProcessor.FRONTIER_KEY;
import static io.parsley.kafka.internal.CausalProcessor.FRONTIER_STORE;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that CausalProcessor persists the frontier to its state store on each advance,
 * and that a processor initialised with a pre-existing frontier value (as happens when Kafka
 * Streams restores a RocksDB state store after a restart) immediately satisfies causal
 * dependencies that were met before the restart.
 */
class CausalProcessorRestartTest {

    private static final KafkaVectorClockSerialiser SERIALISER = new KafkaVectorClockSerialiser();
    private static final String INPUT = "input";

    private final List<CausalViolationReason> violations = new ArrayList<>();

    @Test
    void frontierPersistedAndRestoredAcrossRestart() {
        TopicPartition p = new TopicPartition(INPUT, 0);

        // ---- Phase 1: pre-restart session ----
        TestKeyValueStore store1 = new TestKeyValueStore(FRONTIER_STORE);
        MockProcessorContext<String, String> ctx1 = new MockProcessorContext<>();
        ctx1.addStateStore(store1);
        CausalProcessor<String, String> proc1 = processor();
        proc1.init(ctx1);

        ctx1.setRecordMetadata(INPUT, 0, 0);
        proc1.process(record("R1", noop()));
        ctx1.setRecordMetadata(INPUT, 0, 1);
        proc1.process(record("R2", noop()));
        assertEquals(2, ctx1.forwarded().size());

        byte[] persisted = store1.get(FRONTIER_KEY);
        assertNotNull(persisted, "frontier must be persisted to state store after processing");

        // ---- Phase 2: post-restart session ----
        TestKeyValueStore store2 = new TestKeyValueStore(FRONTIER_STORE);
        store2.put(FRONTIER_KEY, persisted);

        MockProcessorContext<String, String> ctx2 = new MockProcessorContext<>();
        ctx2.addStateStore(store2);
        CausalProcessor<String, String> proc2 = processor();
        proc2.init(ctx2);

        // A: dep {input/0:1} is immediately satisfied by the restored frontier
        ctx2.setRecordMetadata(INPUT, 0, 0);
        proc2.process(record("R3", clock(Map.of(p, 1L))));
        assertEquals(1, ctx2.forwarded().size(), "restored frontier must satisfy dep {input/0:1} immediately");

        // B: dep {input/0:3} is not yet met — record must be buffered
        ctx2.setRecordMetadata(INPUT, 0, 1);
        proc2.process(record("R_held", clock(Map.of(p, 3L))));
        assertEquals(1, ctx2.forwarded().size(), "dep {input/0:3} not yet met — must buffer");

        ctx2.setRecordMetadata(INPUT, 0, 2);
        proc2.process(record("R_advance1", noop()));
        ctx2.setRecordMetadata(INPUT, 0, 3);
        proc2.process(record("R_advance2", noop()));

        var forwarded = ctx2.forwarded();
        assertEquals(4, forwarded.size());
        assertEquals("R3",         forwarded.get(0).record().key());
        assertEquals("R_advance1", forwarded.get(1).record().key());
        assertEquals("R_advance2", forwarded.get(2).record().key());
        assertEquals("R_held",     forwarded.get(3).record().key());
        assertTrue(violations.isEmpty());
    }

    private CausalProcessor<String, String> processor() {
        return new CausalProcessor<>(
                BufferingPolicy.ignore(BufferLimit.ofDuration(Duration.ofSeconds(30))),
                (rec, reason) -> violations.add(reason),
                SERIALISER,
                rec -> {});
    }

    private static Record<String, String> record(String key, KafkaVectorClock clock) {
        RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader(ParsleyAttributes.VECTOR_CLOCK, SERIALISER.serialise(clock)));
        return new Record<>(key, "v", 0L, headers);
    }

    private static KafkaVectorClock noop() { return KafkaVectorClock.empty(); }

    private static KafkaVectorClock clock(Map<TopicPartition, Long> positions) { return new KafkaVectorClock(positions); }
}
