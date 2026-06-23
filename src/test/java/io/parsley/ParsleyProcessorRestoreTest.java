package io.parsley;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link ParsleyProcessor#init} directly via a {@link MockProcessorContext} backed by
 * {@link TestKeyValueStore}, in particular the branch that restores a previously persisted
 * frontier from the frontier state store (the restart path). This is exercised here as a unit
 * test against the processor itself, distinct from {@link CausalProcessorsTopologyTest}'s
 * full-topology {@code TopologyTestDriver} tests — a {@code TopologyTestDriver} does not restore
 * persistent state across separate driver instances, so the restored-frontier branch can only be
 * driven by handing {@code init()} a frontier store that already holds a value.
 */
class ParsleyProcessorRestoreTest {

    private static final Uuid T1_ID = Uuid.randomUuid();
    private static final Uuid OTHER_ID = Uuid.randomUuid();

    private static final ParsleyTopicAdmin ADMIN = TestTopicAdmin.of(Map.of("t1", T1_ID));

    /**
     * When the frontier store already holds a persisted frontier at {@code init()} (as after a
     * restart), the processor restores it — reported via {@link CausalAudit#processorInitialized}
     * — and immediately governs admission: a record whose only dependency is exactly satisfied by
     * the restored frontier is forwarded right away rather than buffered.
     *
     * Asserts {@code processorInitialized} reports {@code frontierRestored = true}, and that the
     * restored frontier is live: the delegate runs immediately and the buffer store stays empty.
     */
    @Test
    void initRestoresAPersistedFrontierAndItGatesAdmissionImmediately() {
        ParsleyClock restoredFrontier = ParsleyClock.empty().observe(OTHER_ID, 0, 5);
        TestKeyValueStore<String, byte[]> frontierStore =
                new TestKeyValueStore<String, byte[]>(Comparator.naturalOrder(), "frontier");
        frontierStore.put(ParsleyStores.FRONTIER_KEY, restoredFrontier.toBytes());
        TestKeyValueStore<Long, byte[]> bufferStore =
                new TestKeyValueStore<Long, byte[]>(Comparator.naturalOrder(), "buffer");
        TestKeyValueStore<byte[], byte[]> candidateIndexStore =
                new TestKeyValueStore<byte[], byte[]>(Arrays::compareUnsigned, "candidate-index");

        RecordingCausalAudit audit = new RecordingCausalAudit();
        List<String> processed = new ArrayList<>();
        Processor<String, String, String, String> delegate = new Processor<>() {
            @Override public void init(ProcessorContext<String, String> context) {}
            @Override public void process(Record<String, String> record) { processed.add(record.value()); }
        };
        ParsleySerializer<String, String> serializer =
                new ParsleySerializer<>(new ParsleyResolver<>(t -> Serdes.String(), t -> Serdes.String()));
        ParsleyProcessor<String, String, String, String> processor = new ParsleyProcessor<>(
                delegate, CausalBufferLimit.ofSize(100), serializer,
                "frontier", "buffer", "candidate-index", Set.of("t1"),
                configs -> ADMIN, ParsleyConfig.from(new Properties()), audit);

        MockProcessorContext<String, String> context = new MockProcessorContext<>();
        context.addStateStore(frontierStore);
        context.addStateStore(bufferStore);
        context.addStateStore(candidateIndexStore);

        processor.init(context);

        assertEquals(1, audit.initializations.size(), "processorInitialized must fire once during init()");
        assertTrue(audit.initializations.get(0).frontierRestored(),
                "a non-empty frontier store must be reported as restored, not a fresh start");

        // A record whose only dependency (OTHER_ID/0@5) is exactly satisfied by the restored
        // frontier — it must be forwarded immediately rather than buffered, which would not be
        // possible if init() had started the engine from an empty frontier.
        context.setRecordMetadata("t1", 0, 0);
        Headers headers = ParsleyHeader.mutableHeaders();
        headers.add(ParsleyHeader.CAUSAL_DEPENDENCIES, restoredFrontier.toBytes());
        processor.process(new Record<>("k", "v", 0L, headers));

        assertEquals(List.of("v"), processed,
                "the delegate must run immediately: the restored frontier already satisfies the dependency");
        assertEquals(0, bufferStore.approximateNumEntries(),
                "the record must never have entered the buffer store");
    }
}
