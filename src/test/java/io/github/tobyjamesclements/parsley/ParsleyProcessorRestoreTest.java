package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests {@link ParsleyProcessor#init} directly via a {@link MockProcessorContext} backed by
 * {@link TestKeyValueStore}, in particular the branch that restores a previously persisted
 * frontier from the frontier state store (the restart path). This is exercised here as a unit
 * test against the processor itself, distinct from {@link ParsleyProcessorsTopologyTest}'s
 * full-topology {@code TopologyTestDriver} tests — a {@code TopologyTestDriver} does not restore
 * persistent state across separate driver instances, so the restored-frontier branch can only be
 * driven by handing {@code init()} a frontier store that already holds a value.
 */
class ParsleyProcessorRestoreTest {

    private static final Uuid C1_ID = Uuid.randomUuid();
    private static final Uuid OTHER_ID = Uuid.randomUuid();

    private static final ParsleyTopicAdmin ADMIN = TestTopicAdmin.of(Map.of("c1", C1_ID));

    /**
     * When the frontier store already holds a persisted frontier at {@code init()} (as after a
     * restart), the processor restores it and immediately governs admission: a record whose only
     * dependency is exactly satisfied by the restored frontier is forwarded right away rather than
     * buffered.
     *
     * Asserts the restored frontier is live: the delegate runs immediately and the buffer store
     * stays empty.
     */
    @Test
    void initRestoresAPersistedFrontierAndItGatesAdmissionImmediately() {
        // Use an in-scope coordinate (C1_ID) so the restored frontier is not pruned away and
        // genuinely gates admission — the dep is satisfied by the frontier, not vacuously.
        ParsleyVectorClock restoredFrontier = ParsleyVectorClock.empty().observe(C1_ID, 0, 5);
        TestKeyValueStore<String, byte[]> frontierStore =
                new TestKeyValueStore<String, byte[]>(Comparator.naturalOrder(), "frontier");
        frontierStore.put(ParsleyStores.FRONTIER_KEY, frontierBlob(restoredFrontier));
        TestKeyValueStore<Long, byte[]> bufferStore =
                new TestKeyValueStore<Long, byte[]>(Comparator.naturalOrder(), "buffer");
        TestKeyValueStore<byte[], byte[]> candidateIndexStore =
                new TestKeyValueStore<byte[], byte[]>(Arrays::compareUnsigned, "candidate-index");
        TestKeyValueStore<byte[], byte[]> forwardedIndexStore =
                new TestKeyValueStore<byte[], byte[]>(Arrays::compareUnsigned, "forwarded-index");

        List<String> processed = new ArrayList<>();
        Processor<String, String, String, String> delegate = new Processor<>() {
            @Override public void init(ProcessorContext<String, String> context) {}
            @Override public void process(Record<String, String> record) { processed.add(record.value()); }
        };
        ParsleySerializer<String, String> serializer =
                new ParsleySerializer<>(new ParsleyResolver<>(t -> Serdes.String(), t -> Serdes.String()));
        ParsleyProcessor<String, String, String, String> processor = new ParsleyProcessor<>(
                delegate, serializer,
                "frontier", "buffer", "candidate-index", "forwarded-index",
                Set.of("c1"), Set.of(), List.of(),
                configs -> ADMIN, null);

        MockProcessorContext<String, String> context = new MockProcessorContext<>();
        context.setCurrentSystemTimeMs(1L);
        context.addStateStore(frontierStore);
        context.addStateStore(bufferStore);
        context.addStateStore(candidateIndexStore);
        context.addStateStore(forwardedIndexStore);

        processor.init(context);

        // A record whose only dependency (C1_ID/0@5) is exactly satisfied by the restored frontier
        // — it must be forwarded immediately rather than buffered, which would not be possible if
        // init() had started the core from an empty frontier. Offset 10 avoids the self-ref strip.
        context.setRecordMetadata("c1", 0, 10);
        Headers headers = ParsleyHeader.mutableHeaders();
        headers.add(ParsleyHeader.CAUSAL_CLOCK, restoredFrontier.toBytes());
        processor.process(new Record<>("k", "v", 0L, headers));

        assertEquals(List.of("v"), processed,
                "the delegate must run immediately: the restored frontier already satisfies the dependency");
        assertEquals(0, bufferStore.approximateNumEntries(),
                "the record must never have entered the buffer store");
    }

    // Builds the combined ParsleyChannels "f" blob for a frontier clock with no channel clocks:
    // [frontier-len:4][frontier bytes][channel-count:4 = 0]. Mirrors ParsleyChannels#toBytes so a
    // restored frontier can be seeded into the frontier store.
    private static byte[] frontierBlob(ParsleyVectorClock frontier) {
        try (java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
             java.io.DataOutputStream dos = new java.io.DataOutputStream(baos)) {
            byte[] f = frontier.toBytes();
            dos.writeInt(f.length);
            dos.write(f);
            dos.writeInt(0);
            dos.flush();
            return baos.toByteArray();
        } catch (java.io.IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
