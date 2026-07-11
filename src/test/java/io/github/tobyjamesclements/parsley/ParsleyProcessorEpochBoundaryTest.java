package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.header.Header;
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
 * Tests {@link ParsleyProcessor}'s handling of a received {@link ParsleyHeader#EPOCH_BOUNDARY} marker:
 * it drives the local epoch transition but is never delivered to the user delegate or buffered, and it
 * relays the marker downstream on the same key (carrying this node's completeness) so the boundary
 * propagates edge by edge through the DAG and every task transitions its owned partitions.
 */
class ParsleyProcessorEpochBoundaryTest {

    private static final Uuid T1_ID = Uuid.randomUuid();
    // A foreign coordinate the marker's carried completeness advertises — distinct from t1 itself, so
    // the "genuine advance" the marker teaches is unambiguous.
    private static final Uuid UPSTREAM_ID = Uuid.randomUuid();
    private static final ParsleyTopicAdmin ADMIN = TestTopicAdmin.of(Map.of("t1", T1_ID));

    /**
     * An epoch-boundary control record drives the local transition and is relayed downstream: the user
     * delegate never sees it and it is not buffered, but the processor re-emits the marker on the same
     * key, carrying its completeness frontier so downstream channel clocks keep advancing across the
     * boundary. The injected marker carries a completeness header this task's channel does not already
     * know, so it genuinely advances the channel and the relay fires (clock-invisible markers: a marker
     * that taught nothing new would not relay — see {@link ParsleyProcessor}'s class Javadoc).
     *
     * Asserts the delegate never runs, nothing is buffered, and exactly one boundary marker (on the same
     * key, carrying a completeness header) is relayed.
     */
    @Test
    void epochBoundaryMarkerDrivesTheTransitionAndIsRelayedDownstream() {
        TestKeyValueStore<String, byte[]> frontierStore =
                new TestKeyValueStore<String, byte[]>(Comparator.naturalOrder(), "frontier");
        TestKeyValueStore<Long, byte[]> bufferStore =
                new TestKeyValueStore<Long, byte[]>(Comparator.naturalOrder(), "buffer");
        TestKeyValueStore<byte[], byte[]> candidateIndexStore =
                new TestKeyValueStore<byte[], byte[]>(Arrays::compareUnsigned, "candidate-index");
        TestKeyValueStore<byte[], byte[]> forwardedIndexStore =
                new TestKeyValueStore<byte[], byte[]>(Arrays::compareUnsigned, "forwarded-index");
        TestKeyValueStore<byte[], byte[]> orphanIndexStore =
                new TestKeyValueStore<byte[], byte[]>(Arrays::compareUnsigned, "orphan-index");

        List<String> processed = new ArrayList<>();
        Processor<String, String, String, String> delegate = new Processor<>() {
            @Override public void init(ProcessorContext<String, String> context) {}
            @Override public void process(Record<String, String> record) { processed.add(record.value()); }
        };
        ParsleySerializer<String, String> serializer =
                new ParsleySerializer<>(new ParsleyResolver<>(t -> Serdes.String(), t -> Serdes.String()));
        ParsleyProcessor<String, String, String, String> processor = new ParsleyProcessor<>(
                delegate, serializer,
                "frontier", "buffer", "candidate-index", "forwarded-index", "orphan-index",
                Set.of("t1"), Set.of(), List.of(), null,
                configs -> ADMIN, ParsleyConfig.from(new Properties()), CausalAudit.NOOP, null);

        MockProcessorContext<String, String> context = new MockProcessorContext<>();
        context.addStateStore(frontierStore);
        context.addStateStore(bufferStore);
        context.addStateStore(candidateIndexStore);
        context.addStateStore(forwardedIndexStore);
        context.addStateStore(orphanIndexStore);
        processor.init(context);

        // Inject an epoch-boundary marker on t1: a control record carrying the serialised ParsleyEpochBoundary,
        // a null value, and no business payload.
        context.setRecordMetadata("t1", 0, 0);
        ParsleyEpochBoundary boundary = new ParsleyEpochBoundary(1, ParsleyClock.empty().observe(T1_ID, 0, 10));
        Headers headers = ParsleyHeader.mutableHeaders();
        headers.add(ParsleyHeader.EPOCH_BOUNDARY, boundary.toBytes());
        // Carries completeness this task's t1 channel does not already know, so the marker genuinely
        // advances the channel and the relay fires.
        headers.add(ParsleyHeader.CAUSAL_DEPENDENCIES, ParsleyClock.empty().observe(UPSTREAM_ID, 0, 3).toBytes());
        processor.process(new Record<>("k", null, 0L, headers));

        assertTrue(processed.isEmpty(), "the epoch-boundary marker must never reach the user delegate");
        assertEquals(0, bufferStore.approximateNumEntries(), "the marker must never be buffered");

        List<? extends MockProcessorContext.CapturedForward<? extends String, ? extends String>> forwarded =
                context.forwarded();
        assertEquals(1, forwarded.size(), "the processor must relay exactly one downstream record for the marker");
        Record<? extends String, ? extends String> emitted = forwarded.get(0).record();
        assertTrue(hasHeader(emitted, ParsleyHeader.EPOCH_BOUNDARY),
                "the boundary marker must be relayed downstream so every task transitions its owned partitions");
        assertEquals("k", emitted.key(),
                "the relayed marker keeps the incoming key so it stays on the same partition lane");
        assertTrue(hasHeader(emitted, ParsleyHeader.CAUSAL_DEPENDENCIES),
                "the relayed marker carries this node's completeness so the downstream clock advances from it");
    }

    private static boolean hasHeader(Record<? extends String, ? extends String> record, String key) {
        for (Header h : record.headers()) {
            if (key.equals(h.key())) {
                return true;
            }
        }
        return false;
    }
}
