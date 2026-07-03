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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link ParsleyProcessor}'s handling of a received {@link ParsleyHeader#EPOCH_SNAPSHOT} marker:
 * it publishes the node's completeness frontier to the coordinator via
 * {@link ParsleyEpochSnapshotPublisher}, but is never delivered to the user delegate or buffered.
 */
class ParsleyProcessorEpochSnapshotTest {

    private static final Uuid T1_ID = Uuid.randomUuid();
    private static final ParsleyTopicAdmin ADMIN = TestTopicAdmin.of(Map.of("t1", T1_ID));

    /** A publisher double that records every (memberId, completeness) publication. */
    private static final class RecordingPublisher implements ParsleyEpochSnapshotPublisher {
        final List<String> members = new ArrayList<>();
        final List<ParsleyClock> clocks = new ArrayList<>();

        @Override
        public void publish(String memberId, ParsleyClock completeness) {
            members.add(memberId);
            clocks.add(completeness);
        }
    }

    /**
     * An epoch-snapshot marker publishes this node's current completeness frontier (tagged with the
     * task id) and nothing else: the user delegate never sees it, it is not buffered, and a completeness
     * watermark is re-emitted so downstream progress is not stalled by the consumed marker.
     *
     * Asserts the publisher is called once with the post-delivery completeness (T1@5), the delegate saw
     * only the business record, and a watermark (not the snapshot marker) is forwarded.
     */
    @Test
    void epochSnapshotMarkerPublishesCompletenessButIsNeverDelivered() {
        TestKeyValueStore<String, byte[]> frontierStore =
                new TestKeyValueStore<String, byte[]>(Comparator.naturalOrder(), "frontier");
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
        RecordingPublisher publisher = new RecordingPublisher();
        ParsleyProcessor<String, String, String, String> processor = new ParsleyProcessor<>(
                delegate, CausalBufferLimit.ofSize(100), serializer,
                "frontier", "buffer", "candidate-index", "forwarded-index", Set.of("t1"), Set.of(),
                configs -> ADMIN, ParsleyConfig.from(new Properties()), CausalAudit.NOOP, null, publisher);

        MockProcessorContext<String, String> context = new MockProcessorContext<>();
        context.addStateStore(frontierStore);
        context.addStateStore(bufferStore);
        context.addStateStore(candidateIndexStore);
        context.addStateStore(forwardedIndexStore);
        processor.init(context);

        // Deliver a business record so this node's completeness advances to T1@5.
        context.setRecordMetadata("t1", 0, 5);
        Headers deps = ParsleyHeader.mutableHeaders();
        deps.add(ParsleyHeader.CAUSAL_DEPENDENCIES, ParsleyClock.empty().toBytes());
        processor.process(new Record<>("k", "v", 0L, deps));
        assertEquals(List.of("v"), processed, "the business record must be delivered to the delegate");

        // Inject the snapshot marker: a control record with no business payload.
        context.setRecordMetadata("t1", 0, 6);
        Headers snapshot = ParsleyHeader.mutableHeaders();
        snapshot.add(ParsleyHeader.EPOCH_SNAPSHOT, new byte[0]);
        processor.process(new Record<>("k", null, 0L, snapshot));

        assertEquals(List.of("v"), processed, "the snapshot marker must never reach the user delegate");
        assertEquals(0, bufferStore.approximateNumEntries(), "the marker must never be buffered");
        assertEquals(1, publisher.clocks.size(), "the marker must trigger exactly one frontier publication");
        assertFalse(publisher.members.get(0).isEmpty(), "the publication must be tagged with the task/member id");
        assertEquals(5L, publisher.clocks.get(0).offsetFor(T1_ID, 0),
                "the published clock must be this node's completeness frontier at the snapshot point (T1@5)");

        List<? extends MockProcessorContext.CapturedForward<? extends String, ? extends String>> forwarded =
                context.forwarded();
        assertTrue(forwarded.stream().anyMatch(f -> hasHeader(f.record(), ParsleyHeader.WATERMARK)),
                "a completeness watermark must be re-emitted so the consumed marker does not stall downstream");
        assertTrue(forwarded.stream().noneMatch(f -> hasHeader(f.record(), ParsleyHeader.EPOCH_SNAPSHOT)),
                "the snapshot marker itself must never be re-emitted — the coordinator broadcasts it to every channel");
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
