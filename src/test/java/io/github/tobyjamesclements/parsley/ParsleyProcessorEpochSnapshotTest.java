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

/**
 * Tests {@link ParsleyProcessor}'s handling of a received {@link ParsleyHeader#EPOCH_SNAPSHOT} marker:
 * it publishes the node's completeness frontier to the coordinator via
 * {@link ParsleyEpochSnapshotPublisher}, but is never delivered to the user delegate or buffered.
 */
class ParsleyProcessorEpochSnapshotTest {

    private static final Uuid T1_ID = Uuid.randomUuid();
    // A foreign coordinate the marker's carried completeness advertises — distinct from t1 itself, so
    // the "genuine advance" the marker teaches is unambiguous.
    private static final Uuid UPSTREAM_ID = Uuid.randomUuid();
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
     * task id) and is relayed downstream: the user delegate never sees it, it is not buffered, but the
     * marker is re-emitted on the same key so the in-band cut propagates through the DAG, carrying this
     * node's completeness so the downstream channel clock advances from the same record. The injected
     * marker carries a completeness header this task's channel does not already know, so it genuinely
     * advances the channel and the relay fires (clock-invisible markers: a marker that taught nothing
     * new would not relay — see {@link ParsleyProcessor}'s class Javadoc).
     *
     * Asserts the publisher is called once with the post-delivery, post-commit completeness (T1@5 —
     * publications carry only committed state, so the test runs two commit-cycle flushes first), the
     * delegate saw only the business record, and the snapshot marker is relayed on the same key
     * carrying this node's live completeness (the in-band stamp rides the task's own transaction, so
     * it stays live, unlike the side-channel publication).
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
                delegate, serializer,
                "frontier", "buffer", "candidate-index", "forwarded-index",
                Set.of("t1"), Set.of(), Set.of(), List.of(),
                configs -> ADMIN, ParsleyConfig.from(new Properties()), null, publisher);

        MockProcessorContext<String, String> context = new MockProcessorContext<>();
        context.setCurrentSystemTimeMs(1L);
        context.addStateStore(frontierStore);
        context.addStateStore(bufferStore);
        context.addStateStore(candidateIndexStore);
        context.addStateStore(forwardedIndexStore);
        ParsleyCommittedCompleteness commitHook = new ParsleyCommittedCompleteness("frontier-commit-hook");
        context.addStateStore(commitHook);
        processor.init(context);

        // Deliver a business record so this node's completeness advances to T1@5.
        context.setRecordMetadata("t1", 0, 5);
        Headers deps = ParsleyHeader.mutableHeaders();
        deps.add(ParsleyHeader.CAUSAL_DEPENDENCIES, ParsleyClock.empty().toBytes());
        processor.process(new Record<>("k", "v", 0L, deps));
        assertEquals(List.of("v"), processed, "the business record must be delivered to the delegate");

        // Two commit cycles, so the delivery's transaction is provably committed and its completeness
        // becomes publishable — side-channel publications read only committed state, never the live
        // clock (see ParsleyCommittedCompleteness).
        commitHook.flush();
        commitHook.flush();

        // Inject the snapshot marker: a control record with no business payload, carrying completeness
        // this task's t1 channel does not already know, so it genuinely advances the channel.
        context.setRecordMetadata("t1", 0, 6);
        Headers snapshot = ParsleyHeader.mutableHeaders();
        snapshot.add(ParsleyHeader.EPOCH_SNAPSHOT, new byte[0]);
        snapshot.add(ParsleyHeader.CAUSAL_DEPENDENCIES, ParsleyClock.empty().observe(UPSTREAM_ID, 0, 3).toBytes());
        processor.process(new Record<>("k", null, 0L, snapshot));

        assertEquals(List.of("v"), processed, "the snapshot marker must never reach the user delegate");
        assertEquals(0, bufferStore.approximateNumEntries(), "the marker must never be buffered");
        assertEquals(1, publisher.clocks.size(), "the marker must trigger exactly one frontier publication");
        assertFalse(publisher.members.get(0).isEmpty(), "the publication must be tagged with the task/member id");
        assertEquals(5L, publisher.clocks.get(0).offsetFor(T1_ID, 0),
                "the published clock must be this node's committed completeness (T1@5, the delivery "
                        + "whose transaction the two flushes proved committed) — never the live clock");

        List<? extends MockProcessorContext.CapturedForward<? extends String, ? extends String>> forwarded =
                context.forwarded();
        List<? extends MockProcessorContext.CapturedForward<? extends String, ? extends String>> relayed =
                forwarded.stream().filter(f -> hasHeader(f.record(), ParsleyHeader.EPOCH_SNAPSHOT)).toList();
        assertEquals(1, relayed.size(), "the snapshot marker must be relayed downstream exactly once");
        assertEquals("k", relayed.get(0).record().key(),
                "the relayed marker keeps the incoming key so it stays on the same partition lane");
        assertEquals(6L, markerCompleteness(relayed.get(0).record()).offsetFor(T1_ID, 0),
                "the relayed marker carries this node's completeness including its own position (T1@6, "
                        + "not merely T1@5) — a marker's own arrival is delivered exactly like a business "
                        + "record's own coordinate, so a marker-only channel's frontier still advances");
    }

    /** Decodes the completeness clock a relayed marker carries in its causal-dependencies header. */
    private static ParsleyClock markerCompleteness(Record<? extends String, ? extends String> record) {
        for (Header h : record.headers()) {
            if (ParsleyHeader.CAUSAL_DEPENDENCIES.equals(h.key()) && h.value() != null) {
                return ParsleyClock.fromBytes(h.value());
            }
        }
        throw new AssertionError("relayed marker carried no completeness header");
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
