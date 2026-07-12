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
import static org.junit.jupiter.api.Assertions.assertThrows;
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
                Set.of("t1"), Set.of(), List.of(),
                configs -> ADMIN, ParsleyConfig.from(new Properties()), null);

        MockProcessorContext<String, String> context = new MockProcessorContext<>();
        context.setCurrentSystemTimeMs(1L);
        context.addStateStore(frontierStore);
        context.addStateStore(bufferStore);
        context.addStateStore(candidateIndexStore);
        context.addStateStore(forwardedIndexStore);
        context.addStateStore(new ParsleyCommittedCompleteness("frontier-commit-hook"));
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
        assertEquals(1L, emitted.timestamp(),
                "the relayed marker must carry the current wall-clock time, never 0L — a 0L-timestamped "
                        + "marker segment looks expired to broker time-based retention the moment it rolls");
    }

    /**
     * A user-delegate failure while delivering a record released by a marker's carried completeness
     * must fail the task (fail-closed), never be swallowed: by the time the delegate runs, the
     * released record has already left the buffer and advanced the frontier, so swallowing the
     * exception would let the task commit past a record the delegate never processed — silently
     * losing it. Regression test for the over-broad catch in the marker channel-clock path that
     * logged the delegate's exception as "Failed to decode marker completeness" and carried on.
     *
     * Asserts the delegate's exception propagates out of {@code process()} once the boundary marker's
     * own delivery releases the held business record, and that the release had already removed the
     * record from the buffer — the state that makes swallowing the failure a permanent loss.
     */
    @Test
    void delegateFailureDuringMarkerTriggeredReleaseFailsTheTaskInsteadOfBeingSwallowed() {
        Uuid t2Id = Uuid.randomUuid();
        ParsleyTopicAdmin admin = TestTopicAdmin.of(Map.of("t1", T1_ID, "t2", t2Id));
        TestKeyValueStore<String, byte[]> frontierStore =
                new TestKeyValueStore<String, byte[]>(Comparator.naturalOrder(), "frontier");
        TestKeyValueStore<Long, byte[]> bufferStore =
                new TestKeyValueStore<Long, byte[]>(Comparator.naturalOrder(), "buffer");
        TestKeyValueStore<byte[], byte[]> candidateIndexStore =
                new TestKeyValueStore<byte[], byte[]>(Arrays::compareUnsigned, "candidate-index");
        TestKeyValueStore<byte[], byte[]> forwardedIndexStore =
                new TestKeyValueStore<byte[], byte[]>(Arrays::compareUnsigned, "forwarded-index");

        RuntimeException boom = new RuntimeException("delegate failure during marker-triggered release");
        Processor<String, String, String, String> delegate = new Processor<>() {
            @Override public void init(ProcessorContext<String, String> context) {}
            @Override public void process(Record<String, String> record) { throw boom; }
        };
        ParsleySerializer<String, String> serializer =
                new ParsleySerializer<>(new ParsleyResolver<>(t -> Serdes.String(), t -> Serdes.String()));
        ParsleyProcessor<String, String, String, String> processor = new ParsleyProcessor<>(
                delegate, serializer,
                "frontier", "buffer", "candidate-index", "forwarded-index",
                Set.of("t1", "t2"), Set.of(), List.of(),
                configs -> admin, ParsleyConfig.from(new Properties()), null);

        MockProcessorContext<String, String> context = new MockProcessorContext<>();
        context.setCurrentSystemTimeMs(1L);
        context.addStateStore(frontierStore);
        context.addStateStore(bufferStore);
        context.addStateStore(candidateIndexStore);
        context.addStateStore(forwardedIndexStore);
        context.addStateStore(new ParsleyCommittedCompleteness("frontier-commit-hook"));
        processor.init(context);

        // A business record on t2@5 depending on t1@0 is held: t1 has never been observed, so the
        // delegate is not invoked yet (and so does not throw yet).
        context.setRecordMetadata("t2", 0, 5);
        Headers held = ParsleyHeader.mutableHeaders();
        held.add(ParsleyHeader.CAUSAL_DEPENDENCIES, ParsleyClock.empty().observe(T1_ID, 0, 0).toBytes());
        processor.process(new Record<>("k", "v", 0L, held));
        assertEquals(1, bufferStore.approximateNumEntries(), "the t2@5 record must be held on t1@0");

        // An epoch-boundary marker arrives on t1@0. Its own delivery advances the t1 frontier to 0,
        // releasing the held record into the delegate — which throws.
        context.setRecordMetadata("t1", 0, 0);
        ParsleyEpochBoundary boundary = new ParsleyEpochBoundary(1, ParsleyClock.empty().observe(T1_ID, 0, 10));
        Headers markerHeaders = ParsleyHeader.mutableHeaders();
        markerHeaders.add(ParsleyHeader.EPOCH_BOUNDARY, boundary.toBytes());
        markerHeaders.add(ParsleyHeader.CAUSAL_DEPENDENCIES,
                ParsleyClock.empty().observe(UPSTREAM_ID, 0, 3).toBytes());
        Record<String, String> marker = new Record<>("k", null, 0L, markerHeaders);

        RuntimeException thrown = assertThrows(RuntimeException.class, () -> processor.process(marker),
                "the delegate's failure must propagate and fail the task, never be swallowed as a "
                        + "marker-decode warning");
        assertEquals(boom, thrown, "the propagated exception must be the delegate's own");
        assertEquals(0, bufferStore.approximateNumEntries(),
                "the release had already removed the record from the buffer — the state that makes "
                        + "swallowing the delegate's failure a permanent record loss");
    }

    /**
     * The idle-round regression for B1: an epoch-boundary marker whose carried completeness the channel
     * already dominates — the quiesced-maintenance-window case epochs exist for, where the boundary
     * re-carries the completeness the preceding snapshot already advertised and so teaches the channel
     * nothing new — must STILL be relayed downstream exactly once on its channel's first sight of it.
     * Gating the relay on the carried clock alone (as watermark and snapshot relay do) would strand the
     * transition here forever: the downstream never sees the marker on this channel, so its
     * marker-on-every-channel window never closes and the floor never advances (silent, permanent).
     *
     * <p>Modelled with an empty carried completeness clock — exactly what a topology with zero traffic
     * during the round produces — so {@code channelAdvanced} is false yet the marker is newly recorded.
     *
     * Asserts the delegate never runs, nothing is buffered, and exactly one boundary marker is relayed
     * on the same key despite the carried completeness advancing nothing.
     */
    @Test
    void idleRoundBoundaryRelaysOnFirstSightEvenWhenItsCarriedCompletenessTeachesNothingNew() {
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
        ParsleyProcessor<String, String, String, String> processor = new ParsleyProcessor<>(
                delegate, serializer,
                "frontier", "buffer", "candidate-index", "forwarded-index",
                Set.of("t1"), Set.of(), List.of(),
                configs -> ADMIN, ParsleyConfig.from(new Properties()), null);

        MockProcessorContext<String, String> context = new MockProcessorContext<>();
        context.setCurrentSystemTimeMs(1L);
        context.addStateStore(frontierStore);
        context.addStateStore(bufferStore);
        context.addStateStore(candidateIndexStore);
        context.addStateStore(forwardedIndexStore);
        context.addStateStore(new ParsleyCommittedCompleteness("frontier-commit-hook"));
        processor.init(context);

        // An epoch-boundary marker on t1 carrying EMPTY completeness: the channel (an empty clock
        // dominates an empty clock) learns nothing new from it, exactly as on an idle, quiesced round.
        context.setRecordMetadata("t1", 0, 0);
        ParsleyEpochBoundary boundary = new ParsleyEpochBoundary(1, ParsleyClock.empty().observe(T1_ID, 0, 10));
        Headers headers = ParsleyHeader.mutableHeaders();
        headers.add(ParsleyHeader.EPOCH_BOUNDARY, boundary.toBytes());
        headers.add(ParsleyHeader.CAUSAL_DEPENDENCIES, ParsleyClock.empty().toBytes());
        processor.process(new Record<>("k", null, 0L, headers));

        assertTrue(processed.isEmpty(), "the epoch-boundary marker must never reach the user delegate");
        assertEquals(0, bufferStore.approximateNumEntries(), "the marker must never be buffered");

        List<? extends MockProcessorContext.CapturedForward<? extends String, ? extends String>> forwarded =
                context.forwarded();
        assertEquals(1, forwarded.size(),
                "the boundary must relay on its channel's first sight even when its carried completeness "
                        + "taught the channel nothing new — otherwise the downstream transition stalls forever");
        assertTrue(hasHeader(forwarded.get(0).record(), ParsleyHeader.EPOCH_BOUNDARY),
                "the relayed record must be the boundary marker");
    }

    /**
     * Cycle-safety: a duplicate of the same epoch's boundary re-delivered on a channel already recorded
     * for that pending transition records nothing new and must NOT relay again. This is what keeps a
     * cyclic topology from ping-ponging the same marker forever once the fix for B1 relays a boundary on
     * first sight regardless of the carried clock.
     *
     * Asserts that after the first boundary relays once, a second delivery of the same boundary on the
     * same channel produces no further relay.
     */
    @Test
    void aDuplicateBoundaryOnAnAlreadySeenChannelDoesNotRelayAgain() {
        TestKeyValueStore<String, byte[]> frontierStore =
                new TestKeyValueStore<String, byte[]>(Comparator.naturalOrder(), "frontier");
        TestKeyValueStore<Long, byte[]> bufferStore =
                new TestKeyValueStore<Long, byte[]>(Comparator.naturalOrder(), "buffer");
        TestKeyValueStore<byte[], byte[]> candidateIndexStore =
                new TestKeyValueStore<byte[], byte[]>(Arrays::compareUnsigned, "candidate-index");
        TestKeyValueStore<byte[], byte[]> forwardedIndexStore =
                new TestKeyValueStore<byte[], byte[]>(Arrays::compareUnsigned, "forwarded-index");

        Processor<String, String, String, String> delegate = new Processor<>() {
            @Override public void init(ProcessorContext<String, String> context) {}
            @Override public void process(Record<String, String> record) {}
        };
        ParsleySerializer<String, String> serializer =
                new ParsleySerializer<>(new ParsleyResolver<>(t -> Serdes.String(), t -> Serdes.String()));
        ParsleyProcessor<String, String, String, String> processor = new ParsleyProcessor<>(
                delegate, serializer,
                "frontier", "buffer", "candidate-index", "forwarded-index",
                Set.of("t1"), Set.of(), List.of(),
                configs -> ADMIN, ParsleyConfig.from(new Properties()), null);

        MockProcessorContext<String, String> context = new MockProcessorContext<>();
        context.setCurrentSystemTimeMs(1L);
        context.addStateStore(frontierStore);
        context.addStateStore(bufferStore);
        context.addStateStore(candidateIndexStore);
        context.addStateStore(forwardedIndexStore);
        context.addStateStore(new ParsleyCommittedCompleteness("frontier-commit-hook"));
        processor.init(context);

        ParsleyEpochBoundary boundary = new ParsleyEpochBoundary(1, ParsleyClock.empty().observe(T1_ID, 0, 10));

        // First sight on t1@0: relays (newly recorded).
        context.setRecordMetadata("t1", 0, 0);
        Headers first = ParsleyHeader.mutableHeaders();
        first.add(ParsleyHeader.EPOCH_BOUNDARY, boundary.toBytes());
        first.add(ParsleyHeader.CAUSAL_DEPENDENCIES, ParsleyClock.empty().toBytes());
        processor.process(new Record<>("k", null, 0L, first));
        assertEquals(1, context.forwarded().size(), "the boundary must relay once on its first sight");

        // Same epoch's boundary re-delivered on the same channel (t1@1): records nothing new, must not relay.
        context.setRecordMetadata("t1", 0, 1);
        Headers duplicate = ParsleyHeader.mutableHeaders();
        duplicate.add(ParsleyHeader.EPOCH_BOUNDARY, boundary.toBytes());
        duplicate.add(ParsleyHeader.CAUSAL_DEPENDENCIES, ParsleyClock.empty().toBytes());
        processor.process(new Record<>("k", null, 0L, duplicate));

        assertEquals(1, context.forwarded().size(),
                "a duplicate boundary on an already-seen channel records nothing new and must not relay "
                        + "again — otherwise a cyclic topology would ping-pong it forever");
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
