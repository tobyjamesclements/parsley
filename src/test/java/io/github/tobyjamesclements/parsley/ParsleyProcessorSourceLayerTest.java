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

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link ParsleyProcessor}'s source-layer behaviour: a task consuming an external topology source
 * (no in-band marker will ever arrive on it) self-initiates the in-band wave off the coordination log —
 * publishing its completeness and injecting the snapshot marker when a round opens, and adopting the
 * committed floor for its source coordinate while injecting the boundary marker when an epoch commits.
 * Driven by a real {@link ParsleyEpochRuntime} over the in-memory transport double.
 */
class ParsleyProcessorSourceLayerTest {

    private static final Uuid T1_ID = Uuid.randomUuid();
    private static final ParsleyTopicAdmin ADMIN = TestTopicAdmin.of(Map.of("t1", T1_ID));

    /**
     * A source-layer task, on seeing an open snapshot round in the log, publishes its completeness to the
     * log and injects the snapshot marker downstream on its last-seen key — the first marker of the cut,
     * which no external source could have produced.
     */
    @Test
    void sourceLayerTaskPublishesAndInjectsSnapshotWhenARoundOpens() {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        ParsleyEpochRuntime runtime = new ParsleyEpochRuntime(new InMemoryEpochTransport(log));

        // Seed an open round owned by a non-local member, so the runtime folds it open without committing.
        InMemoryEpochTransport seeder = new InMemoryEpochTransport(log);
        seeder.append(new EpochEvent.JoinRequested("X"));
        seeder.append(new EpochEvent.SnapshotRequested("X"));
        runtime.runOnce();

        Fixture f = new Fixture(runtime);
        // A business record sets the last-seen key and this node's completeness to T1@5.
        f.processRecord("k", 5L);
        // The poll at the end of process() sees the open round -> publish + inject snapshot.
        runtime.runOnce();   // flush the publication the processor enqueued

        assertTrue(log.events().stream().anyMatch(e -> e instanceof EpochEvent.FrontierPublished),
                "the source-layer task must publish its completeness for the open round");
        List<? extends MockProcessorContext.CapturedForward<? extends String, ? extends String>> snapshots =
                f.forwardedWith(ParsleyHeader.EPOCH_SNAPSHOT);
        assertEquals(1, snapshots.size(), "the source-layer task injects the snapshot marker exactly once");
        assertEquals("k", snapshots.get(0).record().key(),
                "the injected marker reuses the last-seen key so it lands on this task's partition lane");
    }

    /**
     * A source-layer task, on seeing a committed epoch in the log, adopts the boundary for its external
     * source coordinate and injects the boundary marker downstream — the floor for a topology-source
     * channel arrives from the log, since no in-band boundary marker will ever reach it.
     */
    @Test
    void sourceLayerTaskAdoptsAndInjectsBoundaryWhenAnEpochCommits() {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        ParsleyEpochRuntime runtime = new ParsleyEpochRuntime(new InMemoryEpochTransport(log));

        InMemoryEpochTransport seeder = new InMemoryEpochTransport(log);
        seeder.append(new EpochEvent.JoinRequested("X"));
        seeder.append(new EpochEvent.SnapshotRequested("X"));
        seeder.append(new EpochEvent.EpochCommitted(1, ParsleyClock.empty().observe(T1_ID, 0, 10)));
        runtime.runOnce();
        assertEquals(1L, runtime.committedEpochId(), "the seeded log commits epoch 1");

        Fixture f = new Fixture(runtime);
        f.processRecord("k", 5L);

        List<? extends MockProcessorContext.CapturedForward<? extends String, ? extends String>> boundaries =
                f.forwardedWith(ParsleyHeader.EPOCH_BOUNDARY);
        assertEquals(1, boundaries.size(), "the source-layer task injects the boundary marker exactly once");
        assertEquals("k", boundaries.get(0).record().key(),
                "the injected boundary reuses the last-seen key so it lands on this task's partition lane");
        EpochBoundary relayed = boundaryOf(boundaries.get(0).record());
        assertEquals(1L, relayed.epochId(), "the injected boundary carries the committed epoch id");
        assertEquals(10L, relayed.lowerBounds().offsetFor(T1_ID, 0),
                "the injected boundary carries the committed floor for the source coordinate");
    }

    private static EpochBoundary boundaryOf(Record<? extends String, ? extends String> record) {
        for (Header h : record.headers()) {
            if (ParsleyHeader.EPOCH_BOUNDARY.equals(h.key()) && h.value() != null) {
                return EpochBoundary.fromBytes(h.value());
            }
        }
        throw new AssertionError("record carried no epoch-boundary header");
    }

    /** A wired source-layer processor over topic t1 (declared external) plus its MockProcessorContext. */
    private static final class Fixture {
        private final ParsleyProcessor<String, String, String, String> processor;
        private final MockProcessorContext<String, String> context;

        Fixture(ParsleyEpochRuntime runtime) {
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
            this.processor = new ParsleyProcessor<>(
                    delegate, CausalBufferLimit.ofSize(100), serializer,
                    "frontier", "buffer", "candidate-index", "forwarded-index", Set.of("t1"), Set.of(),
                    configs -> ADMIN, ParsleyConfig.from(new Properties()), CausalAudit.NOOP, null,
                    ParsleyEpochSnapshotPublisher.NOOP, runtime, Set.of("t1"));
            this.context = new MockProcessorContext<>();
            context.addStateStore(frontierStore);
            context.addStateStore(bufferStore);
            context.addStateStore(candidateIndexStore);
            context.addStateStore(forwardedIndexStore);
            processor.init(context);
        }

        void processRecord(String key, long offset) {
            context.setRecordMetadata("t1", 0, offset);
            Headers deps = ParsleyHeader.mutableHeaders();
            deps.add(ParsleyHeader.CAUSAL_DEPENDENCIES, ParsleyClock.empty().toBytes());
            processor.process(new Record<>(key, "v", 0L, deps));
        }

        List<? extends MockProcessorContext.CapturedForward<? extends String, ? extends String>> forwardedWith(
                String headerKey) {
            return context.forwarded().stream()
                    .filter(fwd -> {
                        for (Header h : fwd.record().headers()) {
                            if (headerKey.equals(h.key())) {
                                return true;
                            }
                        }
                        return false;
                    })
                    .toList();
        }
    }
}
