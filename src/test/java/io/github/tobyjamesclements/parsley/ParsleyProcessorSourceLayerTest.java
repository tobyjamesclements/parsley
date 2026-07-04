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
        seeder.append(new EpochEvent.JoinRequested("X", Set.of(), Set.of()));
        seeder.append(new EpochEvent.SnapshotRequested("X"));
        runtime.runOnce();

        Fixture f = new Fixture(runtime);
        // The processor's init() declared its input topic t1 (no sink) as a member; fold that so the
        // source-topic registry derives t1 as an external source before the first poll.
        runtime.runOnce();
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

    // The running-node boundary adopt-and-inject path (pollEpochCoordination on a node that was present
    // through the transition) is covered end-to-end by CausalCoordinationTopologyTest, which asserts an
    // epoch-boundary marker with a real floor reaches the sink. A FRESH node at committed>0 is instead a
    // joiner (it blocks and direct-settles); that path is exercised in CausalCoordinationTest and, fully,
    // by the WS4d Docker integration test.

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
                    ParsleyEpochSnapshotPublisher.NOOP, CausalCoordination.forRuntime(runtime));
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
