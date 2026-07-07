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
        seeder.append(new ParsleyEpochEvent.JoinRequested("X", Set.of(), Set.of()));
        seeder.append(new ParsleyEpochEvent.SnapshotRequested("X"));
        runtime.runOnce();

        Fixture f = new Fixture(runtime);
        // The processor's init() declared its input topic t1 (no sink) as a member; fold that so the
        // source-topic registry derives t1 as an external source before the first poll.
        runtime.runOnce();
        // A business record sets the last-seen key and this node's completeness to T1@5.
        f.processRecord("k", 5L);
        // The poll at the end of process() sees the open round -> publish + inject snapshot.
        runtime.runOnce();   // flush the publication the processor enqueued

        assertTrue(log.events().stream().anyMatch(e -> e instanceof ParsleyEpochEvent.FrontierPublished),
                "the source-layer task must publish its completeness for the open round");
        List<? extends MockProcessorContext.CapturedForward<? extends String, ? extends String>> snapshots =
                f.forwardedWith(ParsleyHeader.EPOCH_SNAPSHOT);
        assertEquals(1, snapshots.size(), "the source-layer task injects the snapshot marker exactly once");
        assertEquals("k", snapshots.get(0).record().key(),
                "the injected marker reuses the last-seen key so it lands on this task's partition lane");
    }

    // The running-node boundary adopt-and-inject path (pollEpochCoordination on a node that was present
    // through the transition) is covered end-to-end by ParsleyCoordinationTopologyTest, which asserts an
    // epoch-boundary marker with a real floor reaches the sink. A FRESH node at committed>0 is instead a
    // joiner (it blocks and direct-settles); that path is exercised in ParsleyCoordinationTest and, fully,
    // by the WS4d Docker integration test.

    /**
     * Regression test for the BACKLOG.md marker-reachability gap: {@code externalSourceTopics()} is a
     * live, memoryless view of the log's current declarations, so the instant a new member declares an
     * until-now-external topic as a sink, that topic drops out of the DAG-wide registry immediately —
     * one full round before the declaring member is even running, let alone able to relay anything
     * in-band (it structurally cannot relay the very epoch whose round admits it). Without a grace
     * period, the outgoing self-adopter stops adopting for that topic in the very same poll it leaves
     * the live registry, and nobody ever injects that one epoch's boundary onto it.
     *
     * <p>B is the sole running member, self-adopting t1 (external, no producer) across epoch 1. Member P
     * then joins declaring t1 as its sink; the instant P's join folds, t1 already leaves the live
     * registry, well before P is promoted to running by epoch 2's commit. B's next poll must still
     * inject epoch 2's boundary onto t1 — the live registry alone says otherwise, so this must be driven
     * by B's own record of what it adopted last time.
     *
     * <p>Asserts B injects exactly two boundary markers (epoch 1, then epoch 2) despite t1 having already
     * left the live external-source registry by the time epoch 2 commits.
     */
    @Test
    void outgoingSelfAdopterStillInjectsTheHandoffEpochsBoundaryAfterATopicLeavesTheLiveRegistry() {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        ParsleyEpochRuntime runtime = new ParsleyEpochRuntime(new InMemoryEpochTransport(log));
        InMemoryEpochTransport seeder = new InMemoryEpochTransport(log);
        runtime.runOnce();   // marks the runtime bootstrapped before init() blocks on it

        Fixture b = new Fixture(runtime);   // B's init() joins declaring t1 as input, no sink
        runtime.runOnce();
        String bMemberId = log.events().stream()
                .filter(ParsleyEpochEvent.JoinRequested.class::isInstance)
                .map(e -> ((ParsleyEpochEvent.JoinRequested) e).memberId())
                .findFirst().orElseThrow();

        // Epoch 1: B is the only running member; t1 is external (nobody produces it) throughout.
        seeder.append(new ParsleyEpochEvent.SnapshotRequested(bMemberId));
        runtime.runOnce();
        b.processRecord("k0", 0L);           // B publishes for the open round (owesPublication)
        runtime.runOnce();
        seeder.append(new ParsleyEpochEvent.EpochCommitted(1, ParsleyClock.empty()));
        runtime.runOnce();                   // B promoted to running; epoch 1 settled
        b.processRecord("k1", 1L);           // B's poll adopts epoch 1 -> injects boundary onto t1
        runtime.runOnce();

        List<? extends MockProcessorContext.CapturedForward<? extends String, ? extends String>> afterEpoch1 =
                b.forwardedWith(ParsleyHeader.EPOCH_BOUNDARY);
        assertEquals(1, afterEpoch1.size(), "B must inject epoch 1's boundary while t1 is still external");
        assertEquals(1L, decodeBoundary(afterEpoch1.get(0)).epochId(), "the first injected boundary is epoch 1");

        // P joins declaring t1 as its sink. The instant this folds, t1 leaves the LIVE external registry —
        // before P is anywhere near running.
        seeder.append(new ParsleyEpochEvent.JoinRequested("P", Set.of(), Set.of("t1")));
        runtime.runOnce();
        seeder.append(new ParsleyEpochEvent.SnapshotRequested("P"));
        runtime.runOnce();
        b.processRecord("k2", 2L);           // B publishes for the round that will admit P
        runtime.runOnce();
        seeder.append(new ParsleyEpochEvent.EpochCommitted(2, ParsleyClock.empty().observe(T1_ID, 0, 2)));
        runtime.runOnce();                   // P promoted to running; t1 is now genuinely produced

        // B's next poll must still inject epoch 2's boundary onto t1: the live registry already excludes
        // t1 (P is running), but t1 must get this one handoff epoch from its outgoing self-adopter.
        b.processRecord("k3", 3L);
        runtime.runOnce();

        List<? extends MockProcessorContext.CapturedForward<? extends String, ? extends String>> afterEpoch2 =
                b.forwardedWith(ParsleyHeader.EPOCH_BOUNDARY);
        assertEquals(2, afterEpoch2.size(),
                "B must still inject epoch 2's boundary onto t1 even though t1 already left the live "
                        + "external-source registry by the time epoch 2 committed");
        assertEquals(2L, decodeBoundary(afterEpoch2.get(1)).epochId(), "the second injected boundary is epoch 2");
    }

    private static ParsleyEpochBoundary decodeBoundary(
            MockProcessorContext.CapturedForward<? extends String, ? extends String> forward) {
        for (Header h : forward.record().headers()) {
            if (ParsleyHeader.EPOCH_BOUNDARY.equals(h.key())) {
                return ParsleyEpochBoundary.fromBytes(h.value());
            }
        }
        throw new IllegalArgumentException("not a boundary marker");
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
            TestKeyValueStore<byte[], byte[]> orphanIndexStore =
                    new TestKeyValueStore<byte[], byte[]>(Arrays::compareUnsigned, "orphan-index");
            Processor<String, String, String, String> delegate = new Processor<>() {
                @Override public void init(ProcessorContext<String, String> context) {}
                @Override public void process(Record<String, String> record) {}
            };
            ParsleySerializer<String, String> serializer =
                    new ParsleySerializer<>(new ParsleyResolver<>(t -> Serdes.String(), t -> Serdes.String()));
            this.processor = new ParsleyProcessor<>(
                    delegate, serializer,
                    "frontier", "buffer", "candidate-index", "forwarded-index", "orphan-index",
                    Set.of("t1"), Set.of(), List.of(), null,
                    configs -> ADMIN, ParsleyConfig.from(new Properties()), CausalAudit.NOOP, null,
                    ParsleyEpochSnapshotPublisher.NOOP, ParsleyCoordination.forRuntime(runtime));
            this.context = new MockProcessorContext<>();
            context.addStateStore(frontierStore);
            context.addStateStore(bufferStore);
            context.addStateStore(candidateIndexStore);
            context.addStateStore(forwardedIndexStore);
            context.addStateStore(orphanIndexStore);
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
