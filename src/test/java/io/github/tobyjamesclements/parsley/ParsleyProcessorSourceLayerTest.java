package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.Test;

import java.time.Duration;
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
                "the injected marker reuses the last-seen key as informational wire content (routing "
                        + "itself no longer depends on it — see ParsleyMarkerPartition)");
    }

    /**
     * Regression test for BACKLOG.md's marker-relay finding: before {@link ParsleyMarkerPartition}
     * existed, a source-layer task that had never processed a business record ({@code lastSeenKey} still
     * {@code null}) — exactly the state right after a restart, per the finding's repro — silently skipped
     * its snapshot-marker relay entirely, stalling every downstream lane until the first post-restart
     * business record finally triggered a retry. Routing no longer depends on a key at all, so the relay
     * must go out on the very first poll, key or no key.
     *
     * <p>Same setup as {@link #sourceLayerTaskPublishesAndInjectsSnapshotWhenARoundOpens}, except the
     * open-round poll is reached via {@link Fixture#triggerEpochPoll()} (the wall-clock punctuator an idle
     * task reacts through) instead of {@link Fixture#processRecord}, so {@code lastSeenKey} never becomes
     * non-null.
     *
     * <p>Asserts the source-layer task still publishes its completeness and injects the snapshot marker
     * exactly once, with no business record ever processed.
     */
    @Test
    void sourceLayerTaskInjectsTheSnapshotMarkerEvenWithNoBusinessKeyYet() {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        ParsleyEpochRuntime runtime = new ParsleyEpochRuntime(new InMemoryEpochTransport(log));

        InMemoryEpochTransport seeder = new InMemoryEpochTransport(log);
        seeder.append(new ParsleyEpochEvent.JoinRequested("X", Set.of(), Set.of()));
        seeder.append(new ParsleyEpochEvent.SnapshotRequested("X"));
        runtime.runOnce();

        Fixture f = new Fixture(runtime);
        runtime.runOnce();   // folds this task's own join before its first poll

        f.triggerEpochPoll();   // the idle-task wall-clock poll — no business record has ever been processed
        runtime.runOnce();      // flush the publication the poll enqueued

        assertTrue(log.events().stream().anyMatch(e -> e instanceof ParsleyEpochEvent.FrontierPublished),
                "the source-layer task must publish its completeness for the open round even with no "
                        + "business key yet");
        List<? extends MockProcessorContext.CapturedForward<? extends String, ? extends String>> snapshots =
                f.forwardedWith(ParsleyHeader.EPOCH_SNAPSHOT);
        assertEquals(1, snapshots.size(),
                "the snapshot marker must still be injected with lastSeenKey null — routing no longer "
                        + "depends on a business key, so there is nothing to skip or retry");
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

    /**
     * Regression test for BACKLOG.md's marker-relay finding, the epoch-boundary counterpart to {@link
     * #sourceLayerTaskInjectsTheSnapshotMarkerEvenWithNoBusinessKeyYet}: an outgoing self-adopter that
     * promotes an epoch on its very first poll — before ever processing a business record — must still
     * inject the boundary marker onto its external-source coordinate, not silently skip it because {@code
     * lastSeenKey} is {@code null}. This is exactly the finding's restart repro: a source-layer task
     * restarts (in-memory {@code lastSeenKey} wiped) with completeness already dominating a floor
     * committed while it was down, and self-adopts on its very first post-restart poll.
     *
     * <p>Single running member B, reaching every poll via {@link Fixture#triggerEpochPoll()} instead of
     * {@link Fixture#processRecord} throughout, so {@code lastSeenKey} never becomes non-null.
     *
     * <p>Asserts B injects epoch 1's boundary onto t1 despite never having processed a business record.
     */
    @Test
    void outgoingSelfAdopterInjectsTheBoundaryEvenWithNoBusinessKeyYet() {
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

        // Epoch 1 commits with B never having processed a single business record.
        seeder.append(new ParsleyEpochEvent.SnapshotRequested(bMemberId));
        runtime.runOnce();
        b.triggerEpochPoll();                // B publishes for the open round (owesPublication) — no key needed
        runtime.runOnce();
        seeder.append(new ParsleyEpochEvent.EpochCommitted(1, ParsleyClock.empty()));
        runtime.runOnce();                   // B promoted to running; epoch 1 settled
        b.triggerEpochPoll();                // B's poll adopts epoch 1 -> must inject the boundary regardless
        runtime.runOnce();

        List<? extends MockProcessorContext.CapturedForward<? extends String, ? extends String>> boundaries =
                b.forwardedWith(ParsleyHeader.EPOCH_BOUNDARY);
        assertEquals(1, boundaries.size(),
                "B must inject epoch 1's boundary even with lastSeenKey still null — routing no longer "
                        + "depends on a business key, so there is nothing to skip or retry");
        assertEquals(1L, decodeBoundary(boundaries.get(0)).epochId(), "the injected boundary is epoch 1");
    }

    /**
     * Regression test for BACKLOG.md's handoff-grace-cache-durability finding: the per-task in-memory
     * cache this fix replaced reset on restart, so a crash inside the handoff window — between a topic
     * leaving the live registry and this task's next adoption cycle — lost the departing topic's grace
     * cycle entirely; the post-restart poll would see empty adoption targets and silently advance past
     * the handoff epoch with no relay ever sent, permanently.
     *
     * <p>Same setup as {@link #outgoingSelfAdopterStillInjectsTheHandoffEpochsBoundaryAfterATopicLeavesTheLiveRegistry},
     * except B is never polled again after epoch 2 commits — instead, a brand-new {@link Fixture} (and a
     * brand-new {@link ParsleyEpochRuntime}, sharing only the durable {@code log}, no in-memory state at
     * all) is constructed, simulating a crash-and-restart landing exactly inside the handoff window. The
     * grace set is now derived purely from {@link ParsleyEpochLog#externalSourceTopicsAsOfPreviousCommit()},
     * so the restarted instance computes the identical answer from the log alone.
     *
     * <p>Asserts the restarted instance still injects epoch 2's boundary onto t1 on its very first poll,
     * despite having no memory of ever having adopted epoch 1 itself.
     */
    @Test
    void restartInsideTheHandoffWindowStillInjectsTheAdmittingEpochsBoundary() {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        ParsleyEpochRuntime runtime = new ParsleyEpochRuntime(new InMemoryEpochTransport(log));
        InMemoryEpochTransport seeder = new InMemoryEpochTransport(log);
        runtime.runOnce();

        Fixture b = new Fixture(runtime);   // B's init() joins declaring t1 as input, no sink
        runtime.runOnce();
        String bMemberId = log.events().stream()
                .filter(ParsleyEpochEvent.JoinRequested.class::isInstance)
                .map(e -> ((ParsleyEpochEvent.JoinRequested) e).memberId())
                .findFirst().orElseThrow();

        // Epoch 1: B is the only running member; t1 is external throughout.
        seeder.append(new ParsleyEpochEvent.SnapshotRequested(bMemberId));
        runtime.runOnce();
        b.processRecord("k0", 0L);
        runtime.runOnce();
        seeder.append(new ParsleyEpochEvent.EpochCommitted(1, ParsleyClock.empty()));
        runtime.runOnce();
        b.processRecord("k1", 1L);   // B adopts epoch 1 -> injects boundary onto t1
        runtime.runOnce();

        // P joins declaring t1 as its sink — the instant this folds, t1 leaves the live registry.
        seeder.append(new ParsleyEpochEvent.JoinRequested("P", Set.of(), Set.of("t1")));
        runtime.runOnce();
        seeder.append(new ParsleyEpochEvent.SnapshotRequested("P"));
        runtime.runOnce();
        b.processRecord("k2", 2L);   // B publishes for the round that will admit P
        runtime.runOnce();
        seeder.append(new ParsleyEpochEvent.EpochCommitted(2, ParsleyClock.empty().observe(T1_ID, 0, 2)));
        runtime.runOnce();          // P promoted to running; epoch 2 settled — B never polls again

        // B "crashes" here, before ever reacting to epoch 2's commit, and restarts: a brand-new runtime
        // and processor instance, sharing only the durable log — no lastAdoptedEpoch, no residual state.
        ParsleyEpochRuntime restartedRuntime = new ParsleyEpochRuntime(new InMemoryEpochTransport(log));
        restartedRuntime.runOnce();
        Fixture restarted = new Fixture(restartedRuntime);
        restartedRuntime.runOnce();

        restarted.processRecord("k3", 3L);   // the restarted instance's very first poll
        restartedRuntime.runOnce();

        List<? extends MockProcessorContext.CapturedForward<? extends String, ? extends String>> boundaries =
                restarted.forwardedWith(ParsleyHeader.EPOCH_BOUNDARY);
        assertEquals(1, boundaries.size(),
                "the restarted instance must inject epoch 2's boundary onto t1 despite never having "
                        + "adopted epoch 1 itself — the grace set comes from the log, not from memory "
                        + "that just crashed");
        assertEquals(2L, decodeBoundary(boundaries.get(0)).epochId(), "the injected boundary is epoch 2");
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
            Processor<String, String, String, String> delegate = new Processor<>() {
                @Override public void init(ProcessorContext<String, String> context) {}
                @Override public void process(Record<String, String> record) {}
            };
            ParsleySerializer<String, String> serializer =
                    new ParsleySerializer<>(new ParsleyResolver<>(t -> Serdes.String(), t -> Serdes.String()));
            this.processor = new ParsleyProcessor<>(
                    delegate, serializer,
                    "frontier", "buffer", "candidate-index", "forwarded-index",
                    Set.of("t1"), Set.of(), Set.of(), List.of(),
                    configs -> ADMIN, ParsleyConfig.from(new Properties()), null,
                    ParsleyEpochSnapshotPublisher.NOOP, ParsleyCoordination.forRuntime(runtime));
            this.context = new MockProcessorContext<>();
            context.setCurrentSystemTimeMs(1L);
            context.addStateStore(frontierStore);
            context.addStateStore(bufferStore);
            context.addStateStore(candidateIndexStore);
            context.addStateStore(forwardedIndexStore);
        context.addStateStore(new ParsleyCommittedCompleteness("frontier-commit-hook"));
            processor.init(context);
        }

        void processRecord(String key, long offset) {
            context.setRecordMetadata("t1", 0, offset);
            Headers deps = ParsleyHeader.mutableHeaders();
            deps.add(ParsleyHeader.CAUSAL_DEPENDENCIES, ParsleyClock.empty().toBytes());
            processor.process(new Record<>(key, "v", 0L, deps));
        }

        /**
         * Fires the wall-clock {@code pollEpochCoordination()} punctuator directly — the path an idle
         * source-layer task reacts through, with no business record ever processed (so {@code
         * lastSeenKey} stays {@code null}). {@code MockProcessorContext} never processes a business
         * record on its own, so this is the only way to reach {@code pollEpochCoordination()} without
         * also setting {@code lastSeenKey} via {@link #processRecord}.
         */
        void triggerEpochPoll() {
            context.scheduledPunctuators().stream()
                    .filter(p -> p.getType() == PunctuationType.WALL_CLOCK_TIME
                            && Duration.ofMillis(200).equals(p.getInterval()))
                    .forEach(p -> p.getPunctuator().punctuate(0L));
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
