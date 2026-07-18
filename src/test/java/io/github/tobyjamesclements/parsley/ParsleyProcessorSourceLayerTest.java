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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link ParsleyProcessor}'s source-layer behaviour: a task consuming an external topology source
 * (no in-band marker will ever arrive on it) self-initiates the in-band wave off the coordination log —
 * publishing its completeness and injecting the snapshot marker when a round opens, and adopting the
 * committed floor for its source coordinate while injecting the boundary marker when an epoch commits.
 * Driven by a real {@link ParsleyEpochRuntime} over the in-memory transport double. Members declare a
 * single-task app with a member-app roster; genesis (the first commit) seals once the cohort has declared,
 * at an empty floor. The Fixture's own app id is empty (a {@link MockProcessorContext} has no {@code
 * application.id}), so its default roster is {@code {""}}.
 */
class ParsleyProcessorSourceLayerTest {

    private static final Uuid T1_ID = Uuid.randomUuid();
    private static final ParsleyTopicAdmin ADMIN = TestTopicAdmin.of(Map.of("t1", T1_ID));

    /**
     * The validate-before-declare wedge: a member whose own declared subscriptions do not cover the
     * coordinated domain is rejected in {@code init} <em>before</em> it appends a {@link
     * ParsleyEpochEvent.JoinRequested}, so it never becomes a pending joiner that a commit could promote
     * into a running member that can never be meshed — which would permanently wedge every future epoch
     * round. Rejecting it before it declares crash-loops it in isolation, leaving the shared log carrying
     * no declaration from it.
     */
    @Test
    void aMisMeshedMemberIsRejectedBeforeItDeclaresItself() {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        ParsleyEpochRuntime runtime = new ParsleyEpochRuntime(new InMemoryEpochTransport(log));
        runtime.runOnce();   // marks the runtime bootstrapped before init() validates against the domain

        // An existing member X consuming t1 and producing "extra" makes the coordinated domain {t1, extra}.
        InMemoryEpochTransport seeder = new InMemoryEpochTransport(log);
        seeder.append(new ParsleyEpochEvent.JoinRequested("X", "X", Set.of("t1"), Set.of("extra"), Set.of("X"), 1));
        runtime.runOnce();   // fold X so runtime.domainTopics() = {t1, extra} before the Fixture's init

        // The Fixture's processor consumes only t1 (no sink), so it cannot see "extra" — it is mis-meshed
        // against the domain. Its init must fail BEFORE it declares itself, not join and then be promoted.
        IllegalStateException thrown = assertThrows(IllegalStateException.class, () -> new Fixture(runtime),
                "a member that cannot cover the coordinated domain must fail its init before declaring");
        assertTrue(thrown.getMessage().contains("extra"),
                "the failure names the uncovered domain topic: " + thrown.getMessage());

        runtime.runOnce();   // flush the outbox — nothing the failed init enqueued may reach the log

        List<String> declaredMembers = log.events().stream()
                .filter(ParsleyEpochEvent.JoinRequested.class::isInstance)
                .map(e -> ((ParsleyEpochEvent.JoinRequested) e).memberId())
                .toList();
        assertEquals(List.of("X"), declaredMembers,
                "only the pre-existing member X is declared; the rejected member appended no JoinRequested");
    }

    /**
     * A source-layer task, on seeing an open snapshot round in the log, publishes its completeness to the
     * log and injects the snapshot marker downstream on its last-seen key — the first marker of the cut,
     * which no external source could have produced.
     */
    @Test
    void sourceLayerTaskPublishesAndInjectsSnapshotWhenARoundOpens() {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        ParsleyEpochRuntime runtime = new ParsleyEpochRuntime(new InMemoryEpochTransport(log));
        runtime.runOnce();   // bootstrap

        Fixture f = new Fixture(runtime);   // init declares the member and opens the genesis round
        runtime.runOnce();                  // genesis commits (the lone founder is now running); round clears
        String member = fixtureMemberId(log);

        // Open a fresh round the source-layer task must respond to.
        new InMemoryEpochTransport(log).append(new ParsleyEpochEvent.SnapshotRequested(member));
        runtime.runOnce();
        f.processRecord("k", 5L);   // sets the last-seen key; the poll sees the open round -> publish + inject
        runtime.runOnce();          // flush the publication the processor enqueued

        assertTrue(log.events().stream().anyMatch(e -> e instanceof ParsleyEpochEvent.FrontierPublished),
                "the source-layer task must publish its completeness for the open round");
        List<? extends MockProcessorContext.CapturedForward<? extends String, ? extends String>> snapshots =
                f.forwardedWith(ParsleyHeader.EPOCH_SNAPSHOT);
        assertEquals(1, snapshots.size(), "the source-layer task injects the snapshot marker exactly once");
        assertEquals("k", snapshots.get(0).record().key(),
                "the injected marker reuses the last-seen key as informational wire content");
    }

    /**
     * Regression test for the marker-relay finding: a source-layer task that has never processed a business
     * record ({@code lastSeenKey} still {@code null}, exactly the state right after a restart) must still
     * inject its snapshot-marker relay on the very first poll — routing no longer depends on a key.
     */
    @Test
    void sourceLayerTaskInjectsTheSnapshotMarkerEvenWithNoBusinessKeyYet() {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        ParsleyEpochRuntime runtime = new ParsleyEpochRuntime(new InMemoryEpochTransport(log));
        runtime.runOnce();

        Fixture f = new Fixture(runtime);
        runtime.runOnce();                  // genesis commits; f is running
        String member = fixtureMemberId(log);

        new InMemoryEpochTransport(log).append(new ParsleyEpochEvent.SnapshotRequested(member));
        runtime.runOnce();
        f.triggerEpochPoll();   // the idle-task wall-clock poll — no business record has ever been processed
        runtime.runOnce();      // flush the publication the poll enqueued

        assertTrue(log.events().stream().anyMatch(e -> e instanceof ParsleyEpochEvent.FrontierPublished),
                "the source-layer task must publish its completeness for the open round even with no key yet");
        List<? extends MockProcessorContext.CapturedForward<? extends String, ? extends String>> snapshots =
                f.forwardedWith(ParsleyHeader.EPOCH_SNAPSHOT);
        assertEquals(1, snapshots.size(),
                "the snapshot marker must still be injected with lastSeenKey null — nothing to skip or retry");
    }

    /**
     * Regression test for the marker-reachability gap: {@code externalSourceTopics()} is a live view of the
     * log's current declarations, so the instant a new member declares an until-now-external topic as a
     * sink, that topic drops out of the DAG-wide registry immediately — one round before the declaring
     * member is running. Without a grace period the outgoing self-adopter would stop adopting for that
     * topic in the very poll it leaves the live registry, and nobody would inject that one epoch's boundary.
     *
     * <p>B founds the domain (t1 external, no producer). To add P (which produces t1), B redeploys naming P
     * in its roster and P joins; epoch 2 admits P, and t1 leaves the live registry. B's next poll must still
     * inject epoch 2's boundary onto t1 from its own grace record, one round after t1 left the live view.
     */
    @Test
    void outgoingSelfAdopterStillInjectsTheHandoffEpochsBoundaryAfterATopicLeavesTheLiveRegistry() {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        ParsleyEpochRuntime runtime = new ParsleyEpochRuntime(new InMemoryEpochTransport(log));
        runtime.runOnce();

        Fixture b = new Fixture(runtime);   // B's init joins declaring t1 as input, no sink, roster {""}
        settle(log, runtime);               // genesis commits epoch 1; B running, t1 external
        String bm = fixtureMemberId(log);

        b.processRecord("k1", 1L);          // B's poll adopts epoch 1 -> injects boundary onto t1
        runtime.runOnce();
        List<? extends MockProcessorContext.CapturedForward<? extends String, ? extends String>> afterEpoch1 =
                b.forwardedWith(ParsleyHeader.EPOCH_BOUNDARY);
        assertEquals(1, afterEpoch1.size(), "B must inject epoch 1's boundary while t1 is still external");
        assertEquals(1L, decodeBoundary(afterEpoch1.get(0)).epochId(), "the first injected boundary is epoch 1");

        // Grow the roster to admit P: B redeploys naming P, P joins declaring t1 as its sink, a round opens.
        InMemoryEpochTransport seeder = new InMemoryEpochTransport(log);
        seeder.append(new ParsleyEpochEvent.JoinRequested(bm, "", Set.of("t1"), Set.of(), Set.of("", "P"), 1));
        seeder.append(new ParsleyEpochEvent.JoinRequested("P", "P", Set.of(), Set.of("t1"), Set.of("", "P"), 1));
        seeder.append(new ParsleyEpochEvent.SnapshotRequested(bm));
        runtime.runOnce();
        b.processRecord("k2", 2L);          // B publishes for the round that will admit P
        settle(log, runtime);               // epoch 2 commits (roster {"",P}); P running; t1 now produced

        b.processRecord("k3", 3L);          // B's next poll must still inject epoch 2's boundary onto t1
        runtime.runOnce();
        List<? extends MockProcessorContext.CapturedForward<? extends String, ? extends String>> afterEpoch2 =
                b.forwardedWith(ParsleyHeader.EPOCH_BOUNDARY);
        assertEquals(2, afterEpoch2.size(),
                "B must still inject epoch 2's boundary onto t1 even though t1 already left the live registry");
        assertEquals(2L, decodeBoundary(afterEpoch2.get(1)).epochId(), "the second injected boundary is epoch 2");
    }

    /**
     * Regression test for the marker-relay finding, the epoch-boundary counterpart: an outgoing
     * self-adopter that promotes an epoch on its very first poll — before ever processing a business
     * record — must still inject the boundary marker onto its external-source coordinate.
     */
    @Test
    void outgoingSelfAdopterInjectsTheBoundaryEvenWithNoBusinessKeyYet() {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        ParsleyEpochRuntime runtime = new ParsleyEpochRuntime(new InMemoryEpochTransport(log));
        runtime.runOnce();

        Fixture b = new Fixture(runtime);
        settle(log, runtime);               // genesis commits epoch 1; B running, t1 external

        b.triggerEpochPoll();               // B's first poll adopts epoch 1 -> must inject the boundary
        runtime.runOnce();
        List<? extends MockProcessorContext.CapturedForward<? extends String, ? extends String>> boundaries =
                b.forwardedWith(ParsleyHeader.EPOCH_BOUNDARY);
        assertEquals(1, boundaries.size(),
                "B must inject epoch 1's boundary even with lastSeenKey still null — nothing to skip or retry");
        assertEquals(1L, decodeBoundary(boundaries.get(0)).epochId(), "the injected boundary is epoch 1");
    }

    /**
     * Regression test for the handoff-grace-cache-durability finding: the grace set is derived purely from
     * {@link ParsleyEpochLog#externalSourceTopicsAsOfPreviousCommit()}, so a task that crashes inside the
     * handoff window and restarts (a brand-new runtime and processor, sharing only the durable log)
     * computes the identical answer from the log alone and still injects the admitting epoch's boundary.
     */
    @Test
    void restartInsideTheHandoffWindowStillInjectsTheAdmittingEpochsBoundary() {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        ParsleyEpochRuntime runtime = new ParsleyEpochRuntime(new InMemoryEpochTransport(log));
        runtime.runOnce();

        Fixture b = new Fixture(runtime);
        settle(log, runtime);               // genesis commits epoch 1; B running, t1 external
        String bm = fixtureMemberId(log);
        b.processRecord("k1", 1L);          // B adopts epoch 1
        runtime.runOnce();

        // Grow the roster to admit P; epoch 2 commits and t1 leaves the live registry — B never polls again.
        InMemoryEpochTransport seeder = new InMemoryEpochTransport(log);
        seeder.append(new ParsleyEpochEvent.JoinRequested(bm, "", Set.of("t1"), Set.of(), Set.of("", "P"), 1));
        seeder.append(new ParsleyEpochEvent.JoinRequested("P", "P", Set.of(), Set.of("t1"), Set.of("", "P"), 1));
        seeder.append(new ParsleyEpochEvent.SnapshotRequested(bm));
        runtime.runOnce();
        b.processRecord("k2", 2L);          // B publishes for the round that admits P
        settle(log, runtime);               // epoch 2 commits; B never reacts to it

        // B "crashes" and restarts: a brand-new runtime and processor, sharing only the durable log.
        ParsleyEpochRuntime restartedRuntime = new ParsleyEpochRuntime(new InMemoryEpochTransport(log));
        restartedRuntime.runOnce();
        Fixture restarted = new Fixture(restartedRuntime);
        restartedRuntime.runOnce();

        restarted.processRecord("k3", 3L);  // the restarted instance's very first poll
        restartedRuntime.runOnce();
        List<? extends MockProcessorContext.CapturedForward<? extends String, ? extends String>> boundaries =
                restarted.forwardedWith(ParsleyHeader.EPOCH_BOUNDARY);
        assertEquals(1, boundaries.size(),
                "the restarted instance must inject epoch 2's boundary onto t1 despite never having adopted "
                        + "epoch 1 itself — the grace set comes from the log, not from memory that just crashed");
        assertEquals(2L, decodeBoundary(boundaries.get(0)).epochId(), "the injected boundary is epoch 2");
    }

    /**
     * The admission-floor seed (B1, the H1 defence): a member admitted post-genesis at a NON-EMPTY floor
     * F2 re-adopts ITS OWN cut on a fresh ({@code !restored}) init — not the current committed floor F3
     * (which would over-strip history it never skipped) and not the empty floor (which would replay pre-cut
     * history out of gate order). The Fixture's member {@code 0_0} is driven to be admitted at epoch 2
     * (floor {@code T1@5}), with epoch 3 later raising the committed floor to {@code T1@10}. A fresh init
     * must gate at {@code T1@5}: a below-F2 dependency ({@code T1@3}) is stripped (delivered), while an
     * above-F2, below-F3 dependency ({@code T1@7}) is not (held). Reverting the seed to the current floor
     * delivers {@code T1@7}; reverting it to the empty floor holds {@code T1@3} — either fails this test.
     */
    @Test
    void anAdmittedJoinerSeedsItsOwnAdmissionFloorNotTheCurrentOrEmptyFloor() {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        ParsleyEpochRuntime runtime = new ParsleyEpochRuntime(new InMemoryEpochTransport(log));
        InMemoryEpochTransport seeder = new InMemoryEpochTransport(log);
        // Founder F founds genesis (roster {F}); F then redeploys naming the Fixture's app ""; the Fixture's
        // member 0_0 is admitted at epoch 2 (floor T1@5), and epoch 3 raises the committed floor to T1@10.
        seeder.append(new ParsleyEpochEvent.JoinRequested("F/0_0", "F", Set.of("t1"), Set.of(), Set.of("F"), 1));
        seeder.append(new ParsleyEpochEvent.SnapshotRequested("F/0_0"));
        seeder.append(new ParsleyEpochEvent.EpochCommitted(1, ParsleyClock.empty(), Set.of("F")));
        seeder.append(new ParsleyEpochEvent.JoinRequested("F/0_0", "F", Set.of("t1"), Set.of(), Set.of("F", ""), 1));
        seeder.append(new ParsleyEpochEvent.JoinRequested("0_0", "", Set.of("t1"), Set.of(), Set.of("F", ""), 1));
        seeder.append(new ParsleyEpochEvent.SnapshotRequested("F/0_0"));
        seeder.append(new ParsleyEpochEvent.FrontierPublished("F/0_0", ParsleyClock.empty().observe(T1_ID, 0, 5)));
        seeder.append(new ParsleyEpochEvent.EpochCommitted(2, ParsleyClock.empty().observe(T1_ID, 0, 5), Set.of("F", "")));
        seeder.append(new ParsleyEpochEvent.SnapshotRequested("F/0_0"));
        seeder.append(new ParsleyEpochEvent.FrontierPublished("F/0_0", ParsleyClock.empty().observe(T1_ID, 0, 10)));
        seeder.append(new ParsleyEpochEvent.FrontierPublished("0_0", ParsleyClock.empty().observe(T1_ID, 0, 10)));
        seeder.append(new ParsleyEpochEvent.EpochCommitted(3, ParsleyClock.empty().observe(T1_ID, 0, 10), Set.of("F", "")));
        runtime.runOnce();   // fold it: committedEpoch=(3, T1@10), admissionFloors[0_0]=(2, T1@5)

        Fixture f = new Fixture(runtime);   // 0_0 is a running member -> restart path -> seeds ITS admission floor
        runtime.runOnce();

        f.processRecordWithDep("belowF2", 0L, T1_ID, 3L);   // dep below F2 -> stripped -> delivered
        f.processRecordWithDep("aboveF2", 1L, T1_ID, 7L);   // dep above F2, below F3 -> not stripped -> held

        assertTrue(f.delivered().contains("belowF2"),
                "a below-admission-floor dependency (T1@3) must be stripped and the record delivered — proving "
                        + "the seed is NOT the empty floor");
        assertFalse(f.delivered().contains("aboveF2"),
                "an above-admission-floor dependency (T1@7) must NOT be stripped — proving the seed is the "
                        + "member's own admission floor (T1@5), not the current committed floor (T1@10)");
    }

    /** Drives {@code runtime.runOnce()} until the shared log stops growing (three quiet passes), so a
     * commit that {@code driveCommit} appends is also folded back before the test proceeds. */
    private static void settle(InMemoryEpochTransport.SharedLog log, ParsleyEpochRuntime runtime) {
        int quiet = 0;
        while (quiet < 3) {
            long before = log.events().size();
            runtime.runOnce();
            quiet = (log.events().size() == before) ? quiet + 1 : 0;
        }
    }

    /** The member id of the first (Fixture) declaration on the shared log. */
    private static String fixtureMemberId(InMemoryEpochTransport.SharedLog log) {
        return log.events().stream()
                .filter(ParsleyEpochEvent.JoinRequested.class::isInstance)
                .map(e -> ((ParsleyEpochEvent.JoinRequested) e).memberId())
                .findFirst().orElseThrow();
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
        private final java.util.List<String> delivered = new java.util.ArrayList<>();

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
                @Override public void process(Record<String, String> record) {
                    delivered.add(record.value());   // records every business delivery for gate assertions
                }
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

        /** Feeds a business record at {@code t1@offset} carrying a single dependency on {@code
         * depTopic@depOffset}; the value equals the key, so a delivery is identifiable in {@link #delivered}. */
        void processRecordWithDep(String key, long offset, Uuid depTopic, long depOffset) {
            context.setRecordMetadata("t1", 0, offset);
            Headers deps = ParsleyHeader.mutableHeaders();
            deps.add(ParsleyHeader.CAUSAL_DEPENDENCIES,
                    ParsleyClock.empty().observe(depTopic, 0, depOffset).toBytes());
            processor.process(new Record<>(key, key, 0L, deps));
        }

        List<String> delivered() {
            return delivered;
        }

        /**
         * Fires the wall-clock {@code pollEpochCoordination()} punctuator directly — the path an idle
         * source-layer task reacts through, with no business record ever processed (so {@code lastSeenKey}
         * stays {@code null}).
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
