package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ParsleyEpochRuntime}: the per-instance driver that folds the shared {@code
 * epoch-events} log and, for a round one of its own members owns, performs the owner's collect → commit.
 * Every test drives {@link ParsleyEpochRuntime#runOnce()} synchronously against the in-memory transport
 * double, so the exact production loop body runs with no thread and no timing flakiness. Members declare a
 * single-task app with a member-app roster; genesis (the first commit) settles once the whole roster's
 * cohort has declared, at an empty floor.
 */
class ParsleyEpochRuntimeTest {

    private static final Uuid T1 = Uuid.randomUuid();
    private static final Uuid T2 = Uuid.randomUuid();
    private static final Set<String> AB = Set.of("A", "B");

    /** A lone node (roster {A}) joining and requesting a snapshot drives genesis: epoch 1 with an empty
     * floor (no running members to bound), and the node becomes a running member. */
    @Test
    void singleNodeJoinAndSnapshotCommitsGenesis() {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        ParsleyEpochRuntime a = runtimeOver(log);

        join(a, "A", Set.of("A"));
        a.requestSnapshot("A");
        settle(log, a);

        assertEquals(1L, a.committedEpochId(), "the lone-cohort genesis commits epoch 1");
        assertTrue(a.committedLowerBounds().isEmpty(), "the genesis floor is empty (nothing to bound)");
        assertEquals(Set.of("A"), a.committedRoster(), "the committed roster is {A}");
        assertEquals(1L, log.commitCount(), "exactly one commit was appended");
    }

    /** Two founders (roster {A,B}) both become running members at genesis; a later complete round commits
     * their frontiers' merge-min. Dedup makes concurrent identical commits one epoch, so the decision is
     * read via {@code committedEpochId} + the floor, not the append count. */
    @Test
    void aCompleteRoundCommitsTheMergeMinOfPublishedFrontiers() {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        ParsleyEpochRuntime a = runtimeOver(log);   // owns member A
        ParsleyEpochRuntime b = runtimeOver(log);   // owns member B

        genesisAB(log, a, b);
        assertEquals(1L, a.committedEpochId(), "genesis committed, promoting both founders to running");

        // Second round: both publish and the round commits epoch 2 with the merge-min of the frontiers.
        a.requestSnapshot("A");
        a.publishFrontier("A", ParsleyVectorClock.empty().observe(T1, 0, 10).observe(T2, 0, 5));
        b.publishFrontier("B", ParsleyVectorClock.empty().observe(T1, 0, 7).observe(T2, 0, 9));
        settle(log, a, b);

        ParsleyVectorClock expected = ParsleyVectorClock.empty().observe(T1, 0, 7).observe(T2, 0, 5);
        assertEquals(2L, a.committedEpochId(), "epoch 2 is committed");
        assertEquals(expected, a.committedLowerBounds(), "epoch 2 floor is the per-coordinate min of A and B");
        assertEquals(a.committedLowerBounds(), b.committedLowerBounds(),
                "both nodes agree on the floor regardless of which node's commit won the dedup");
    }

    /** {@link ParsleyEpochRuntime#committedEpoch()} returns the id and bounds from one volatile snapshot,
     * so a caller never pairs a fresh id with a stale floor. */
    @Test
    void committedEpochReturnsTheIdAndBoundsFromTheSameCommitTogether() {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        ParsleyEpochRuntime a = runtimeOver(log);
        ParsleyEpochRuntime b = runtimeOver(log);

        genesisAB(log, a, b);
        assertEquals(1L, a.committedEpoch().epochId(), "genesis's pairing reports id 1");
        assertTrue(a.committedEpoch().lowerBounds().isEmpty(), "genesis's pairing reports its own empty floor");

        a.requestSnapshot("A");
        a.publishFrontier("A", ParsleyVectorClock.empty().observe(T1, 0, 10).observe(T2, 0, 5));
        b.publishFrontier("B", ParsleyVectorClock.empty().observe(T1, 0, 7).observe(T2, 0, 9));
        settle(log, a, b);

        ParsleyVectorClock epoch2Floor = ParsleyVectorClock.empty().observe(T1, 0, 7).observe(T2, 0, 5);
        ParsleyEpochRuntime.CommittedEpoch committed = a.committedEpoch();
        assertEquals(2L, committed.epochId(), "epoch 2's pairing reports id 2");
        assertEquals(epoch2Floor, committed.lowerBounds(),
                "epoch 2's pairing reports epoch 2's own floor, never genesis's stale empty one");
        assertEquals(a.committedEpochId(), committed.epochId(), "the paired and standalone id accessors agree");
        assertEquals(a.committedLowerBounds(), committed.lowerBounds(),
                "the paired and standalone bounds accessors agree");
    }

    /** Concurrent snapshot requests coalesce into one round with one owner, yielding exactly one commit. */
    @Test
    void concurrentSnapshotRequestsCoalesceIntoOneCommit() {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        ParsleyEpochRuntime a = runtimeOver(log);
        ParsleyEpochRuntime b = runtimeOver(log);

        genesisAB(log, a, b);

        a.requestSnapshot("A");
        b.requestSnapshot("B");   // coalesces — a round is already open
        a.publishFrontier("A", ParsleyVectorClock.empty().observe(T1, 0, 3));
        b.publishFrontier("B", ParsleyVectorClock.empty().observe(T1, 0, 8));
        settle(log, a, b);

        assertEquals(2L, a.committedEpochId(), "the coalesced round yields exactly one new epoch");
        assertEquals(2L, b.committedEpochId(), "both nodes agree the coalesced round yielded one new epoch");
    }

    /** Two runtimes folding the same shared log reach the identical committed epoch and lower bounds. */
    @Test
    void everyRuntimeFoldsTheSameLogToTheSameDecision() {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        ParsleyEpochRuntime a = runtimeOver(log);
        ParsleyEpochRuntime b = runtimeOver(log);

        genesisAB(log, a, b);
        a.requestSnapshot("A");
        a.publishFrontier("A", ParsleyVectorClock.empty().observe(T1, 0, 4).observe(T2, 0, 6));
        b.publishFrontier("B", ParsleyVectorClock.empty().observe(T1, 0, 9).observe(T2, 0, 2));
        settle(log, a, b);

        assertEquals(a.committedEpochId(), b.committedEpochId(), "both nodes agree on the committed epoch");
        assertEquals(a.committedLowerBounds(), b.committedLowerBounds(), "both nodes agree on the lower bounds");
    }

    /** The round owner does not commit until its transport has caught up with the startup backlog. */
    @Test
    void ownerDoesNotCommitUntilBootstrapped() {
        GatedTransport transport = new GatedTransport();
        ParsleyEpochRuntime runtime = new ParsleyEpochRuntime(transport);
        join(runtime, "A", Set.of("A"));
        transport.seed(new ParsleyEpochEvent.SnapshotRequested("A"));   // opens a round A owns

        for (int i = 0; i < 5; i++) {
            runtime.runOnce();
        }
        assertEquals(0L, runtime.committedEpochId(), "the owner must not commit before the backlog is folded");
        assertFalse(runtime.isBootstrapped(), "the runtime is not yet bootstrapped");

        transport.markCaughtUp();
        for (int i = 0; i < 5; i++) {
            runtime.runOnce();
        }
        assertTrue(runtime.isBootstrapped(), "the runtime is bootstrapped once the transport has caught up");
        assertEquals(1L, runtime.committedEpochId(), "the owner commits genesis once bootstrapped");
    }

    /** Block-until-drained: a running member that never publishes keeps the round open — it is not
     * evicted — so no new epoch commits until it returns and publishes. */
    @Test
    void aSilentRunningMemberBlocksTheRoundUntilItPublishes() {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        ParsleyEpochRuntime a = runtimeOver(log);   // owns A
        ParsleyEpochRuntime b = runtimeOver(log);   // owns B (which then falls silent)

        genesisAB(log, a, b);   // A and B running at genesis (epoch 1)

        a.requestSnapshot("A");         // open round 2; A publishes, B stays silent
        a.publishFrontier("A", ParsleyVectorClock.empty().observe(T1, 0, 4));
        settle(log, a);                 // drive only A (B is "down")

        assertEquals(1L, a.committedEpochId(),
                "the silent member B keeps round 2 open, so no new epoch commits (never evicted)");
        assertTrue(a.isRunningMember("B"), "the silent member remains a running member — it is not evicted");

        b.publishFrontier("B", ParsleyVectorClock.empty().observe(T1, 0, 6));   // B returns and publishes
        settle(log, a, b);
        assertEquals(2L, a.committedEpochId(), "once B publishes, round 2 commits epoch 2");
        assertEquals(4L, a.committedLowerBounds().offsetFor(T1, 0),
                "the floor is min(A@4, B@6) = 4 over the published frontiers");
    }

    /** {@code owesPublication} tracks whether a running member has published for the open round, re-derived
     * purely from the folded log — so a member that restarts mid-round still owes and re-publishes. */
    @Test
    void owesPublicationTracksTheOpenRoundAndSurvivesRestart() {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        ParsleyEpochRuntime a = runtimeOver(log);
        ParsleyEpochRuntime b = runtimeOver(log);

        genesisAB(log, a, b);

        a.requestSnapshot("A");   // open round 2; neither has published
        settle(log, a, b);
        assertTrue(a.owesPublication("A"), "A owes a publication for the open round");
        assertTrue(b.owesPublication("B"), "B owes a publication for the open round");

        a.publishFrontier("A", ParsleyVectorClock.empty().observe(T1, 0, 2));
        settle(log, a, b);
        assertFalse(a.owesPublication("A"), "A no longer owes a publication once it has published");
        assertTrue(b.owesPublication("B"), "B still owes one, keeping the round open");
        assertEquals(1L, a.committedEpochId(), "the round has not committed while B is outstanding");

        ParsleyEpochRuntime bRestarted = runtimeOver(log);
        settle(log, bRestarted);
        assertTrue(bRestarted.owesPublication("B"),
                "a restarted runtime re-derives from the log alone that B still owes a publication");
    }

    /** A stalled local member (its task thread wedged in a join wait) is auto-published from its registered
     * completeness snapshot, so the round still completes from {@code runOnce()} alone. */
    @Test
    void aStalledLocalMemberIsAutoPublishedFromItsRegisteredCompletenessSnapshot() {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        ParsleyEpochRuntime a = runtimeOver(log);
        ParsleyEpochRuntime b = runtimeOver(log);

        genesisAB(log, a, b);

        ParsleyVectorClock aCompleteness = ParsleyVectorClock.empty().observe(T1, 0, 3).observe(T2, 0, 6);
        a.registerLocalCompleteness("A", () -> aCompleteness);   // A's only publication channel
        b.requestSnapshot("B");   // open round 2; A never calls publishFrontier directly
        b.publishFrontier("B", ParsleyVectorClock.empty().observe(T1, 0, 9).observe(T2, 0, 1));
        settle(log, a, b);

        assertEquals(2L, a.committedEpochId(), "round 2 committed using A's auto-published completeness alone");
        assertEquals(ParsleyVectorClock.empty().observe(T1, 0, 3).observe(T2, 0, 1), a.committedLowerBounds(),
                "the floor merge-mins A's auto-published snapshot with B's directly published frontier");
    }

    /** The drain mirror gates a graceful leave: {@code allLocalMembersDrained} is true only once every
     * local member has reported an empty buffer, and {@code hasRunningLocalMembers} reflects the fold. */
    @Test
    void theDrainMirrorAndRunningSetGateAGracefulLeave() {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        ParsleyEpochRuntime a = runtimeOver(log);   // hosts both local members A and B
        join(a, "A", AB);
        join(a, "B", AB);

        assertFalse(a.allLocalMembersDrained(), "no local member has reported a drained buffer yet");
        a.reportDrained("A", true);
        assertFalse(a.allLocalMembersDrained(), "only A is drained; B is still outstanding");
        a.reportDrained("B", true);
        assertTrue(a.allLocalMembersDrained(), "every local member has reported an empty buffer");
        a.reportDrained("B", false);
        assertFalse(a.allLocalMembersDrained(), "B's buffer filled again — no longer all drained");

        assertFalse(a.hasRunningLocalMembers(), "pending joiners are not yet running members");
        a.requestSnapshot("A");
        settle(log, a);
        assertTrue(a.hasRunningLocalMembers(), "A and B are running members after genesis");
    }

    /** {@code unregisterMember} drops a departed member from every local-bookkeeping set, so a rebalance
     * that migrates a task off this instance does not leave the member stuck forever in this view. */
    @Test
    void unregisterMemberDropsTheMemberFromLocalBookkeeping() {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        ParsleyEpochRuntime a = runtimeOver(log);
        join(a, "A", AB);
        join(a, "B", AB);
        a.reportDrained("A", true);
        assertFalse(a.allLocalMembersDrained(), "B has not reported drained, so the instance is not fully drained");

        a.unregisterMember("B");   // B's task left this instance (a rebalance), as ParsleyProcessor#close does

        assertTrue(a.allLocalMembersDrained(),
                "once the departed member is unregistered, the drain gate no longer waits on it");
        a.requestSnapshot("A");
        settle(log, a);
        assertTrue(a.hasRunningLocalMembers(), "A is still local and running");

        a.leaveLocalMembers();
        settle(log, a);
        assertFalse(a.isRunningMember("A"), "leaveLocalMembers() removed A, the sole remaining local member");
        assertTrue(a.isRunningMember("B"),
                "B was never appended a Leave — it was unregistered locally, not evicted from the domain");
    }

    /** An outbox intent whose append throws stays queued and is retried, not dropped (peek-then-remove). */
    @Test
    void anIntentWhoseAppendThrowsIsRetainedAndAppendedOnRetry() {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        FlakyTransport transport = new FlakyTransport(new InMemoryEpochTransport(log), 1, 0);
        ParsleyEpochRuntime runtime = new ParsleyEpochRuntime(transport);
        join(runtime, "A", Set.of("A"));   // enqueues one JoinRequested intent into the outbox

        assertThrows(RuntimeException.class, runtime::runOnce,
                "the transient append failure must surface from runOnce");
        assertEquals(0, log.events().size(),
                "nothing may reach the log while the append is failing — the intent stays queued");

        runtime.runOnce();   // the transport has recovered
        assertEquals(1, log.events().size(),
                "the retained intent must be appended on retry, never lost to the failed append");
        assertTrue(log.events().get(0) instanceof ParsleyEpochEvent.JoinRequested,
                "the appended event is the retained join");
    }

    /** The runtime's background thread survives a transient transport failure rather than dying and
     * wedging every join forever with {@code bootstrapped} stuck false. */
    @Test
    void theBackgroundThreadSurvivesTransientTransportFailuresAndStillBootstraps() {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        FlakyTransport transport = new FlakyTransport(new InMemoryEpochTransport(log), 1, 1);
        ParsleyEpochRuntime runtime = new ParsleyEpochRuntime(transport);
        join(runtime, "A", Set.of("A"));

        try {
            runtime.start();
            assertTimeoutPreemptively(java.time.Duration.ofSeconds(10), () -> {
                while (!runtime.isBootstrapped()) {
                    Thread.sleep(20L);
                }
            }, "the runtime thread must survive transient transport failures and still bootstrap");
        } finally {
            runtime.close();
        }

        assertTrue(log.events().stream().anyMatch(e -> e instanceof ParsleyEpochEvent.JoinRequested),
                "the enqueued join whose first append threw must still reach the log, not be lost");
    }

    // --- helpers --------------------------------------------------------------------------------

    private static ParsleyEpochRuntime runtimeOver(InMemoryEpochTransport.SharedLog log) {
        return new ParsleyEpochRuntime(new InMemoryEpochTransport(log));
    }

    /** Joins {@code member} as a single-task app declaring member-app roster {@code roster}. */
    private static void join(ParsleyEpochRuntime rt, String member, Set<String> roster) {
        rt.join(member, member, Set.of(member), Set.of(), Set.of(), roster, 1);
    }

    /** Bootstraps founders A and B (roster {A,B}) into genesis: both declare, A opens the genesis round,
     * and it commits epoch 1 at an empty floor, promoting both to running. */
    private static void genesisAB(InMemoryEpochTransport.SharedLog log, ParsleyEpochRuntime a, ParsleyEpochRuntime b) {
        join(a, "A", AB);
        join(b, "B", AB);
        settle(log, a, b);
        a.requestSnapshot("A");
        settle(log, a, b);
    }

    /**
     * A transport that throws on its first {@code appendFailures} append calls and first {@code
     * pollFailures} poll calls, then delegates — a transient broker blip that later recovers.
     */
    private static final class FlakyTransport implements ParsleyEpochTransport {
        private final ParsleyEpochTransport delegate;
        private final java.util.concurrent.atomic.AtomicInteger appendFailures;
        private final java.util.concurrent.atomic.AtomicInteger pollFailures;

        FlakyTransport(ParsleyEpochTransport delegate, int appendFailures, int pollFailures) {
            this.delegate = delegate;
            this.appendFailures = new java.util.concurrent.atomic.AtomicInteger(appendFailures);
            this.pollFailures = new java.util.concurrent.atomic.AtomicInteger(pollFailures);
        }

        @Override
        public void append(ParsleyEpochEvent event) {
            if (appendFailures.getAndDecrement() > 0) {
                throw new RuntimeException("simulated transport append failure");
            }
            delegate.append(event);
        }

        @Override
        public java.util.List<ParsleyEpochEvent> poll(java.time.Duration timeout) {
            if (pollFailures.getAndDecrement() > 0) {
                throw new RuntimeException("simulated transport poll failure");
            }
            return delegate.poll(timeout);
        }

        @Override
        public boolean caughtUp() {
            return delegate.caughtUp();
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    /** A transport whose {@link #caughtUp()} is controllable, to exercise the bootstrap gate. */
    private static final class GatedTransport implements ParsleyEpochTransport {
        private final java.util.List<ParsleyEpochEvent> events = new java.util.ArrayList<>();
        private int cursor;
        private boolean caughtUp;

        /** Appends an event as if produced by another node (bypassing the runtime's outbox). */
        void seed(ParsleyEpochEvent event) {
            events.add(event);
        }

        void markCaughtUp() {
            caughtUp = true;
        }

        @Override
        public void append(ParsleyEpochEvent event) {
            events.add(event);
        }

        @Override
        public java.util.List<ParsleyEpochEvent> poll(java.time.Duration timeout) {
            java.util.List<ParsleyEpochEvent> fresh = new java.util.ArrayList<>(events.subList(cursor, events.size()));
            cursor = events.size();
            return fresh;
        }

        @Override
        public boolean caughtUp() {
            return caughtUp;
        }

        @Override
        public void close() {
        }
    }

    /**
     * Drives each runtime's loop body until the shared log stops growing — three consecutive full passes
     * with no append means every outbox is flushed, every event folded, and no owner has a commit left to
     * write. Deterministic and terminating for these small, acyclic handshakes.
     */
    private static void settle(InMemoryEpochTransport.SharedLog log, ParsleyEpochRuntime... runtimes) {
        int quietPasses = 0;
        while (quietPasses < 3) {
            long before = log.events().size();
            for (ParsleyEpochRuntime runtime : runtimes) {
                runtime.runOnce();
            }
            quietPasses = (log.events().size() == before) ? quietPasses + 1 : 0;
        }
    }
}
