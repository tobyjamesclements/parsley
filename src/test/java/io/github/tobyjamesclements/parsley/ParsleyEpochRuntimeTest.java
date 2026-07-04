package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ParsleyEpochRuntime}: the per-instance driver that folds the shared {@code
 * epoch-events} log and, for a round one of its own members owns, performs the owner's collect → commit.
 * Every test drives {@link ParsleyEpochRuntime#runOnce()} synchronously against the in-memory transport
 * double, so the exact production loop body runs with no thread and no timing flakiness.
 */
class ParsleyEpochRuntimeTest {

    private static final Uuid T1 = Uuid.randomUuid();
    private static final Uuid T2 = Uuid.randomUuid();

    /**
     * A single node joining an empty topology and requesting a snapshot drives one commit: epoch 1 with
     * an empty floor (no running members to bound), and the node becomes a running member.
     */
    @Test
    void singleNodeJoinAndSnapshotCommitsEpochOne() {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        ParsleyEpochRuntime a = runtimeOver(log);

        a.join("A", Set.of(), Set.of());
        a.requestSnapshot("A");
        settle(log, a);

        assertEquals(1L, a.committedEpochId(), "the first join+snapshot commits epoch 1");
        assertTrue(a.committedLowerBounds().isEmpty(), "epoch 1 has an empty floor (nothing to bound)");
        assertEquals(1L, log.commitCount(), "exactly one commit was appended");
    }

    /**
     * Two nodes sharing one log both become running members, then a complete round commits their
     * frontiers' merge-min. With any-node commit both nodes may append the (identical) commit; dedup makes
     * it one effective epoch, so the decision is read via {@code committedEpochId} + the floor, not the
     * append count.
     */
    @Test
    void aCompleteRoundCommitsTheMergeMinOfPublishedFrontiers() {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        ParsleyEpochRuntime a = runtimeOver(log);   // owns member A
        ParsleyEpochRuntime b = runtimeOver(log);   // owns member B

        // Bootstrap: both join, then A opens the first round — running is still empty, so it commits
        // epoch 1 vacuously and promotes both A and B to running members.
        a.join("A", Set.of(), Set.of());
        b.join("B", Set.of(), Set.of());
        settle(log, a, b);
        a.requestSnapshot("A");
        settle(log, a, b);
        assertEquals(1L, a.committedEpochId(), "epoch 1 is committed, promoting both to running");

        // Second round: both publish and the round commits epoch 2 with the merge-min of the frontiers.
        ParsleyClock fA = ParsleyClock.empty().observe(T1, 0, 10).observe(T2, 0, 5);
        ParsleyClock fB = ParsleyClock.empty().observe(T1, 0, 7).observe(T2, 0, 9);
        a.requestSnapshot("A");
        a.publishFrontier("A", fA);
        b.publishFrontier("B", fB);
        settle(log, a, b);

        ParsleyClock expected = ParsleyClock.empty().observe(T1, 0, 7).observe(T2, 0, 5);
        assertEquals(2L, a.committedEpochId(), "epoch 2 is committed");
        assertEquals(expected, a.committedLowerBounds(), "epoch 2 floor is the per-coordinate min of A and B");
        assertEquals(a.committedLowerBounds(), b.committedLowerBounds(),
                "both nodes agree on the floor regardless of which node's commit won the dedup");
    }

    /**
     * When two nodes request a snapshot concurrently, the requests coalesce into one round with one owner,
     * yielding exactly one commit — never two competing cuts.
     */
    @Test
    void concurrentSnapshotRequestsCoalesceIntoOneCommit() {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        ParsleyEpochRuntime a = runtimeOver(log);
        ParsleyEpochRuntime b = runtimeOver(log);

        a.join("A", Set.of(), Set.of());
        b.join("B", Set.of(), Set.of());
        settle(log, a, b);
        a.requestSnapshot("A");
        settle(log, a, b);   // epoch 1: both running

        // Both nodes propose a snapshot at once; the first on the log owns, the second coalesces.
        a.requestSnapshot("A");
        b.requestSnapshot("B");
        a.publishFrontier("A", ParsleyClock.empty().observe(T1, 0, 3));
        b.publishFrontier("B", ParsleyClock.empty().observe(T1, 0, 8));
        settle(log, a, b);

        assertEquals(2L, a.committedEpochId(), "the coalesced round yields exactly one new epoch");
        assertEquals(2L, b.committedEpochId(), "both nodes agree the coalesced round yielded exactly one new epoch");
    }

    /**
     * Two runtimes folding the same shared log reach the identical committed epoch and lower bounds — the
     * leaderless determinism guarantee: agreement without a leader, from the log's total order alone.
     */
    @Test
    void everyRuntimeFoldsTheSameLogToTheSameDecision() {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        ParsleyEpochRuntime a = runtimeOver(log);
        ParsleyEpochRuntime b = runtimeOver(log);

        a.join("A", Set.of(), Set.of());
        b.join("B", Set.of(), Set.of());
        settle(log, a, b);
        a.requestSnapshot("A");
        settle(log, a, b);
        a.requestSnapshot("A");
        a.publishFrontier("A", ParsleyClock.empty().observe(T1, 0, 4).observe(T2, 0, 6));
        b.publishFrontier("B", ParsleyClock.empty().observe(T1, 0, 9).observe(T2, 0, 2));
        settle(log, a, b);

        assertEquals(a.committedEpochId(), b.committedEpochId(), "both nodes agree on the committed epoch");
        assertEquals(a.committedLowerBounds(), b.committedLowerBounds(), "both nodes agree on the lower bounds");
    }

    /**
     * The round owner does not commit until its transport has caught up with the startup backlog: a
     * just-started runtime that has not yet folded the whole log must not decide a round against a
     * topology it has only partially observed (which would commit a stale epoch).
     */
    @Test
    void ownerDoesNotCommitUntilBootstrapped() {
        GatedTransport transport = new GatedTransport();
        ParsleyEpochRuntime runtime = new ParsleyEpochRuntime(transport);
        runtime.join("A", Set.of(), Set.of());                                          // A is local to this runtime
        transport.seed(new EpochEvent.SnapshotRequested("A"));      // opens a round A owns

        // Not caught up yet: the round is folded open but the owner must not commit.
        for (int i = 0; i < 5; i++) {
            runtime.runOnce();
        }
        assertEquals(0L, runtime.committedEpochId(), "the owner must not commit before the backlog is folded");
        assertFalse(runtime.isBootstrapped(), "the runtime is not yet bootstrapped");

        // Caught up: the owner may now commit the round it owns.
        transport.markCaughtUp();
        for (int i = 0; i < 5; i++) {
            runtime.runOnce();
        }
        assertTrue(runtime.isBootstrapped(), "the runtime is bootstrapped once the transport has caught up");
        assertEquals(1L, runtime.committedEpochId(), "the owner commits the round once bootstrapped");
    }

    /**
     * A running member that never publishes is evicted once the round has waited past the eviction
     * timeout, so the round completes and commits — a gone member cannot freeze the epoch forever.
     */
    @Test
    void aSilentRunningMemberIsEvictedSoTheRoundCommits() {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        // Seed R as a running member (epoch 1); it then falls silent.
        InMemoryEpochTransport seeder = new InMemoryEpochTransport(log);
        seeder.append(new EpochEvent.JoinRequested("R", Set.of(), Set.of()));
        seeder.append(new EpochEvent.SnapshotRequested("R"));
        seeder.append(new EpochEvent.EpochCommitted(1, ParsleyClock.empty()));

        ParsleyEpochRuntime a = new ParsleyEpochRuntime(new InMemoryEpochTransport(log), java.time.Duration.ZERO);
        a.join("A", Set.of(), Set.of());
        a.requestSnapshot("A");   // opens round 2; running = {R}; R never publishes
        settle(log, a);

        assertEquals(2L, a.committedEpochId(), "the silent member is evicted so the round commits epoch 2");
        assertFalse(a.isRunningMember("R"), "the evicted member is no longer a running member");
    }

    /** A local member evicted by another node is surfaced via {@code isEvicted}, so the task can fail and re-join. */
    @Test
    void aLocalMemberEvictedByAnotherNodeIsSurfaced() {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        ParsleyEpochRuntime a = runtimeOver(log);
        a.join("A", Set.of(), Set.of());
        settle(log, a);

        new InMemoryEpochTransport(log).append(new EpochEvent.Leave("A"));   // another node evicts A
        settle(log, a);

        assertTrue(a.isEvicted("A"), "a local member evicted by another node is surfaced for re-join");
    }

    /**
     * The eviction flag clears once the member is re-admitted (running again), so a task that blocked back
     * in and re-adopted the floor does not keep failing in a loop — the fix for a crashed-then-evicted
     * member restarting.
     */
    @Test
    void anEvictionFlagClearsOnceTheMemberIsReAdmitted() {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        ParsleyEpochRuntime a = runtimeOver(log);
        a.join("A", Set.of(), Set.of());
        a.requestSnapshot("A");
        settle(log, a);
        assertTrue(a.isRunningMember("A"), "A is a running member (epoch 1)");

        new InMemoryEpochTransport(log).append(new EpochEvent.Leave("A"));   // A is evicted
        settle(log, a);
        assertTrue(a.isEvicted("A"), "while evicted and not running, A is flagged");

        a.join("A", Set.of(), Set.of());                 // A restarts and re-joins
        a.requestSnapshot("A");
        settle(log, a);
        assertTrue(a.isRunningMember("A"), "A is re-admitted as a running member");
        assertFalse(a.isEvicted("A"), "the eviction flag clears on re-admission, so the task does not loop");
    }

    private static ParsleyEpochRuntime runtimeOver(InMemoryEpochTransport.SharedLog log) {
        return new ParsleyEpochRuntime(new InMemoryEpochTransport(log));
    }

    /** A transport whose {@link #caughtUp()} is controllable, to exercise the bootstrap gate. */
    private static final class GatedTransport implements ParsleyEpochTransport {
        private final java.util.List<EpochEvent> events = new java.util.ArrayList<>();
        private int cursor;
        private boolean caughtUp;

        /** Appends an event as if produced by another node (bypassing the runtime's outbox). */
        void seed(EpochEvent event) {
            events.add(event);
        }

        void markCaughtUp() {
            caughtUp = true;
        }

        @Override
        public void append(EpochEvent event) {
            events.add(event);
        }

        @Override
        public java.util.List<EpochEvent> poll(java.time.Duration timeout) {
            java.util.List<EpochEvent> fresh = new java.util.ArrayList<>(events.subList(cursor, events.size()));
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
