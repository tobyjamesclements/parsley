package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
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

        a.join("A");
        a.requestSnapshot("A");
        settle(log, a);

        assertEquals(1L, a.committedEpochId(), "the first join+snapshot commits epoch 1");
        assertTrue(a.committedLowerBounds().isEmpty(), "epoch 1 has an empty floor (nothing to bound)");
        assertEquals(1L, log.commitCount(), "exactly one commit was appended");
    }

    /**
     * Two nodes sharing one log both become running members, then the round owner alone collects both
     * published frontiers and commits their merge-min; the non-owner appends no commit.
     */
    @Test
    void ownerAloneCommitsMergeMinOfPublishedFrontiers() {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        ParsleyEpochRuntime a = runtimeOver(log);   // owns member A
        ParsleyEpochRuntime b = runtimeOver(log);   // owns member B

        // Bootstrap: both join, then A opens the first round — running is still empty, so it commits
        // epoch 1 vacuously and promotes both A and B to running members.
        a.join("A");
        b.join("B");
        settle(log, a, b);
        a.requestSnapshot("A");
        settle(log, a, b);
        assertEquals(1L, a.committedEpochId(), "epoch 1 is committed, promoting both to running");

        // Second round: A owns it (its request is first after the epoch-1 commit); both publish; A alone
        // commits epoch 2 with the merge-min of the two frontiers.
        ParsleyClock fA = ParsleyClock.empty().observe(T1, 0, 10).observe(T2, 0, 5);
        ParsleyClock fB = ParsleyClock.empty().observe(T1, 0, 7).observe(T2, 0, 9);
        a.requestSnapshot("A");
        a.publishFrontier("A", fA);
        b.publishFrontier("B", fB);
        settle(log, a, b);

        ParsleyClock expected = ParsleyClock.empty().observe(T1, 0, 7).observe(T2, 0, 5);
        assertEquals(2L, a.committedEpochId(), "epoch 2 is committed");
        assertEquals(expected, a.committedLowerBounds(), "epoch 2 floor is the per-coordinate min of A and B");
        assertEquals(2L, log.commitCount(), "exactly two commits (epoch 1 and epoch 2); the non-owner added none");
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

        a.join("A");
        b.join("B");
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
        assertEquals(2L, log.commitCount(), "only one commit for the coalesced round (plus epoch 1)");
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

        a.join("A");
        b.join("B");
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
        runtime.join("A");                                          // A is local to this runtime
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
