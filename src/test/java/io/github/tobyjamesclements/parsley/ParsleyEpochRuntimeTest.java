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
     * Regression test for BACKLOG.md's LOW item: {@code committedEpochId()} and {@code
     * committedLowerBounds()} are independent volatile reads, so a caller pairing them across two
     * separate calls risks a commit landing in between, stamping id-from-commit-N with
     * bounds-from-commit-{@code N-1}. {@link ParsleyEpochRuntime#committedEpoch()} returns both from one
     * volatile snapshot instead.
     *
     * <p>Drives the same two-commit sequence as {@link #aCompleteRoundCommitsTheMergeMinOfPublishedFrontiers}
     * (epoch 1 with an empty floor, then epoch 2 with a distinct floor) and asserts {@code
     * committedEpoch()} always reports the id paired with <em>that same commit's</em> bounds — in
     * particular, after the second commit, the pairing must reflect epoch 2's floor, never epoch 1's
     * stale empty one.
     *
     * Asserts the {@code (epochId, lowerBounds)} pairing is internally consistent at both commits.
     */
    @Test
    void committedEpochReturnsTheIdAndBoundsFromTheSameCommitTogether() {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        ParsleyEpochRuntime a = runtimeOver(log);
        ParsleyEpochRuntime b = runtimeOver(log);

        a.join("A", Set.of(), Set.of());
        b.join("B", Set.of(), Set.of());
        settle(log, a, b);
        a.requestSnapshot("A");
        settle(log, a, b);

        assertEquals(1L, a.committedEpoch().epochId(), "epoch 1's pairing must report id 1");
        assertTrue(a.committedEpoch().lowerBounds().isEmpty(), "epoch 1's pairing must report its own empty floor");

        ParsleyClock fA = ParsleyClock.empty().observe(T1, 0, 10).observe(T2, 0, 5);
        ParsleyClock fB = ParsleyClock.empty().observe(T1, 0, 7).observe(T2, 0, 9);
        a.requestSnapshot("A");
        a.publishFrontier("A", fA);
        b.publishFrontier("B", fB);
        settle(log, a, b);

        ParsleyClock epoch2Floor = ParsleyClock.empty().observe(T1, 0, 7).observe(T2, 0, 5);
        ParsleyEpochRuntime.CommittedEpoch committed = a.committedEpoch();
        assertEquals(2L, committed.epochId(), "epoch 2's pairing must report id 2");
        assertEquals(epoch2Floor, committed.lowerBounds(),
                "epoch 2's pairing must report epoch 2's own floor, never epoch 1's stale empty one");
        assertEquals(a.committedEpochId(), committed.epochId(),
                "the paired accessor and the standalone id accessor must agree");
        assertEquals(a.committedLowerBounds(), committed.lowerBounds(),
                "the paired accessor and the standalone bounds accessor must agree");
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
        transport.seed(new ParsleyEpochEvent.SnapshotRequested("A"));      // opens a round A owns

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
     * Block-until-drained: a running member that never publishes keeps the round open — it is not evicted —
     * so no new epoch commits until it returns and publishes. This is the causal-safety choice: a member's
     * un-drained buffer can never be stranded below a floor committed without it.
     */
    @Test
    void aSilentRunningMemberBlocksTheRoundUntilItPublishes() {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        // Seed R as a running member (epoch 1); it then falls silent.
        InMemoryEpochTransport seeder = new InMemoryEpochTransport(log);
        seeder.append(new ParsleyEpochEvent.JoinRequested("R", Set.of(), Set.of()));
        seeder.append(new ParsleyEpochEvent.SnapshotRequested("R"));
        seeder.append(new ParsleyEpochEvent.EpochCommitted(1, ParsleyClock.empty()));

        ParsleyEpochRuntime a = runtimeOver(log);
        a.join("A", Set.of(), Set.of());
        a.requestSnapshot("A");   // opens round 2; running = {R}; R has not published
        settle(log, a);

        assertEquals(1L, a.committedEpochId(),
                "the silent member keeps round 2 open, so no new epoch commits (never evicted)");
        assertTrue(a.isRunningMember("R"), "the silent member remains a running member — it is not evicted");
        assertFalse(a.isRunningMember("A"), "A is still a pending joiner (round 2 has not committed)");

        // R returns and publishes; the round now completes and commits epoch 2 over its frontier.
        new InMemoryEpochTransport(log).append(
                new ParsleyEpochEvent.FrontierPublished("R", ParsleyClock.empty().observe(T1, 0, 4)));
        settle(log, a);

        assertEquals(2L, a.committedEpochId(), "once the member publishes, the round commits epoch 2");
        assertEquals(ParsleyClock.empty().observe(T1, 0, 4), a.committedLowerBounds(),
                "the floor is the returned member's published frontier");
        assertTrue(a.isRunningMember("A"), "the commit promotes the pending joiner A to running");
    }

    /**
     * {@code owesPublication} tracks whether a running member has published for the open round, and is
     * re-derived purely from the folded log — so a member that restarts mid-round still sees it owes a
     * publication and re-publishes, rather than deadlocking the round because a one-shot marker was already
     * consumed. This is the mechanism that keeps block-until-drained live across restarts.
     */
    @Test
    void owesPublicationTracksTheOpenRoundAndSurvivesRestart() {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        ParsleyEpochRuntime a = runtimeOver(log);   // owns A
        ParsleyEpochRuntime b = runtimeOver(log);   // owns B

        a.join("A", Set.of(), Set.of());
        b.join("B", Set.of(), Set.of());
        settle(log, a, b);
        a.requestSnapshot("A");
        settle(log, a, b);   // epoch 1: A and B running

        a.requestSnapshot("A");   // open round 2; neither has published
        settle(log, a, b);
        assertTrue(a.owesPublication("A"), "A owes a publication for the open round");
        assertTrue(b.owesPublication("B"), "B owes a publication for the open round");

        a.publishFrontier("A", ParsleyClock.empty().observe(T1, 0, 2));
        settle(log, a, b);
        assertFalse(a.owesPublication("A"), "A no longer owes a publication once it has published");
        assertTrue(b.owesPublication("B"), "B still owes one, keeping the round open");
        assertEquals(1L, a.committedEpochId(), "the round has not committed while B is outstanding");

        // B "restarts": a fresh runtime folding the same log must still derive that B owes a publication.
        ParsleyEpochRuntime bRestarted = runtimeOver(log);
        settle(log, bRestarted);
        assertTrue(bRestarted.owesPublication("B"),
                "a restarted runtime re-derives from the log alone that B still owes a publication");
    }

    /**
     * Regression test for the BACKLOG.md deadlock: {@code awaitJoinCommit} blocks a joiner's task thread
     * inside {@code init()}, and Kafka Streams runs every task on a {@code StreamThread} sequentially, so a
     * running member sharing that thread can never run {@code pollEpochCoordination()} to publish its
     * frontier — the round the joiner opened would never complete. Here A never calls {@code
     * publishFrontier} directly (modelling exactly that stuck task thread); its only channel to publish is
     * the completeness snapshot registered via {@link ParsleyEpochRuntime#registerLocalCompleteness}. The
     * round must still complete from {@code runOnce()} alone.
     */
    @Test
    void aStalledLocalMemberIsAutoPublishedFromItsRegisteredCompletenessSnapshot() {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        ParsleyEpochRuntime a = runtimeOver(log);
        ParsleyEpochRuntime b = runtimeOver(log);

        a.join("A", Set.of(), Set.of());
        b.join("B", Set.of(), Set.of());
        settle(log, a, b);
        a.requestSnapshot("A");
        settle(log, a, b);   // epoch 1: A and B running

        ParsleyClock aCompleteness = ParsleyClock.empty().observe(T1, 0, 3).observe(T2, 0, 6);
        a.registerLocalCompleteness("A", () -> aCompleteness);   // A's only publication channel — never publishFrontier directly
        b.requestSnapshot("B");   // open round 2; A never calls publishFrontier
        b.publishFrontier("B", ParsleyClock.empty().observe(T1, 0, 9).observe(T2, 0, 1));
        settle(log, a, b);

        assertEquals(2L, a.committedEpochId(), "round 2 committed using A's auto-published completeness alone");
        assertEquals(ParsleyClock.empty().observe(T1, 0, 3).observe(T2, 0, 1), a.committedLowerBounds(),
                "the floor merge-mins A's auto-published snapshot with B's directly published frontier");
    }

    /**
     * The drain mirror gates a graceful leave: {@code allLocalMembersDrained} is true only once every local
     * member has reported an empty buffer, and {@code hasRunningLocalMembers} reflects the fold's running
     * set — the two conditions {@link ParsleyCoordination#leave()} waits on across its drain and remove phases.
     */
    @Test
    void theDrainMirrorAndRunningSetGateAGracefulLeave() {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        ParsleyEpochRuntime a = runtimeOver(log);
        a.join("A", Set.of(), Set.of());
        a.join("B", Set.of(), Set.of());

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
        assertTrue(a.hasRunningLocalMembers(), "A and B are running members after the commit");
    }

    /**
     * Regression test for the BACKLOG.md gap: {@code unregisterMember} must drop a departed member from
     * every local-bookkeeping set — {@code allLocalMembersDrained}'s drain gate and {@code
     * hasRunningLocalMembers}'s scope — so a rebalance that migrates a task off this instance (which calls
     * this from {@code ParsleyProcessor#close}) does not leave the departed member stuck forever in this
     * instance's view. Without the call, B's last-reported (non-drained) state would never update again
     * here, and B would still count toward this instance's local membership even though it now runs
     * elsewhere.
     */
    @Test
    void unregisterMemberDropsTheMemberFromLocalBookkeeping() {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        ParsleyEpochRuntime a = runtimeOver(log);
        a.join("A", Set.of(), Set.of());
        a.join("B", Set.of(), Set.of());
        a.reportDrained("A", true);
        // B never reports drained — mirrors a task whose StreamThread migrated away mid-flight, so no
        // further reportDrained ever arrives for it on this instance.
        assertFalse(a.allLocalMembersDrained(), "B has not reported drained, so the instance is not fully drained");

        a.unregisterMember("B");   // B's task left this instance (e.g. a rebalance), as ParsleyProcessor#close does

        assertTrue(a.allLocalMembersDrained(),
                "once the departed member is unregistered, the drain gate no longer waits on it");
        a.requestSnapshot("A");
        settle(log, a);
        assertTrue(a.hasRunningLocalMembers(), "A is still local and running");

        // A leave() driven now must not touch B: leaveLocalMembers() only iterates the (now A-only) local set.
        a.leaveLocalMembers();
        settle(log, a);
        assertFalse(a.isRunningMember("A"), "leaveLocalMembers() removed A, the sole remaining local member");
        assertTrue(a.isRunningMember("B"),
                "B was never appended a Leave — it was unregistered locally, not evicted from the domain");
    }

    private static ParsleyEpochRuntime runtimeOver(InMemoryEpochTransport.SharedLog log) {
        return new ParsleyEpochRuntime(new InMemoryEpochTransport(log));
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
