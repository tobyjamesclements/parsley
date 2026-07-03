package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ParsleyEpochLog}: the deterministic fold over the {@code epoch-events} log that gives
 * every node the same view of round ownership, membership, completeness, and the committed lower bounds.
 */
class ParsleyEpochLogTest {

    private static final Uuid T1_ID = Uuid.randomUuid();
    private static final Uuid T2_ID = Uuid.randomUuid();

    /**
     * The first node joining an empty topology and requesting a snapshot commits epoch 1 with an empty
     * floor (no running members to bound), and becomes a running member.
     */
    @Test
    void firstJoinIntoEmptyTopologyCommitsEpochOneWithNoFloor() {
        ParsleyEpochLog log = new ParsleyEpochLog();
        log.apply(new EpochEvent.JoinRequested("A"));
        log.apply(new EpochEvent.SnapshotRequested("A"));

        assertTrue(log.isRoundOpen(), "the snapshot request opens a round");
        assertEquals("A", log.roundOwner(), "the first requester owns the round");
        assertTrue(log.isRoundComplete(), "with no running members the round is vacuously complete");

        EpochEvent.EpochCommitted commit = log.proposeCommit();
        assertEquals(1L, commit.epochId(), "the first commit is epoch 1");
        assertTrue(commit.lowerBounds().isEmpty(), "epoch 1 has an empty floor (nothing to bound)");

        log.apply(commit);
        assertEquals(1L, log.committedEpochId(), "the settled epoch advances to 1");
        assertFalse(log.isRoundOpen(), "the round closes on commit");
        assertEquals(java.util.Set.of("A"), log.runningMembers(), "the joiner is now a running member");
    }

    /** The first snapshot request after a commit owns the round; later requests coalesce (owner unchanged). */
    @Test
    void concurrentSnapshotRequestsCoalesceIntoOneRoundOwnedByTheFirst() {
        ParsleyEpochLog log = bootstrappedWithRunningMember("A");
        log.apply(new EpochEvent.JoinRequested("B"));

        log.apply(new EpochEvent.SnapshotRequested("B"));
        log.apply(new EpochEvent.SnapshotRequested("A"));   // coalesces — a round is already open

        assertTrue(log.isRoundOpen(), "a round is open");
        assertEquals("B", log.roundOwner(), "the first requester (B) owns; the second coalesces");
    }

    /** A round is not complete until every running member has published its frontier. */
    @Test
    void roundIsNotCompleteUntilEveryRunningMemberPublishes() {
        ParsleyEpochLog log = bootstrappedWithRunningMember("A");
        log.apply(new EpochEvent.JoinRequested("B"));
        commitRound(log, "B");                              // A publishes, epoch 2, A+B running
        // Now A and B both running. Open a new round.
        log.apply(new EpochEvent.SnapshotRequested("A"));

        assertFalse(log.isRoundComplete(), "no member has published yet");
        log.apply(new EpochEvent.FrontierPublished("A", ParsleyClock.empty().observe(T1_ID, 0, 5)));
        assertFalse(log.isRoundComplete(), "only A has published; B is still outstanding");
        log.apply(new EpochEvent.FrontierPublished("B", ParsleyClock.empty().observe(T1_ID, 0, 9)));
        assertTrue(log.isRoundComplete(), "both running members have published");
    }

    /**
     * The committed lower bounds are the per-coordinate {@code mergeMin} of the published frontiers: the
     * minimum over the members that observed a coordinate, keeping a coordinate seen on only one side.
     */
    @Test
    void lowerBoundsAreTheMergeMinOfPublishedFrontiers() {
        ParsleyEpochLog log = bootstrappedWithRunningMember("A");
        log.apply(new EpochEvent.JoinRequested("B"));
        commitRound(log, "B");                              // A+B running (epoch 2)

        log.apply(new EpochEvent.SnapshotRequested("A"));
        // A observes T1@10 and T2@4; B observes T1@6 only.
        log.apply(new EpochEvent.FrontierPublished("A",
                ParsleyClock.empty().observe(T1_ID, 0, 10).observe(T2_ID, 0, 4)));
        log.apply(new EpochEvent.FrontierPublished("B",
                ParsleyClock.empty().observe(T1_ID, 0, 6)));

        EpochEvent.EpochCommitted commit = log.proposeCommit();
        assertEquals(3L, commit.epochId(), "the epoch id is strictly increasing");
        assertEquals(6L, commit.lowerBounds().offsetFor(T1_ID, 0),
                "T1 is bounded at min(10, 6) = 6 — both members observed it");
        assertEquals(4L, commit.lowerBounds().offsetFor(T2_ID, 0),
                "T2 is bounded at 4 — only A observed it, so it is kept at A's value");
    }

    /** A commit advances the epoch monotonically, promotes pending joiners to running, and clears the round. */
    @Test
    void commitAdvancesEpochPromotesJoinersAndClearsTheRound() {
        ParsleyEpochLog log = bootstrappedWithRunningMember("A");   // epoch 1, A running
        log.apply(new EpochEvent.JoinRequested("B"));               // B pending
        log.apply(new EpochEvent.SnapshotRequested("A"));
        log.apply(new EpochEvent.FrontierPublished("A", ParsleyClock.empty().observe(T1_ID, 0, 5)));

        log.apply(log.proposeCommit());

        assertEquals(2L, log.committedEpochId(), "the epoch advanced to 2");
        assertEquals(java.util.Set.of("A", "B"), log.runningMembers(), "the pending joiner B is now running");
        assertFalse(log.isRoundOpen(), "the round is cleared");
        assertEquals(3L, log.nextEpochId(), "the next epoch id continues monotonically");
    }

    /** A frontier publication with no round open, or from a not-yet-running joiner, is ignored. */
    @Test
    void strayFrontierPublicationsAreIgnored() {
        ParsleyEpochLog log = bootstrappedWithRunningMember("A");   // A running, no round open
        // No round open: this publication must not count.
        log.apply(new EpochEvent.FrontierPublished("A", ParsleyClock.empty().observe(T1_ID, 0, 5)));

        log.apply(new EpochEvent.JoinRequested("B"));
        log.apply(new EpochEvent.SnapshotRequested("A"));
        // B is a pending joiner, not running: its publication must not count toward completeness.
        log.apply(new EpochEvent.FrontierPublished("B", ParsleyClock.empty().observe(T1_ID, 0, 9)));
        assertFalse(log.isRoundComplete(), "only A (running) is awaited; A has not published in this round");

        log.apply(new EpochEvent.FrontierPublished("A", ParsleyClock.empty().observe(T1_ID, 0, 5)));
        assertTrue(log.isRoundComplete(), "the round completes once the running member A publishes");
    }

    /** {@link ParsleyEpochLog#proposeCommit()} rejects a round that is not complete. */
    @Test
    void proposeCommitRejectsAnIncompleteRound() {
        ParsleyEpochLog log = bootstrappedWithRunningMember("A");
        log.apply(new EpochEvent.JoinRequested("B"));
        commitRound(log, "B");                              // A+B running
        log.apply(new EpochEvent.SnapshotRequested("A"));   // round open, nobody published

        assertThrows(IllegalStateException.class, log::proposeCommit,
                "committing before every running member has published must be rejected");
    }

    // --- helpers --------------------------------------------------------------------------------

    /** A log where {@code member} has joined and been committed into epoch 1 (so it is running). */
    private static ParsleyEpochLog bootstrappedWithRunningMember(String member) {
        ParsleyEpochLog log = new ParsleyEpochLog();
        log.apply(new EpochEvent.JoinRequested(member));
        log.apply(new EpochEvent.SnapshotRequested(member));
        log.apply(log.proposeCommit());                     // vacuously complete (no prior running members)
        return log;
    }

    /** Opens a round owned by {@code owner}, has every currently-running member publish an empty frontier,
     * and commits it — advancing the epoch. */
    private static void commitRound(ParsleyEpochLog log, String owner) {
        log.apply(new EpochEvent.SnapshotRequested(owner));
        for (String member : log.runningMembers()) {
            log.apply(new EpochEvent.FrontierPublished(member, ParsleyClock.empty()));
        }
        log.apply(log.proposeCommit());
    }
}
