package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import java.util.Set;
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
        log.apply(new ParsleyEpochEvent.JoinRequested("A", Set.of(), Set.of()));
        log.apply(new ParsleyEpochEvent.SnapshotRequested("A"));

        assertTrue(log.isRoundOpen(), "the snapshot request opens a round");
        assertEquals("A", log.roundOwner(), "the first requester owns the round");
        assertTrue(log.isRoundComplete(), "with no running members the round is vacuously complete");

        ParsleyEpochEvent.EpochCommitted commit = log.proposeCommit();
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
        log.apply(new ParsleyEpochEvent.JoinRequested("B", Set.of(), Set.of()));

        log.apply(new ParsleyEpochEvent.SnapshotRequested("B"));
        log.apply(new ParsleyEpochEvent.SnapshotRequested("A"));   // coalesces — a round is already open

        assertTrue(log.isRoundOpen(), "a round is open");
        assertEquals("B", log.roundOwner(), "the first requester (B) owns; the second coalesces");
    }

    /** A round is not complete until every running member has published its frontier. */
    @Test
    void roundIsNotCompleteUntilEveryRunningMemberPublishes() {
        ParsleyEpochLog log = bootstrappedWithRunningMember("A");
        log.apply(new ParsleyEpochEvent.JoinRequested("B", Set.of(), Set.of()));
        commitRound(log, "B");                              // A publishes, epoch 2, A+B running
        // Now A and B both running. Open a new round.
        log.apply(new ParsleyEpochEvent.SnapshotRequested("A"));

        assertFalse(log.isRoundComplete(), "no member has published yet");
        log.apply(new ParsleyEpochEvent.FrontierPublished("A", ParsleyClock.empty().observe(T1_ID, 0, 5)));
        assertFalse(log.isRoundComplete(), "only A has published; B is still outstanding");
        log.apply(new ParsleyEpochEvent.FrontierPublished("B", ParsleyClock.empty().observe(T1_ID, 0, 9)));
        assertTrue(log.isRoundComplete(), "both running members have published");
    }

    /**
     * The committed lower bounds are the per-coordinate {@code mergeMin} of the published frontiers: the
     * minimum over the members that observed a coordinate, keeping a coordinate seen on only one side.
     */
    @Test
    void lowerBoundsAreTheMergeMinOfPublishedFrontiers() {
        ParsleyEpochLog log = bootstrappedWithRunningMember("A");
        log.apply(new ParsleyEpochEvent.JoinRequested("B", Set.of(), Set.of()));
        commitRound(log, "B");                              // A+B running (epoch 2)

        log.apply(new ParsleyEpochEvent.SnapshotRequested("A"));
        // A observes T1@10 and T2@4; B observes T1@6 only.
        log.apply(new ParsleyEpochEvent.FrontierPublished("A",
                ParsleyClock.empty().observe(T1_ID, 0, 10).observe(T2_ID, 0, 4)));
        log.apply(new ParsleyEpochEvent.FrontierPublished("B",
                ParsleyClock.empty().observe(T1_ID, 0, 6)));

        ParsleyEpochEvent.EpochCommitted commit = log.proposeCommit();
        assertEquals(3L, commit.epochId(), "the epoch id is strictly increasing");
        assertEquals(6L, commit.lowerBounds().offsetFor(T1_ID, 0),
                "T1 is bounded at min(10, 6) = 6 — both members observed it");
        assertEquals(4L, commit.lowerBounds().offsetFor(T2_ID, 0),
                "T2 is bounded at 4 — only A observed it, so it is kept at A's value");
    }

    /**
     * Regression test for BACKLOG.md's LOW item: the committed floor is not actually monotonic across
     * epochs from the raw {@code mergeMin} alone. A member admitted mid-round that consumes from {@code
     * earliest} publishes completeness far behind the already-committed floor, and {@code mergeMin}
     * would drag a shared coordinate's floor <em>backwards</em> on promotion — every consumer of the
     * floor tolerates that except {@code ParsleyFrontier#pruneStaleOrphans}, where a regression below an
     * already-pruned orphan entry would hold that coordinate's dependents forever again.
     *
     * <p>Epoch 2 commits {@code T1@5, T2@3} (A alone). B then joins and is promoted to running by that
     * same commit. Epoch 3's round has both publish: A has advanced on both coordinates ({@code
     * T1@10, T2@8}), but B — freshly admitted, reading from {@code earliest} — is far behind on T1
     * ({@code T1@2}) though ahead on T2 ({@code T2@9}). The raw {@code mergeMin} would report {@code
     * T1@2} (a regression from 5) and {@code T2@8} (genuine progress from 3, no clamp needed).
     *
     * Asserts epoch 3's floor keeps T1 clamped at the previous commit's value (5, not the regressed 2)
     * while T2 advances normally to 8 — the clamp only ever raises a floor back to where it already was,
     * never further, and never touches a coordinate that did not regress.
     */
    @Test
    void proposeCommitClampsTheFloorToNeverRegressBelowThePreviousCommit() {
        ParsleyEpochLog log = bootstrappedWithRunningMember("A");   // epoch 1, A running, floor empty
        log.apply(new ParsleyEpochEvent.JoinRequested("B", Set.of(), Set.of()));   // B pending

        log.apply(new ParsleyEpochEvent.SnapshotRequested("A"));
        log.apply(new ParsleyEpochEvent.FrontierPublished("A",
                ParsleyClock.empty().observe(T1_ID, 0, 5).observe(T2_ID, 0, 3)));
        log.apply(log.proposeCommit());   // epoch 2: floor {T1:5, T2:3}; B promoted to running
        assertEquals(2L, log.committedEpochId(), "epoch 2 is committed, promoting B to running");

        log.apply(new ParsleyEpochEvent.SnapshotRequested("A"));
        log.apply(new ParsleyEpochEvent.FrontierPublished("A",
                ParsleyClock.empty().observe(T1_ID, 0, 10).observe(T2_ID, 0, 8)));
        // B, freshly running and reading from earliest, is far behind on T1 but ahead on T2.
        log.apply(new ParsleyEpochEvent.FrontierPublished("B",
                ParsleyClock.empty().observe(T1_ID, 0, 2).observe(T2_ID, 0, 9)));

        ParsleyEpochEvent.EpochCommitted commit = log.proposeCommit();
        assertEquals(3L, commit.epochId(), "the epoch id is still strictly increasing");
        assertEquals(5L, commit.lowerBounds().offsetFor(T1_ID, 0),
                "T1's raw mergeMin(10, 2) = 2 would regress below epoch 2's floor of 5 — the clamp must "
                        + "hold it at 5, never letting the floor move backwards");
        assertEquals(8L, commit.lowerBounds().offsetFor(T2_ID, 0),
                "T2's mergeMin(8, 9) = 8 is genuine forward progress from epoch 2's floor of 3 — the "
                        + "clamp must not interfere with a coordinate that did not regress");
    }

    /** A commit advances the epoch monotonically, promotes pending joiners to running, and clears the round. */
    @Test
    void commitAdvancesEpochPromotesJoinersAndClearsTheRound() {
        ParsleyEpochLog log = bootstrappedWithRunningMember("A");   // epoch 1, A running
        log.apply(new ParsleyEpochEvent.JoinRequested("B", Set.of(), Set.of()));               // B pending
        log.apply(new ParsleyEpochEvent.SnapshotRequested("A"));
        log.apply(new ParsleyEpochEvent.FrontierPublished("A", ParsleyClock.empty().observe(T1_ID, 0, 5)));

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
        log.apply(new ParsleyEpochEvent.FrontierPublished("A", ParsleyClock.empty().observe(T1_ID, 0, 5)));

        log.apply(new ParsleyEpochEvent.JoinRequested("B", Set.of(), Set.of()));
        log.apply(new ParsleyEpochEvent.SnapshotRequested("A"));
        // B is a pending joiner, not running: its publication must not count toward completeness.
        log.apply(new ParsleyEpochEvent.FrontierPublished("B", ParsleyClock.empty().observe(T1_ID, 0, 9)));
        assertFalse(log.isRoundComplete(), "only A (running) is awaited; A has not published in this round");

        log.apply(new ParsleyEpochEvent.FrontierPublished("A", ParsleyClock.empty().observe(T1_ID, 0, 5)));
        assertTrue(log.isRoundComplete(), "the round completes once the running member A publishes");
    }

    /** {@link ParsleyEpochLog#proposeCommit()} rejects a round that is not complete. */
    @Test
    void proposeCommitRejectsAnIncompleteRound() {
        ParsleyEpochLog log = bootstrappedWithRunningMember("A");
        log.apply(new ParsleyEpochEvent.JoinRequested("B", Set.of(), Set.of()));
        commitRound(log, "B");                              // A+B running
        log.apply(new ParsleyEpochEvent.SnapshotRequested("A"));   // round open, nobody published

        assertThrows(IllegalStateException.class, log::proposeCommit,
                "committing before every running member has published must be rejected");
    }

    /**
     * A stale or duplicate {@link ParsleyEpochEvent.EpochCommitted} is ignored: the first commit for an epoch is
     * authoritative, so a second commit for the same epoch (from an owner-plus-takeover, or a re-append)
     * folds to a no-op and does not clobber a round that has since opened for the next epoch. This is the
     * dedup-by-epochId property the leaderless protocol relies on for safe failover.
     */
    @Test
    void duplicateOrStaleCommitIsIgnored() {
        ParsleyEpochLog log = bootstrappedWithRunningMember("A");   // epoch 1, A running
        // Open and commit epoch 2 normally.
        log.apply(new ParsleyEpochEvent.SnapshotRequested("A"));
        log.apply(new ParsleyEpochEvent.FrontierPublished("A", ParsleyClock.empty().observe(T1_ID, 0, 5)));
        ParsleyEpochEvent.EpochCommitted epochTwo = log.proposeCommit();
        log.apply(epochTwo);
        assertEquals(2L, log.committedEpochId(), "epoch 2 is committed");

        // A new round for epoch 3 opens.
        log.apply(new ParsleyEpochEvent.SnapshotRequested("A"));
        assertTrue(log.isRoundOpen(), "a round for epoch 3 is open");

        // A duplicate of the epoch-2 commit now lands (e.g. a takeover re-append). It must be ignored,
        // leaving the epoch-3 round untouched.
        log.apply(epochTwo);
        assertEquals(2L, log.committedEpochId(), "the settled epoch stays at 2 — the duplicate is a no-op");
        assertTrue(log.isRoundOpen(), "the open epoch-3 round is not cleared by the stale commit");
        assertEquals("A", log.roundOwner(), "the epoch-3 round keeps its owner");
    }

    /**
     * A {@link ParsleyEpochEvent.Leave} removes a member from the domain, so an open round the member was holding
     * up completes without it — the mechanism that stops a gone member freezing rounds forever.
     */
    @Test
    void leaveRemovesAMemberSoAnOpenRoundCompletesWithoutIt() {
        ParsleyEpochLog log = bootstrappedWithRunningMember("A");
        log.apply(new ParsleyEpochEvent.JoinRequested("B", Set.of(), Set.of()));
        commitRound(log, "B");                              // A+B running (epoch 2)

        log.apply(new ParsleyEpochEvent.SnapshotRequested("A"));
        log.apply(new ParsleyEpochEvent.FrontierPublished("A", ParsleyClock.empty().observe(T1_ID, 0, 5)));
        assertFalse(log.isRoundComplete(), "B is a running member that has not published, so the round waits");

        log.apply(new ParsleyEpochEvent.Leave("B"));               // B decommissioned or evicted
        assertFalse(log.isRunningMember("B"), "the departed member is removed from the running set");
        assertTrue(log.isRoundComplete(), "the round completes once the only outstanding member has left");
    }

    /** A {@link ParsleyEpochEvent.Leave} for a member that is not present is a no-op. */
    @Test
    void leaveForANonMemberIsANoOp() {
        ParsleyEpochLog log = bootstrappedWithRunningMember("A");
        log.apply(new ParsleyEpochEvent.Leave("ghost"));
        assertEquals(java.util.Set.of("A"), log.runningMembers(), "leaving a non-member changes nothing");
        assertTrue(log.isRunningMember("A"), "the real member is untouched");
    }

    /**
     * The DAG-wide source-topic registry is derived from every declared member: an external source is a
     * topic some member consumes but no member produces ({@code ∪inputs − ∪sinks}). A two-stage DAG
     * (m1: IN→mid, m2: mid→out) has exactly one external source, IN.
     */
    @Test
    void externalSourceTopicsAreInputsThatNoMemberProduces() {
        ParsleyEpochLog log = new ParsleyEpochLog();
        log.apply(new ParsleyEpochEvent.JoinRequested("m1", Set.of("IN"), Set.of("mid")));
        log.apply(new ParsleyEpochEvent.JoinRequested("m2", Set.of("mid"), Set.of("out")));

        assertEquals(Set.of("IN"), log.externalSourceTopics(),
                "IN is consumed but produced by no member; mid is produced by m1, so it is internal");
    }

    /**
     * A member's declaration counts as soon as it joins (pending), before any commit promotes it to
     * running — so source-layer identity is correct from the very first round.
     */
    @Test
    void externalSourceTopicsCountPendingDeclarationsAndSurviveACommit() {
        ParsleyEpochLog log = new ParsleyEpochLog();
        log.apply(new ParsleyEpochEvent.JoinRequested("m1", Set.of("IN"), Set.of("mid")));   // pending
        log.apply(new ParsleyEpochEvent.JoinRequested("m2", Set.of("mid"), Set.of("out")));  // pending
        assertEquals(Set.of("IN"), log.externalSourceTopics(),
                "a pending member's declaration counts toward the registry");

        log.apply(new ParsleyEpochEvent.SnapshotRequested("m1"));
        log.apply(log.proposeCommit());                                               // promote m1, m2 to running
        assertEquals(Set.of("IN"), log.externalSourceTopics(),
                "declarations persist across a commit; the registry is unchanged");
    }

    /**
     * A {@link ParsleyEpochEvent.Leave} drops the member's declaration from the registry, so a topic only that
     * member produced becomes external again (nothing produces it anymore).
     */
    @Test
    void leaveDropsAMembersDeclarationFromTheRegistry() {
        ParsleyEpochLog log = new ParsleyEpochLog();
        log.apply(new ParsleyEpochEvent.JoinRequested("m1", Set.of("IN"), Set.of("mid")));
        log.apply(new ParsleyEpochEvent.JoinRequested("m2", Set.of("mid"), Set.of("out")));
        assertEquals(Set.of("IN"), log.externalSourceTopics(), "mid is internal while m1 produces it");

        log.apply(new ParsleyEpochEvent.Leave("m1"));   // the producer of mid leaves
        assertEquals(Set.of("mid"), log.externalSourceTopics(),
                "with m1 gone, nobody produces mid, so m2's input mid is now an external source");
    }

    /**
     * {@link ParsleyEpochLog#externalSourceTopicsAsOfPreviousCommit()} is a two-slot shift register:
     * empty before two commits have happened, then always "the registry as of one commit ago" — the exact
     * set a departing topic's outgoing self-adopter needs to give it one more adoption cycle, since a
     * declaring producer's {@code JoinRequested} always folds (excluding the topic from the live registry)
     * before the commit that promotes it to running.
     *
     * <p>Mirrors {@code ParsleyProcessorSourceLayerTest}'s handoff scenario at the log level: B declares t1
     * as input (epoch 1, t1 external); P then joins declaring t1 as its sink, and epoch 2 commits,
     * admitting P — t1 has already left the live registry by commit 2, but the previous-commit snapshot
     * (frozen at commit 1, before P's join) still has it. A further commit with no new declarations lets
     * the grace window close.
     */
    @Test
    void externalSourceTopicsAsOfPreviousCommitGivesADepartingTopicExactlyOneMoreCycle() {
        ParsleyEpochLog log = new ParsleyEpochLog();
        log.apply(new ParsleyEpochEvent.JoinRequested("B", Set.of("t1"), Set.of()));
        log.apply(new ParsleyEpochEvent.SnapshotRequested("B"));
        log.apply(log.proposeCommit());   // epoch 1: only B declared, t1 external
        assertEquals(Set.of(), log.externalSourceTopicsAsOfPreviousCommit(),
                "before a second commit, there is no 'previous' snapshot yet");

        log.apply(new ParsleyEpochEvent.JoinRequested("P", Set.of(), Set.of("t1")));   // folds immediately
        assertEquals(Set.of(), log.externalSourceTopics(), "t1 leaves the LIVE registry the instant P joins");

        log.apply(new ParsleyEpochEvent.SnapshotRequested("P"));
        log.apply(new ParsleyEpochEvent.FrontierPublished("B", ParsleyClock.empty()));   // B is running; P is not yet
        log.apply(log.proposeCommit());   // epoch 2: admits P
        assertEquals(Set.of("t1"), log.externalSourceTopicsAsOfPreviousCommit(),
                "the previous-commit snapshot (frozen at epoch 1, before P's join) must still have t1 — "
                        + "this is what gives t1 its one handoff cycle at epoch 2");

        log.apply(new ParsleyEpochEvent.SnapshotRequested("B"));
        log.apply(new ParsleyEpochEvent.FrontierPublished("B", ParsleyClock.empty()));
        log.apply(new ParsleyEpochEvent.FrontierPublished("P", ParsleyClock.empty()));   // both now running
        log.apply(log.proposeCommit());   // epoch 3: no new declarations
        assertEquals(Set.of(), log.externalSourceTopicsAsOfPreviousCommit(),
                "the grace window has closed — epoch 2's own snapshot (already excluding t1) is now 'previous'");
    }

    // --- helpers --------------------------------------------------------------------------------

    /** A log where {@code member} has joined and been committed into epoch 1 (so it is running). */
    private static ParsleyEpochLog bootstrappedWithRunningMember(String member) {
        ParsleyEpochLog log = new ParsleyEpochLog();
        log.apply(new ParsleyEpochEvent.JoinRequested(member, Set.of(), Set.of()));
        log.apply(new ParsleyEpochEvent.SnapshotRequested(member));
        log.apply(log.proposeCommit());                     // vacuously complete (no prior running members)
        return log;
    }

    /** Opens a round owned by {@code owner}, has every currently-running member publish an empty frontier,
     * and commits it — advancing the epoch. */
    private static void commitRound(ParsleyEpochLog log, String owner) {
        log.apply(new ParsleyEpochEvent.SnapshotRequested(owner));
        for (String member : log.runningMembers()) {
            log.apply(new ParsleyEpochEvent.FrontierPublished(member, ParsleyClock.empty()));
        }
        log.apply(log.proposeCommit());
    }
}
