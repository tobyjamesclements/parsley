package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ParsleyEpochLog}: the deterministic fold over the {@code epoch-events} log. Under the
 * genesis-cohort-barrier model, genesis (the first commit) does not settle until the whole founding cohort
 * — every app in the agreed roster, present with its full task set — has declared, and its floor is empty
 * by construction. After genesis the committed roster is authoritative: a round commits only when the
 * committed members unanimously agree on one roster, every newly-admitted app is cohort-complete, every
 * running member has published, and the mesh holds; {@link ParsleyEpochLog#evaluateCommit()} is the single
 * predicate for both proposing and (via {@link ParsleyEpochLog#apply}) validating a commit.
 */
class ParsleyEpochLogTest {

    private static final Uuid T1_ID = Uuid.randomUuid();
    private static final Uuid T2_ID = Uuid.randomUuid();

    /** A lone-app genesis: app A (roster {A}, one task) declares and requests a snapshot, commits epoch 1
     * with an empty floor (no running members to bound), and becomes a running member of roster {A}. */
    @Test
    void genesisCommitsEpochOneWithEmptyFloorOnceTheLoneCohortHasDeclared() {
        ParsleyEpochLog log = new ParsleyEpochLog();
        log.apply(join("A", "0_0", Set.of("t1"), Set.of(), Set.of("A"), 1));
        log.apply(new ParsleyEpochEvent.SnapshotRequested("A/0_0"));

        assertTrue(log.isRoundOpen(), "the snapshot request opens the genesis round");
        ParsleyEpochEvent.EpochCommitted commit = log.evaluateCommit().orElseThrow();
        assertEquals(1L, commit.epochId(), "the first commit is epoch 1 (genesis)");
        assertTrue(commit.lowerBounds().isEmpty(), "the genesis floor is empty by construction");
        assertEquals(Set.of("A"), commit.committedRoster(), "genesis commits the founding roster {A}");

        log.apply(commit);
        assertEquals(1L, log.committedEpochId(), "the settled epoch advances to genesis (1)");
        assertFalse(log.isRoundOpen(), "the round closes on commit");
        assertEquals(Set.of("A/0_0"), log.runningMembers(), "the founder is now a running member");
        assertEquals(Set.of("A"), log.committedRoster(), "the committed roster is {A}");
    }

    /** Genesis does not commit until every app in the roster has declared: with roster {A,B} but only A
     * present, the round is not committable; it becomes committable once B declares too. */
    @Test
    void genesisWaitsUntilEveryCohortAppHasDeclared() {
        ParsleyEpochLog log = new ParsleyEpochLog();
        log.apply(join("A", "0_0", Set.of("t1"), Set.of(), Set.of("A", "B"), 1));
        log.apply(new ParsleyEpochEvent.SnapshotRequested("A/0_0"));
        assertTrue(log.evaluateCommit().isEmpty(), "genesis must wait: B (in roster {A,B}) has not declared");

        log.apply(join("B", "0_0", Set.of("t1"), Set.of(), Set.of("A", "B"), 1));
        assertTrue(log.evaluateCommit().isPresent(), "with the whole cohort {A,B} present, genesis can commit");
        assertEquals(Set.of("A", "B"), log.evaluateCommit().orElseThrow().committedRoster(),
                "genesis commits the full founding roster");
    }

    /** Genesis waits until every TASK of a cohort app has declared — the barrier is at task granularity,
     * so a multi-task app whose partitions have not all declared cannot seal genesis (which would leave a
     * straggler to be mis-admitted later at a non-empty floor). */
    @Test
    void genesisWaitsUntilEveryTaskOfAnAppHasDeclared() {
        ParsleyEpochLog log = new ParsleyEpochLog();
        log.apply(join("A", "0_0", Set.of("t1"), Set.of(), Set.of("A"), 2));   // app A has 2 tasks
        log.apply(new ParsleyEpochEvent.SnapshotRequested("A/0_0"));
        assertTrue(log.evaluateCommit().isEmpty(), "only task 0_0 of a 2-task app has declared");

        log.apply(join("A", "0_1", Set.of("t1"), Set.of(), Set.of("A"), 2));
        assertTrue(log.evaluateCommit().isPresent(), "both tasks of app A have now declared — cohort complete");
    }

    /** Genesis refuses to commit while the domain is not a full mesh: a member whose own subscriptions do
     * not cover the whole domain would publish unsound completeness. */
    @Test
    void genesisRefusesToCommitWhenTheCohortIsNotAFullMesh() {
        ParsleyEpochLog log = new ParsleyEpochLog();
        // A consumes t1; B consumes t2 — neither covers the domain {t1,t2}, so the mesh fails.
        log.apply(join("A", "0_0", Set.of("t1"), Set.of(), Set.of("A", "B"), 1));
        log.apply(join("B", "0_0", Set.of("t2"), Set.of(), Set.of("A", "B"), 1));
        log.apply(new ParsleyEpochEvent.SnapshotRequested("A/0_0"));
        assertTrue(log.evaluateCommit().isEmpty(), "genesis must not commit while the cohort is not a full mesh");
    }

    /** The committed lower bounds are the per-coordinate {@code mergeMin} of the published frontiers,
     * clamped to never regress below the previous commit's floor. */
    @Test
    void postGenesisFloorIsClampedMergeMinOfPublishedFrontiers() {
        ParsleyEpochLog log = genesisAB();   // A/0_0, B/0_0 running at empty floor, roster {A,B}

        // Epoch 2: both observe {T1:5, T2:3}.
        openRound(log, "A/0_0");
        log.apply(publish("A/0_0", 5, 3));
        log.apply(publish("B/0_0", 5, 3));
        log.apply(commit(log));
        assertEquals(2L, log.committedEpochId(), "epoch 2 committed");

        // Epoch 3: A advances to {T1:10, T2:8}; B regresses on T1 ({T1:2}) but advances T2 ({T2:9}).
        openRound(log, "A/0_0");
        log.apply(publish("A/0_0", 10, 8));
        log.apply(publish("B/0_0", 2, 9));
        ParsleyEpochEvent.EpochCommitted commit = log.evaluateCommit().orElseThrow();
        assertEquals(3L, commit.epochId(), "the epoch id increases monotonically");
        assertEquals(5L, commit.lowerBounds().offsetFor(T1_ID, 0),
                "raw mergeMin(10,2)=2 would regress below epoch 2's floor of 5 — the clamp holds it at 5");
        assertEquals(8L, commit.lowerBounds().offsetFor(T2_ID, 0),
                "mergeMin(8,9)=8 is genuine progress from 3 — the clamp leaves an un-regressed coordinate alone");
    }

    /** A round is not committable until every running member has published its frontier. */
    @Test
    void postGenesisRoundWaitsForEveryRunningMemberToPublish() {
        ParsleyEpochLog log = genesisAB();
        openRound(log, "A/0_0");
        assertTrue(log.evaluateCommit().isEmpty(), "no member has published yet");
        log.apply(publish("A/0_0", 5, 0));
        assertTrue(log.evaluateCommit().isEmpty(), "only A has published; B is still outstanding");
        log.apply(publish("B/0_0", 9, 0));
        assertTrue(log.evaluateCommit().isPresent(), "both running members have published");
    }

    /** A commit promotes only pending joiners whose app is in the newly-committed roster; a pending joiner
     * whose app the roster does not name (a rogue, or one the incumbents have not acknowledged) is left
     * pending, never admitted at a non-empty floor. */
    @Test
    void commitPromotesOnlyRosterMembersAndLeavesANonRosterPending() {
        ParsleyEpochLog log = genesisAB();   // roster {A,B}
        // A rogue app R declares the same roster {A,B} (it copied the config) — but R is not in {A,B}.
        log.apply(join("R", "0_0", Set.of("t1"), Set.of(), Set.of("A", "B"), 1));
        openRound(log, "A/0_0");
        log.apply(publish("A/0_0", 1, 0));
        log.apply(publish("B/0_0", 1, 0));
        log.apply(commit(log));

        assertFalse(log.isRunningMember("R/0_0"), "R is not in the committed roster {A,B}, so it is not promoted");
        assertEquals(Set.of("A", "B"), log.committedRoster(), "the committed roster stays {A,B}");
    }

    /**
     * {@code apply} re-validates a commit against the fold state at its own log position: a cohort
     * contradiction (here, a task 0_2 declaring under a task total of 2 — proof of an undercount) that
     * folds in just before an otherwise-valid genesis commit makes every node reject that commit, so
     * genesis does not seal a roster while a real task is still missing.
     */
    @Test
    void applyRejectsACommitInvalidatedByACohortConflictFoldingBeforeIt() {
        ParsleyEpochLog log = new ParsleyEpochLog();
        log.apply(join("A", "0_0", Set.of("t1"), Set.of(), Set.of("A"), 2));
        log.apply(join("A", "0_1", Set.of("t1"), Set.of(), Set.of("A"), 2));
        log.apply(new ParsleyEpochEvent.SnapshotRequested("A/0_0"));
        ParsleyEpochEvent.EpochCommitted genesis = log.evaluateCommit().orElseThrow();   // valid right now

        // A task 0_2 declaring under total 2 is proof of an undercount — it folds in before the commit.
        log.apply(join("A", "0_2", Set.of("t1"), Set.of(), Set.of("A"), 2));
        log.apply(genesis);
        assertEquals(0L, log.committedEpochId(), "the commit is rejected — genesis stays uncommitted");
        assertEquals(1L, log.consumeLastRejectedCommitEpoch(),
                "the rejected epoch is signalled (read-once) for a latch reset");
        assertEquals(-1L, log.consumeLastRejectedCommitEpoch(),
                "the signal is read-once — a second read is cleared, so the runtime resets its latch at most once");

        // Recovery: the undercount task 0_2 leaves, the cohort is consistent again, and genesis becomes
        // committable — no permanent wedge from a rejected commit.
        log.apply(new ParsleyEpochEvent.Leave("A/0_2"));
        assertTrue(log.evaluateCommit().isPresent(),
                "once the conflict clears, genesis is committable again (the reject is not a permanent wedge)");
    }

    /** Roster agreement is AGREE when the voters declare one roster, CONVERGING when their views are
     * nested (a change in flight), and CONFLICT when they are incomparable — and a non-AGREE state never
     * commits. */
    @Test
    void rosterAgreementClassifiesAgreeConvergingAndConflict() {
        // CONVERGING: committed {A,B}; A re-declares {A,B,C} while B still says {A,B} (a rolling add).
        ParsleyEpochLog converging = genesisAB();
        converging.apply(join("A", "0_0", Set.of("t1"), Set.of(), Set.of("A", "B", "C"), 1));
        assertEquals(ParsleyEpochLog.RosterAgreement.CONVERGING, converging.rosterAgreement(),
                "nested views {A,B} subset of {A,B,C} are a change in flight");
        openRound(converging, "A/0_0");
        converging.apply(publish("A/0_0", 1, 0));
        converging.apply(publish("B/0_0", 1, 0));
        assertTrue(converging.evaluateCommit().isEmpty(), "no commit while the roster is converging");

        // CONFLICT: committed {A,B}; A says {A,B,C}, B says {A,B,D} — incomparable.
        ParsleyEpochLog conflict = genesisAB();
        conflict.apply(join("A", "0_0", Set.of("t1"), Set.of(), Set.of("A", "B", "C"), 1));
        conflict.apply(join("B", "0_0", Set.of("t1"), Set.of(), Set.of("A", "B", "D"), 1));
        assertEquals(ParsleyEpochLog.RosterAgreement.CONFLICT, conflict.rosterAgreement(),
                "incomparable views {A,B,C} vs {A,B,D} are a conflict");
    }

    /** A same-app task-total disagreement is a cohort conflict (partition-count instability), so no commit. */
    @Test
    void sameAppTaskTotalDisagreementIsACohortConflict() {
        ParsleyEpochLog log = new ParsleyEpochLog();
        log.apply(join("A", "0_0", Set.of("t1"), Set.of(), Set.of("A"), 3));
        log.apply(join("A", "0_1", Set.of("t1"), Set.of(), Set.of("A"), 2));   // disagrees: total 2 vs 3
        assertEquals(ParsleyEpochLog.RosterAgreement.CONFLICT, log.rosterAgreement(),
                "two tasks of one app declaring different totals is a conflict");
    }

    /** {@link ParsleyEpochLog#admissibleApps()}: a committed member is always admissible; a newcomer is
     * admissible only once every committed member has named it; a rogue the incumbents have not named is
     * not. */
    @Test
    void admissibleAppsAreCommittedMembersPlusThoseEveryVoterNames() {
        ParsleyEpochLog log = genesisAB();   // committed roster {A,B}
        assertTrue(log.admissibleApps().contains("A"), "a committed member is admissible");
        assertFalse(log.admissibleApps().contains("C"), "C is not named by any committed member — not admissible");

        // Only A re-declares {A,B,C}; B still {A,B}. C is not yet named by EVERY voter → not admissible.
        log.apply(join("A", "0_0", Set.of("t1"), Set.of(), Set.of("A", "B", "C"), 1));
        assertFalse(log.admissibleApps().contains("C"), "C is named by A but not B — not yet admissible");

        // Now B also re-declares {A,B,C}. Every committed member names C → admissible.
        log.apply(join("B", "0_0", Set.of("t1"), Set.of(), Set.of("A", "B", "C"), 1));
        assertTrue(log.admissibleApps().contains("C"), "every committed member names C — now admissible");
    }

    /** A member's admission cut (epoch + floor) is recorded at promotion, so a stateless restart re-seeds
     * from its own cut. A genesis founder is recorded at the empty genesis floor. */
    @Test
    void admissionFloorIsRecordedAtPromotion() {
        ParsleyEpochLog log = genesisAB();
        ParsleyEpochRuntime.CommittedEpoch admission = log.admissionFloorsSnapshot().get("A/0_0");
        assertEquals(1L, admission.epochId(), "the founder was admitted at genesis (epoch 1)");
        assertTrue(admission.lowerBounds().isEmpty(), "a genesis founder's admission floor is empty");
    }

    /** A member's admission floor is pinned to the epoch that admitted it and does NOT advance with later
     * commits: a joiner admitted at epoch 2's non-empty floor F2 keeps F2 even after epoch 3 raises the
     * committed floor to F3. This is load-bearing for the seed (the joiner must re-adopt F2 on a stateless
     * restart, not the current F3 — which would over-strip history it never actually skipped). */
    @Test
    void admissionFloorIsPinnedToTheAdmittingEpochNotLaterCommits() {
        ParsleyEpochLog log = new ParsleyEpochLog();
        // Genesis: founder A alone (roster {A}), running at the empty floor.
        log.apply(join("A", "0_0", Set.of("t1"), Set.of(), Set.of("A"), 1));
        openRound(log, "A/0_0");
        log.apply(commit(log));

        // A redeploys naming B, B joins; epoch 2 admits B at a NON-EMPTY floor F2 = T1@5 (A's publication).
        log.apply(join("A", "0_0", Set.of("t1"), Set.of(), Set.of("A", "B"), 1));
        log.apply(join("B", "0_0", Set.of("t1"), Set.of(), Set.of("A", "B"), 1));
        openRound(log, "A/0_0");
        log.apply(publish("A/0_0", 5, 0));
        log.apply(commit(log));
        assertEquals(2L, log.committedEpochId(), "epoch 2 admits B");
        ParsleyEpochRuntime.CommittedEpoch bAdmission = log.admissionFloorsSnapshot().get("B/0_0");
        assertEquals(2L, bAdmission.epochId(), "B is admitted at epoch 2");
        assertEquals(5L, bAdmission.lowerBounds().offsetFor(T1_ID, 0), "B's admission floor is epoch 2's F2 (T1@5)");

        // Epoch 3 raises the committed floor to F3 = T1@10; B is already running, not re-promoted.
        openRound(log, "A/0_0");
        log.apply(publish("A/0_0", 10, 0));
        log.apply(publish("B/0_0", 10, 0));
        log.apply(commit(log));
        assertEquals(10L, log.committedLowerBounds().offsetFor(T1_ID, 0), "the committed floor advanced to T1@10");

        ParsleyEpochRuntime.CommittedEpoch stillF2 = log.admissionFloorsSnapshot().get("B/0_0");
        assertEquals(2L, stillF2.epochId(), "B's admission floor stays pinned to its admitting epoch (2)");
        assertEquals(5L, stillF2.lowerBounds().offsetFor(T1_ID, 0),
                "B's admission floor stays F2 (T1@5) even after the committed floor advanced to T1@10 — the seed "
                        + "must re-adopt this, not the current floor");
    }

    /** A stale or duplicate {@link ParsleyEpochEvent.EpochCommitted} is ignored (dedup by epoch id), so a
     * re-appended commit does not clobber a round that has since opened. */
    @Test
    void duplicateOrStaleCommitIsIgnored() {
        ParsleyEpochLog log = genesisAB();   // committed epoch 1
        openRound(log, "A/0_0");
        log.apply(publish("A/0_0", 5, 0));
        log.apply(publish("B/0_0", 5, 0));
        ParsleyEpochEvent.EpochCommitted epochTwo = commit(log);
        log.apply(epochTwo);
        assertEquals(2L, log.committedEpochId(), "epoch 2 is committed");

        openRound(log, "A/0_0");
        assertTrue(log.isRoundOpen(), "a round for epoch 3 is open");
        log.apply(epochTwo);                                       // a duplicate epoch-2 commit lands
        assertEquals(2L, log.committedEpochId(), "the duplicate is a no-op — the settled epoch stays at 2");
        assertTrue(log.isRoundOpen(), "the open epoch-3 round is not cleared by the stale commit");
    }

    /** A {@link ParsleyEpochEvent.Leave} removes a member so an open round it was holding up completes. */
    @Test
    void leaveRemovesAMemberSoAnOpenRoundBecomesCommittable() {
        ParsleyEpochLog log = genesisAB();
        openRound(log, "A/0_0");
        log.apply(publish("A/0_0", 5, 0));
        assertTrue(log.evaluateCommit().isEmpty(), "B has not published, so the round waits");

        log.apply(new ParsleyEpochEvent.Leave("B/0_0"));
        assertFalse(log.isRunningMember("B/0_0"), "the departed member is removed from the running set");
        // With B gone, the committed roster still says {A,B} until the next commit, but the voter set drops
        // B (no live declaration) so A alone agrees on... {A,B} (A's own view) — A covers t1, mesh holds.
        assertTrue(log.evaluateCommit().isPresent(), "the round is committable once the outstanding member has left");
    }

    /** The DAG-wide source-topic registry is derived from the effective roster's members: an external
     * source is a topic some member consumes but none produces ({@code ∪inputs − ∪sinks}). */
    @Test
    void externalSourceTopicsAreInputsThatNoMemberProduces() {
        ParsleyEpochLog log = new ParsleyEpochLog();
        // Two founders of one domain: m1 (IN->mid), m2 (mid->out); roster {m1app, m2app}.
        Set<String> roster = Set.of("m1app", "m2app");
        log.apply(join("m1app", "0_0", Set.of("IN", "mid", "out"), Set.of("mid"), roster, 1));
        log.apply(join("m2app", "0_0", Set.of("IN", "mid", "out"), Set.of("out"), roster, 1));
        assertEquals(Set.of("IN"), log.externalSourceTopics(),
                "IN is consumed but produced by no member; mid and out are produced within the domain");
    }

    // --- helpers --------------------------------------------------------------------------------

    /** A {@link ParsleyEpochEvent.JoinRequested} for {@code app}'s task {@code taskId}. */
    private static ParsleyEpochEvent.JoinRequested join(String app, String taskId, Set<String> inputs,
            Set<String> sinks, Set<String> roster, int taskTotal) {
        return new ParsleyEpochEvent.JoinRequested(app + "/" + taskId, app, inputs, sinks, roster, taskTotal);
    }

    /** A {@link ParsleyEpochEvent.FrontierPublished} observing {@code T1@t1off, T2@t2off}. */
    private static ParsleyEpochEvent.FrontierPublished publish(String memberId, long t1off, long t2off) {
        return new ParsleyEpochEvent.FrontierPublished(memberId,
                ParsleyVectorClock.empty().observe(T1_ID, 0, t1off).observe(T2_ID, 0, t2off));
    }

    /** Opens a round owned by {@code owner}. */
    private static void openRound(ParsleyEpochLog log, String owner) {
        log.apply(new ParsleyEpochEvent.SnapshotRequested(owner));
    }

    /** The commit the currently-open round would produce (must be committable). */
    private static ParsleyEpochEvent.EpochCommitted commit(ParsleyEpochLog log) {
        return log.evaluateCommit().orElseThrow(() -> new AssertionError("round is not committable"));
    }

    /** A genesis log with founders A and B (roster {A,B}, one task each, both consuming t1), committed
     * into genesis epoch 1 at an empty floor — so A/0_0 and B/0_0 are running. */
    private static ParsleyEpochLog genesisAB() {
        ParsleyEpochLog log = new ParsleyEpochLog();
        log.apply(join("A", "0_0", Set.of("t1"), Set.of(), Set.of("A", "B"), 1));
        log.apply(join("B", "0_0", Set.of("t1"), Set.of(), Set.of("A", "B"), 1));
        log.apply(new ParsleyEpochEvent.SnapshotRequested("A/0_0"));
        log.apply(commit(log));
        return log;
    }
}
