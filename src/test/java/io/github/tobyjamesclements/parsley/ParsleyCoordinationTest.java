package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ParsleyCoordination}, the handle that turns on topology-epoch coordination. Exercised
 * over the in-memory transport double via the package-private {@link ParsleyCoordination#forRuntime} seam,
 * with the runtime driven synchronously — no broker. Members declare a single-task app with a member-app
 * roster; genesis (the first commit) settles once the whole roster's cohort has declared.
 */
class ParsleyCoordinationTest {

    private static final Uuid T1 = Uuid.randomUuid();
    // A join budget large enough that these synchronous, promptly-admitted scenarios never hit it.
    private static final Duration GENEROUS_BUDGET = Duration.ofSeconds(30);

    /** {@link ParsleyCoordination#requestEpochTransition()} attributes the snapshot to a local member and
     * drives the round to a commit — here, genesis for the lone founder A. */
    @Test
    void requestEpochTransitionOpensAndCommitsARoundOwnedByALocalMember() {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        ParsleyEpochRuntime runtime = new ParsleyEpochRuntime(new InMemoryEpochTransport(log));
        ParsleyCoordination coordination = ParsleyCoordination.forRuntime(runtime);
        join(runtime, "A", Set.of("A"));   // a local founder, as a participating task's init() would declare

        coordination.requestEpochTransition();
        settle(log, runtime);

        assertEquals(1L, runtime.committedEpochId(),
                "requesting a transition opens a round owned by the local member and commits genesis");
    }

    /** Requesting a transition before any task has initialised coordination fails with a clear message. */
    @Test
    void requestEpochTransitionBeforeAnyRuntimeFails() {
        ParsleyCoordination coordination = ParsleyCoordination.create("epoch-events");
        assertThrows(IllegalStateException.class, coordination::requestEpochTransition,
                "with no initialised runtime there is nothing to request a transition on");
    }

    /** Requesting a transition with a runtime but no joined member fails: no local owner to attribute it to. */
    @Test
    void requestEpochTransitionWithNoLocalMemberFails() {
        ParsleyEpochRuntime runtime =
                new ParsleyEpochRuntime(new InMemoryEpochTransport(new InMemoryEpochTransport.SharedLog()));
        ParsleyCoordination coordination = ParsleyCoordination.forRuntime(runtime);
        assertThrows(IllegalStateException.class, coordination::requestEpochTransition,
                "no local member has joined, so the request cannot be attributed to a local owner");
    }

    /** {@link ParsleyCoordination#close()} is idempotent and safe to call before any task initialised the runtime. */
    @Test
    void closeIsIdempotentAndSafeBeforeInit() {
        ParsleyCoordination uninitialised = ParsleyCoordination.create("epoch-events");
        uninitialised.close();   // no runtime built yet — a no-op

        ParsleyEpochRuntime runtime =
                new ParsleyEpochRuntime(new InMemoryEpochTransport(new InMemoryEpochTransport.SharedLog()));
        ParsleyCoordination coordination = ParsleyCoordination.forRuntime(runtime);
        coordination.close();
        coordination.close();   // idempotent
    }

    /** A founder does not block at genesis: with an empty genesis floor there is no history to strip, so
     * {@code awaitJoinCommit} opens the genesis round and returns without waiting, and genesis then seals. */
    @Test
    void aFounderDoesNotBlockAtGenesisAndGenesisSeals() throws Exception {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        ParsleyEpochRuntime runtime = new ParsleyEpochRuntime(new InMemoryEpochTransport(log));
        ParsleyCoordination coordination = ParsleyCoordination.forRuntime(runtime);
        runtime.start();
        try {
            join(runtime, "A", Set.of("A"));
            coordination.awaitJoinCommit(runtime, "A", "A", GENEROUS_BUDGET);   // returns without blocking
            waitUntil(() -> runtime.committedEpochId() == 1L);
            assertTrue(runtime.isRunningMember("A"), "genesis seals and promotes the lone founder");
            assertEquals(Set.of("A"), runtime.committedRoster(), "genesis commits roster {A}");
        } finally {
            runtime.close();
        }
    }

    /** The join wait is bounded: a joiner whose app is not an agreed roster member can never be admitted,
     * so its bounded wait fails loudly with {@link ParsleyJoinTimeoutException} rather than hanging past
     * {@code max.poll.interval.ms} into a silent rebalance crash-loop. */
    @Test
    void awaitJoinCommitFailsFastWhenTheAppIsNotAnAgreedRosterMember() {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        InMemoryEpochTransport seeder = new InMemoryEpochTransport(log);
        // An established domain of roster {M}: J's app is not a member, so J can never be admitted.
        seeder.append(seedJoin("M", Set.of("M")));
        seeder.append(new ParsleyEpochEvent.SnapshotRequested("M"));
        seeder.append(seedCommit(1, ParsleyVectorClock.empty(), Set.of("M")));

        ParsleyEpochRuntime runtime = new ParsleyEpochRuntime(new InMemoryEpochTransport(log));
        ParsleyCoordination coordination = ParsleyCoordination.forRuntime(runtime);
        runtime.runOnce();   // fold the seeded log: bootstrapped, committedEpochId=1, roster {M}
        try {
            assertThrows(ParsleyJoinTimeoutException.class,
                    () -> coordination.awaitJoinCommit(runtime, "J", "J", Duration.ofMillis(150)),
                    "a join whose app is not an agreed roster member must fail loudly, not hang");
        } finally {
            runtime.close();
        }
    }

    /** {@link ParsleyCoordination#leave()} drains before removing: it must not remove a member while its
     * buffer is not drained; once drained it appends the {@code Leave} and waits until the member has left
     * the running set. */
    @Test
    void leaveDrainsBeforeRemovingTheMember() throws Exception {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        ParsleyEpochRuntime runtime = new ParsleyEpochRuntime(new InMemoryEpochTransport(log));
        ParsleyCoordination coordination = ParsleyCoordination.forRuntime(runtime);
        runtime.start();
        try {
            join(runtime, "A", Set.of("A"));
            runtime.requestSnapshot("A");                 // genesis: commits epoch 1, promoting A to running
            waitUntil(() -> runtime.isRunningMember("A"));

            Thread leaver = new Thread(() -> coordination.leave(() -> true), "leave-test");
            leaver.start();

            Thread.sleep(200);
            assertTrue(leaver.isAlive(), "leave() blocks while A's buffer is not drained");
            assertTrue(runtime.isRunningMember("A"), "leave() must not remove A before it is drained");

            runtime.reportDrained("A", true);             // A drains -> leave() proceeds through its phases
            leaver.join(5000);
            assertFalse(leaver.isAlive(), "leave() returns once A is drained and removed");
            assertFalse(runtime.isRunningMember("A"), "leave() removed A from the running set");
        } finally {
            runtime.close();
        }
    }

    /** {@link ParsleyCoordination#leave} abandons the decommission — promptly, without hanging — when its
     * liveness probe reports the instance can no longer drain, leaving the member IN the domain (as a crash
     * would), never evicted with an undrained buffer. */
    @Test
    void leaveAbandonsTheDecommissionWhenTheInstanceCanNoLongerDrain() throws Exception {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        ParsleyEpochRuntime runtime = new ParsleyEpochRuntime(new InMemoryEpochTransport(log));
        ParsleyCoordination coordination = ParsleyCoordination.forRuntime(runtime);
        runtime.start();
        try {
            join(runtime, "A", Set.of("A"));
            runtime.requestSnapshot("A");                 // genesis: commits epoch 1, promoting A to running
            waitUntil(() -> runtime.isRunningMember("A"));
            runtime.reportDrained("A", false);            // a held record the dead instance can never drain

            Thread leaver = new Thread(() -> coordination.leave(() -> false), "leave-abandon-test");
            leaver.start();
            leaver.join(5000);

            assertFalse(leaver.isAlive(),
                    "leave() must abandon the decommission promptly when the instance can no longer drain");
            assertTrue(runtime.isRunningMember("A"),
                    "an abandoned decommission must leave the member in the domain (as a crash would)");
            assertEquals(1L, runtime.committedEpochId(),
                    "no re-settle epoch may be requested for an abandoned decommission");
        } finally {
            runtime.close();
        }
    }

    /** A normal restart of an already-running member does not block: {@code awaitJoinCommit} sees it is
     * still a running member on the log and returns at once. */
    @Test
    void awaitJoinCommitDoesNotBlockForAnAlreadyRunningMember() {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        InMemoryEpochTransport seeder = new InMemoryEpochTransport(log);
        seeder.append(seedJoin("M", Set.of("M")));
        seeder.append(new ParsleyEpochEvent.SnapshotRequested("M"));
        seeder.append(seedCommit(1, ParsleyVectorClock.empty(), Set.of("M")));

        ParsleyEpochRuntime runtime = new ParsleyEpochRuntime(new InMemoryEpochTransport(log));
        ParsleyCoordination coordination = ParsleyCoordination.forRuntime(runtime);
        runtime.start();
        try {
            coordination.awaitJoinCommit(runtime, "M", "M", GENEROUS_BUDGET);   // already running -> returns at once
            assertEquals(1L, runtime.committedEpochId(),
                    "a normal restart of a running member neither blocks nor bumps the epoch");
        } finally {
            runtime.close();
        }
    }

    /** A joiner blocked in {@code awaitJoinCommit} (standing in for a task stuck in {@code init()}) must not
     * wait forever on a running member that shares its {@code StreamThread} and so can never publish
     * directly — the runtime auto-publishes that member's registered completeness, the round commits, and
     * the joiner unblocks. Here the incumbent R redeploys naming J in its roster (a legitimate add). */
    @Test
    void awaitJoinCommitUnblocksViaAnIncumbentsAutoPublishedCompleteness() throws Exception {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        InMemoryEpochTransport seeder = new InMemoryEpochTransport(log);
        seeder.append(seedJoin("R", Set.of("R")));
        seeder.append(new ParsleyEpochEvent.SnapshotRequested("R"));
        seeder.append(seedCommit(1, ParsleyVectorClock.empty(), Set.of("R")));   // R running at genesis, roster {R}

        ParsleyEpochRuntime runtime = new ParsleyEpochRuntime(new InMemoryEpochTransport(log));
        ParsleyCoordination coordination = ParsleyCoordination.forRuntime(runtime);
        // The incumbent R redeploys naming the new member J in its roster, so J becomes admissible.
        runtime.join("R", "R", Set.of("R"), Set.of(), Set.of(), Set.of("R", "J"), 1);
        ParsleyVectorClock rCompleteness = ParsleyVectorClock.empty().observe(T1, 0, 9);
        runtime.registerLocalCompleteness("R", () -> rCompleteness);   // R's only publish channel
        runtime.start();
        try {
            Thread joiner = new Thread(() -> {
                runtime.join("J", "J", Set.of("J"), Set.of(), Set.of(), Set.of("R", "J"), 1);
                coordination.awaitJoinCommit(runtime, "J", "J", GENEROUS_BUDGET);
            }, "joiner-test");
            joiner.start();
            joiner.join(5000);

            assertFalse(joiner.isAlive(),
                    "the joiner must unblock once R's completeness is auto-published and the round commits");
            assertTrue(runtime.isRunningMember("J"), "the commit promoted the joiner to running");
            assertEquals(2L, runtime.committedEpochId(), "the admitting round committed epoch 2");
            assertEquals(Set.of("R", "J"), runtime.committedRoster(), "the new roster {R,J} is committed");
            assertEquals(rCompleteness, runtime.committedLowerBounds(),
                    "the floor is R's registered completeness — its only publication came via auto-publish");
        } finally {
            runtime.close();
        }
    }

    /** A member whose task migrated off this instance (a rebalance) must not stall or corrupt a later
     * {@code leave()}: {@code unregisterMember} drops it from this instance's local set, so leave() waits
     * only on the members that remain here and never appends a {@code Leave} for the migrated one. */
    @Test
    void leaveIgnoresAMemberThatMigratedOffThisInstance() throws Exception {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        ParsleyEpochRuntime runtime = new ParsleyEpochRuntime(new InMemoryEpochTransport(log));
        ParsleyCoordination coordination = ParsleyCoordination.forRuntime(runtime);
        runtime.start();
        try {
            join(runtime, "A", Set.of("A", "B"));
            join(runtime, "B", Set.of("A", "B"));
            runtime.requestSnapshot("A");   // genesis: commits epoch 1, promoting A and B to running
            waitUntil(() -> runtime.isRunningMember("A") && runtime.isRunningMember("B"));

            runtime.unregisterMember("B");   // B's task migrates off this instance (ParsleyProcessor#close)

            Thread leaver = new Thread(() -> coordination.leave(() -> true), "leave-test");
            leaver.start();

            Thread.sleep(200);
            assertTrue(leaver.isAlive(), "leave() still blocks while A's buffer is not drained");

            runtime.reportDrained("A", true);   // A drains -> leave() proceeds; B was never waited on
            leaver.join(5000);
            assertFalse(leaver.isAlive(), "leave() returns once A alone is drained and removed");

            assertFalse(runtime.isRunningMember("A"), "leave() removed A from the running set");
            assertTrue(runtime.isRunningMember("B"),
                    "B was never appended a Leave — it migrated away, it was not evicted from the domain");
        } finally {
            runtime.close();
        }
    }

    // --- helpers --------------------------------------------------------------------------------

    /** Joins {@code member} as a single-task app declaring member-app roster {@code roster}. */
    private static void join(ParsleyEpochRuntime rt, String member, Set<String> roster) {
        rt.join(member, member, Set.of(member), Set.of(), Set.of(), roster, 1);
    }

    /** A seed-side {@link ParsleyEpochEvent.JoinRequested} for a single-task app {@code member}. */
    private static ParsleyEpochEvent.JoinRequested seedJoin(String member, Set<String> roster) {
        return new ParsleyEpochEvent.JoinRequested(member, member, Set.of(), Set.of(), roster, 1);
    }

    /** A seed-side {@link ParsleyEpochEvent.EpochCommitted}. */
    private static ParsleyEpochEvent.EpochCommitted seedCommit(long epoch, ParsleyVectorClock floor, Set<String> roster) {
        return new ParsleyEpochEvent.EpochCommitted(epoch, floor, roster);
    }

    /** Polls {@code condition} (10ms) until true or 5s elapses; throws {@link AssertionError} on timeout. */
    private static void waitUntil(java.util.function.BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5000;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("condition not met within 5s");
            }
            Thread.sleep(10);
        }
    }

    /** Folds every runtime until the shared log stops growing (three quiet passes). */
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
