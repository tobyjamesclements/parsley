package io.github.tobyjamesclements.parsley;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link CausalCoordination}, the public handle that turns on topology-epoch coordination.
 * Exercised over the in-memory transport double via the package-private {@link CausalCoordination#forRuntime}
 * seam, with the runtime driven synchronously — no broker.
 */
class CausalCoordinationTest {

    /**
     * {@link CausalCoordination#requestEpochTransition()} attributes the snapshot to a local member and
     * drives the round to a commit — the operator surface for evolving already-running nodes.
     */
    @Test
    void requestEpochTransitionOpensAndCommitsARoundOwnedByALocalMember() {
        InMemoryEpochTransport.SharedLog log = new InMemoryEpochTransport.SharedLog();
        ParsleyEpochRuntime runtime = new ParsleyEpochRuntime(new InMemoryEpochTransport(log));
        CausalCoordination coordination = CausalCoordination.forRuntime(runtime, Set.of());
        runtime.join("A");   // a local member, as a participating task's init() would

        coordination.requestEpochTransition();
        settle(log, runtime);

        assertEquals(1L, runtime.committedEpochId(),
                "requesting a transition opens a round owned by the local member and commits it");
    }

    /** Requesting a transition before any task has initialised coordination fails with a clear message. */
    @Test
    void requestEpochTransitionBeforeAnyRuntimeFails() {
        CausalCoordination coordination = CausalCoordination.create("epoch-events", Set.of());
        assertThrows(IllegalStateException.class, coordination::requestEpochTransition,
                "with no initialised runtime there is nothing to request a transition on");
    }

    /** Requesting a transition with a runtime but no joined member fails: there is no local owner to attribute it to. */
    @Test
    void requestEpochTransitionWithNoLocalMemberFails() {
        ParsleyEpochRuntime runtime =
                new ParsleyEpochRuntime(new InMemoryEpochTransport(new InMemoryEpochTransport.SharedLog()));
        CausalCoordination coordination = CausalCoordination.forRuntime(runtime, Set.of());
        assertThrows(IllegalStateException.class, coordination::requestEpochTransition,
                "no local member has joined, so the request cannot be attributed to a local owner");
    }

    /** {@link CausalCoordination#close()} is idempotent and safe to call before any task initialised the runtime. */
    @Test
    void closeIsIdempotentAndSafeBeforeInit() {
        CausalCoordination uninitialised = CausalCoordination.create("epoch-events", Set.of());
        uninitialised.close();   // no runtime built yet — a no-op

        ParsleyEpochRuntime runtime =
                new ParsleyEpochRuntime(new InMemoryEpochTransport(new InMemoryEpochTransport.SharedLog()));
        CausalCoordination coordination = CausalCoordination.forRuntime(runtime, Set.of());
        coordination.close();
        coordination.close();   // idempotent
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
