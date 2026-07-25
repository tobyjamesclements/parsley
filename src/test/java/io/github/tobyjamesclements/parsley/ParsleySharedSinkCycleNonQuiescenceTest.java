package io.github.tobyjamesclements.parsley;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * KNOWN-FAILING repro, isolated 2026-07-25: the random-topology sweep's first non-quiescence.
 * Deep-tier seed 92672 = topology index 557 (topology seed 41557), run-seed suffix {@code s=1},
 * plain fault profile ({@code runIndex 1672 % 4 == 0}, no faults injected). Under this schedule the
 * settle {@link ParsleyTopologySim#drain} never quiesces — a sustained I6 null-message relay —
 * tripping the drain liveness guard ({@link ParsleyTopologySim#RUN_TIMEOUT_MS}).
 *
 * <p>The topology is a multi-node cycle with shared sinks:
 * <pre>
 *   p1 in=c1        out=c2,c3
 *   p2 in=c1,c2,c3  out=c2      (self-loop on c2)
 *   p3 in=c3        out=c2
 *   p4 in=c2,c3     out=-       (terminal)
 *   p5 in=c1,c2,c3  out=c3      (self-loop on c3)
 *   p6 in=c1,c2,c3  out=c3      (self-loop on c3)
 * </pre>
 * c1 is external; c2 has three producers (p1, p2, p3) and c3 has three producers (p1, p5, p6) —
 * more producers on a shared sink than the {@code sharedSinkThreeNodeCycle...} pin in
 * {@link ParsleyGossipCycleQuiescenceTest}, and every cycle member also consumes both.
 *
 * <p><strong>Why this asserts quiescence yet is expected to fail:</strong> the open question is
 * whether this is a genuine Parsley I6 non-quiescence on 3-producer shared-sink cycles that the
 * 2026-07-21 consumed-channel trigger fix (see {@link ParsleyGossipCycleQuiescenceTest}) does not
 * cover, or a simulator drain-model artifact (the lazy-ack settle storming where prompt-acking
 * production would not). The recorded schedule does NOT fail under replay — it is a generate-mode
 * -only sustained storm — so the distinction needs protocol-level analysis of {@link ParsleyGossip}
 * against this shape. The next session distils this into a minimal hand-built spec (as the sibling
 * quiescence pins are) once the mechanism is understood.
 *
 * <p>Run fast with {@code -Dparsley.sim.run.timeoutMs=5000}; the default 300s guard makes it slow.
 * Tracked in issue #32.
 */
@Disabled("Known non-quiescing I6 relay pending root-cause (#32); enabling it fails the build. "
        + "Reproduce in ~5s with -Dparsley.sim.run.timeoutMs=5000.")
class ParsleySharedSinkCycleNonQuiescenceTest {

    /**
     * Rebuilds the exact sweep run (topology seed 41557, schedule seed 92672, plain profile) and
     * asserts every node drains. Currently fails: the settle never quiesces and the drain liveness
     * guard throws. Passes once the relay quiesces on this shape.
     */
    @Test
    void deepSweepSeed92672SharedSinkCycleMustQuiesce() {
        ParsleySimTrace.SimSpec spec = ParsleyTopologyGen.generate(41_557L);
        // Pin the shape: a generator change must fail loudly here, not silently retarget the test.
        assertEquals(6, spec.nodes().size(),
                "generator drift: topology seed 41557 no longer yields the 6-node shared-sink cycle");

        ParsleyTopologySim sim = ParsleyTopologySim.fromSpec(spec, 92_672L).withNoTrace();
        if (spec.partitions() > 1) {
            sim.withEdgeProducers();
        }
        sim.run(2000);

        for (ParsleySimTrace.SimSpec.NodeSpec node : spec.nodes()) {
            assertEquals(0, sim.nodeNamed(node.name()).bufferSize(),
                    "node " + node.name() + " must end drained once the relay quiesces");
        }
    }
}
